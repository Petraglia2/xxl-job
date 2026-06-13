# ExecutorRouteFailover.java 讲解（面向刚接手项目的后端工程师）

下面这份讲解假设你刚接手项目，对 xxl-job 的整体调度链路还不熟，所以我会先把它"放回上下文"，再逐行拆这个文件，最后讲清设计意图和容易踩的坑。

---

## 一句话定位

`ExecutorRouteFailover`（故障转移）是 10 种执行器路由策略中的一种。它的职责很单纯：**在一批执行器地址里，按顺序逐个发"心跳探测"，挑出第一个还活着的节点返回。** 你可以把它理解成 `FIRST` 策略（永远取第一台）的"带体检版本"——不是闭眼取第一台，而是取第一台**确认在线**的。

---

## 它在调度链路里的位置

路由器不是自己跑起来的，调用它的是 `JobTrigger.processTrigger(...)`。关键几行（`xxl-job-admin/.../trigger/JobTrigger.java:144、181-184`）：

```java
// 1. 根据任务配置(jobInfo)解析出本次该用哪种路由策略
ExecutorRouteStrategyEnum executorRouteStrategyEnum =
        ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null);
...
// 2. 非分片广播的策略，都走统一入口：把"在线执行器地址列表"交给路由器选一台
routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, group.getRegistryList());
if (routeAddressResult.isSuccess()) {
    address = routeAddressResult.getData();   // 选中的地址在 Response.data 里
}
```

由此能看清这个文件的**输入/输出契约**：

| | 内容 |
|---|---|
| 输入 | `triggerParam`（本次触发参数，failover 其实没用它）、`addressList`（该执行器组当前**已注册在线**的全部地址） |
| 输出 | `Response<String>`：成功时 `isSuccess()=true`、`data=`选中的地址；全部失败时 `ofFail(...)`，触发流程会因为拿不到 address 而判定本次调度失败 |

策略与实现类的对应关系在 `ExecutorRouteStrategyEnum:18`：
```java
FAILOVER(I18nUtil.getString("jobconf_route_failover"), new ExecutorRouteFailover()),
```
注意每个枚举值持有一个**单例** router 实例（`new ExecutorRouteFailover()`），所以这个类必须是无状态的——它确实没有任何成员变量，天然线程安全。（唯一的例外是 `SHARDING_BROADCAST`，它的 router 是 `null`，因为分片广播由 `JobTrigger` 单独处理，不走这条路由入口。）

---

## 逐行拆解

整个文件就一个 `route` 方法（`ExecutorRouteFailover.java:18-47`）：

```java
public Response<String> route(TriggerRequest triggerParam, List<String> addressList) {

    StringBuffer beatResultSB = new StringBuffer();          // ① 诊断日志累加器
    for (String address : addressList) {                     // ② 按列表顺序逐台探测
        Response<String> beatResult = null;
        try {
            ExecutorBiz executorBiz = XxlJobAdminBootstrap.getExecutorBiz(address); // ③ 拿到指向该执行器的 RPC 客户端
            beatResult = executorBiz.beat();                 // ④ 发心跳：你还活着吗？
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            beatResult = Response.ofFail(e.getMessage());    // ⑤ 探测异常也兜底成"失败响应"，保证不为 null
        }
        beatResultSB.append(...)                             // ⑥ 把这次探测结果拼成一段 HTML，记进日志
                .append("<br>address：").append(address)
                .append("<br>code：").append(beatResult.getCode())
                .append("<br>msg：").append(beatResult.getMsg());

        if (beatResult.isSuccess()) {                        // ⑦ 第一台探测成功的就是赢家
            beatResult.setMsg(beatResultSB.toString());      //    把完整探测日志塞回 msg（给 UI 看）
            beatResult.setData(address);                     //    data = 选中地址（这是给调用方用的关键字段）
            return beatResult;                               //    短路返回，后面的地址不再探测
        }
    }
    return Response.ofFail(beatResultSB.toString());         // ⑧ 一台都没活着 → 整体失败
}
```

把几个点说透：

- **③ `getExecutorBiz(address)`**：根据地址拿到一个调用该执行器 openapi 的客户端（`ExecutorBiz` 接口，见 `xxl-job-core/.../openapi/ExecutorBiz.java`）。`beat()` 是其中最轻量的一个方法，语义就是"健康探测/握手"，不触发任何业务。
- **④⑤ 的容错设计**：无论 `beat()` 返回失败，还是直接抛异常（网络不通、连接超时），都会落到 `beatResult` 上一个**非空**的 `Response`。这点很关键——正因为 `catch` 里兜底了 `Response.ofFail(...)`，第 ⑥ 步访问 `beatResult.getCode()` 才不会出现 NPE。读代码时容易担心这里空指针，实际上是安全的。
- **⑥ 为什么路由类里会拼 HTML？** 这段 `beatResultSB` 不是给程序用的，是给**人**看的：它最终会成为你在 admin 后台"调度日志"里看到的那段"探测地址 A：失败；探测地址 B：成功"的明细。这是 failover 相比其它策略很贴心的一点——出问题时你能直接看到它探了哪几台、各自什么结果。
- **⑦ 短路返回**：一旦命中第一台活着的就立刻返回，不会把列表里剩下的都探一遍。所以**列表顺序 = 优先级**。
- **⑧ 全失败**：返回 `ofFail`，`isSuccess()=false`，`data` 为空。回到 `JobTrigger` 那边 `address` 拿不到值，本次触发就会被记为失败，而失败信息正是这段完整的探测日志。

---

## 设计意图与权衡（这部分最值得理解）

**1. "故障转移"转移的是什么？**
是**路由那一刻的节点选择**，不是任务执行失败后的重试。它在"开火之前"先做一次实时体检，把已经挂掉的节点跳过去。

**2. 为什么不能只靠注册表？**
`addressList` 来自注册表（已注册在线的地址）。但注册表的存活判断是**基于心跳的、有探测窗口/延迟**的——一个节点刚宕机，在被注册表剔除之前，仍然会出现在这个列表里。`FIRST` 策略闭眼取第一台，就可能恰好打到这台"名义在线、实际已死"的节点。failover 用一次实时 `beat()` 弥补了这个时间差，这就是它存在的核心价值。

**3. 和 `FIRST` 的关系——记住这个心智模型就够了：**
> `FAILOVER ≈ FIRST + 实时心跳探测`
> 只要列表头部的节点一直健康，failover 每次都会选同一台（行为"黏性"、可预测，对本地缓存友好）；一旦它挂了，自动顺延到下一台。

**4. 代价：**
- **不做负载均衡**：它总是偏向列表前面的节点，流量会集中在头部，不会像 `ROUND`/`RANDOM` 那样分摊。需要均衡时不要选它。
- **不健康时路由会变慢**：正常情况只发 1 次 `beat()`（第一台就活）；但极端情况（前面 N-1 台都挂了）要**串行**探测 N 次，每次都可能卡到连接超时才返回。集群大面积故障时，路由本身的耗时会被放大。

---

## 新人最容易混淆的两点

1. **"故障转移路由" ≠ "失败重试(failRetryCount)"**
   这是两套独立机制，经常被搞混：
   - *故障转移路由（本文件）*：开火**前**选一台健康节点。
   - *失败重试*：任务真正执行**失败后**，把整次触发再发一遍（可能换台机器）。
   两者正交，可以叠加使用：failover 降低"打到死节点"的概率，失败重试兜住"打到活节点但执行仍失败"的情况。

2. **父类注释里的 `ReturnT` 是历史遗留**
   `ExecutorRouter.route` 的 javadoc 写着 `@return ReturnT.content=address`，但代码实际返回的是 `com.xxl.tool.response.Response`，选中地址在 `Response.data` 里。这是这个 fork 从早期 xxl-job 的 `ReturnT` 迁移到 `Response` 后，注释没同步更新留下的痕迹——看代码以 `Response.data` 为准，别被注释带偏。

---

## 小结

`ExecutorRouteFailover` 是个只有约 30 行、无状态、线程安全的小类，但它把"高可用路由"这件事做得很到位：**按优先级顺序实时探活，选第一个能用的，并把探测过程完整记录给运维看。** 理解它只需抓住三点——*顺序即优先级、beat 是实时体检、data 携带最终地址*；选型时记住它**牺牲负载均衡换取确定性与可用性**，且**和失败重试是两回事**即可。

如果你想继续往下看，建议顺着 `JobTrigger.processTrigger` 看完整条触发链路（选地址 → `doTrigger` 远程调用 → 回写调度日志），或者对照 `ExecutorRouteBusyover`（它和 failover 几乎是孪生结构，只是把 `beat()` 换成了 `idleBeat()`——探"忙不忙"而不是"活没活"）。
