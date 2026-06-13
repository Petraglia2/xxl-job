# ExecutorRouteFailover.java —— 故障转移路由策略详解

## 一、先搞清楚它在哪里、它是什么

```
xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/route/strategy/ExecutorRouteFailover.java
```

这个类只有不到 50 行代码，但它是 xxl-job **路由策略体系**中非常关键的一环。

### 1.1 它在"路由策略家族"中的位置

xxl-job 的 admin 端在触发任务时，需要决定**把任务发给哪一台 executor（执行器）节点**。这个"选地址"的过程就是**路由**。系统提供了 9 种路由策略，全部继承自抽象父类 `ExecutorRouter`：

| 策略 | 类名 | 核心思想 |
|---|---|---|
| 第一个 | `ExecutorRouteFirst` | 永远选列表第一个地址 |
| 最后一个 | `ExecutorRouteLast` | 永远选列表最后一个地址 |
| 轮询 | `ExecutorRouteRound` | 轮流选 |
| 随机 | `ExecutorRouteRandom` | 随机选 |
| 一致性哈希 | `ExecutorRouteConsistentHash` | 按 jobId 哈希到固定节点 |
| LFU | `ExecutorRouteLFU` | 最不常用优先 |
| LRU | `ExecutorRouteLRU` | 最近最少使用优先 |
| **故障转移** | **`ExecutorRouteFailover`** | **逐个探测，选第一个活的** |
| 忙碌转移 | `ExecutorRouteBusyover` | 逐个探测，选第一个空闲的 |

父类 `ExecutorRouter` 只定义了一个抽象方法：

```java
public abstract Response<String> route(
    TriggerRequest triggerParam,   // 本次触发的任务信息
    List<String> addressList       // 该执行器组注册的所有节点地址
);
// 返回值：Response.data = 选中的地址（成功时）
```

**Failover 的使命就是：从 addressList 里找到一个"还活着"的节点，把任务交给它。**

### 1.2 它在哪里被调用

在 `JobTrigger.trigger()` 方法中（第 181 行附近）：

```java
// 根据任务配置的路由策略，选出一个 executor 地址
routeAddressResult = executorRouteStrategyEnum.getRouter().route(
    triggerParam, group.getRegistryList()
);
if (routeAddressResult.isSuccess()) {
    address = routeAddressResult.getData();  // 拿到选中的地址
}
// 然后向该地址发送真正的执行请求
triggerResult = doTrigger(triggerParam, address);
```

也就是说：**先路由选地址 → 再向该地址发 run 请求**。Failover 负责的就是"选地址"这一步。

---

## 二、逐行拆解核心逻辑

```java
public class ExecutorRouteFailover extends ExecutorRouter {

    @Override
    public Response<String> route(TriggerRequest triggerParam, List<String> addressList) {

        // ① 用一个 StringBuffer 收集每一轮探测的详细日志
        StringBuffer beatResultSB = new StringBuffer();

        // ② 遍历所有 executor 地址（按注册列表的顺序）
        for (String address : addressList) {

            // ③ 对当前地址做一次"心跳探测"
            Response<String> beatResult = null;
            try {
                // 获取与该 executor 通信的 RPC 客户端（内部有连接池缓存）
                ExecutorBiz executorBiz = XxlJobAdminBootstrap.getExecutorBiz(address);
                // 发送心跳请求：你活着吗？
                beatResult = executorBiz.beat();
            } catch (Exception e) {
                // 连接超时、网络异常等，视为"该节点挂了"
                logger.error(e.getMessage(), e);
                beatResult = Response.ofFail(e.getMessage());
            }

            // ④ 把本次探测结果追加到日志缓冲区
            beatResultSB.append((beatResultSB.length() > 0) ? "<br><br>" : "")
                .append(I18nUtil.getString("jobconf_beat") + "：")
                .append("<br>address：").append(address)
                .append("<br>code：").append(beatResult.getCode())
                .append("<br>msg：").append(beatResult.getMsg());

            // ⑤ 关键判断：心跳成功 → 立即返回这个地址
            if (beatResult.isSuccess()) {
                beatResult.setMsg(beatResultSB.toString());  // 携带完整探测日志
                beatResult.setData(address);                  // 选中的地址
                return beatResult;                            // 找到了，立刻返回
            }
            // 心跳失败 → 继续探测下一个地址
        }

        // ⑥ 所有地址都探测完了，没有一个活的 → 返回失败
        return Response.ofFail(beatResultSB.toString());
    }
}
```

### 执行流程画成图

```
addressList = [addr-A, addr-B, addr-C, addr-D]
                │
                ▼
        ┌─ beat(addr-A) ──→ 超时/异常 ──→ 记录日志，继续
        │
        ├─ beat(addr-B) ──→ 失败 ──→ 记录日志，继续
        │
        ├─ beat(addr-C) ──→ 成功 ──→ 立即返回 addr-C
        │
        └─ addr-D 不再探测（短路返回）
```

**核心设计思想：贪婪短路 + 顺序降级。** 不是先把所有节点都探一遍再选，而是"找到一个活的就立刻用"，这样既节省时间，又天然倾向于使用列表中靠前的节点。

---

## 三、几个关键细节深入剖析

### 3.1 `beat()` 心跳到底在做什么？

`ExecutorBiz` 是 admin 与 executor 之间的 RPC 接口，`beat()` 是最轻量的探活方法。executor 端收到 beat 请求后，只需回复"我还活着"即可，**不涉及任何任务执行**。它的开销极小，可以理解为 TCP 层的 ping。

### 3.2 `getExecutorBiz(address)` 的连接管理

```java
// XxlJobAdminBootstrap.java 第 138-139 行
private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<>();

public static ExecutorBiz getExecutorBiz(String address) {
    // 每个地址缓存一个 RPC 客户端实例，避免每次探测都重新建连
    return executorBizRepository.computeIfAbsent(address, addr -> {
        return new ExecutorBizImpl(addr);  // HTTP/RPC 客户端
    });
}
```

这意味着：**beat 探测复用了长连接**，不会因为 Failover 路由而频繁创建/销毁连接。

### 3.3 日志拼接的设计意图

注意 `beatResultSB` 不是随便拼的，它会作为最终 `Response.msg` 返回，并最终写入**任务触发日志**（`jobLog.triggerMsg`）。在 admin 管理后台的"调度日志"页面，运维人员能看到类似这样的信息：

```
心跳探测：
address：192.168.1.10:9999
code：500
msg：Connection refused

心跳探测：
address：192.168.1.11:9999
code：200
msg：success
```

这对于排查"为什么任务被路由到了某台机器"非常有价值。

### 3.4 与 Busyover 策略的对比

Busyover（`ExecutorRouteBusyover`）的代码结构和 Failover 几乎一模一样，区别只有一处：

| | Failover | Busyover |
|---|---|---|
| **探测方式** | `executorBiz.beat()` | `executorBiz.idleBeat(jobId)` |
| **判断标准** | 节点是否活着 | 节点对该任务是否有空闲线程 |
| **适用场景** | 节点宕机检测 | 节点虽活但线程池已满 |

可以这样理解：**Failover 关心"机器死没死"，Busyover 关心"机器忙不忙"。**

---

## 四、生产环境中需要注意的点

### 4.1 最坏情况下的延迟

如果 addressList 有 N 个节点，且前 N-1 个都挂了，Failover 要依次做完 N-1 次超时探测后才能选到第 N 个。假设每次心跳超时是 3 秒，最坏情况下仅路由阶段就要 `3 × (N-1)` 秒。

**优化建议**：如果执行器节点很多（>5 台），可以考虑适当缩短 RPC 超时时间，或者将节点列表按历史可用率排序。

### 4.2 没有"重试回到顶部"

当前实现是**严格线性扫描**：从第一个地址到最后一个，不回绕。如果所有节点在这一轮里恰好都在 GC 停顿导致 beat 超时，路由会直接返回失败，而不会"再试一轮"。

### 4.3 顺序偏好问题

由于遍历是 `for (String address : addressList)`，在正常情况下（所有节点都健康），**永远会选中第一个地址**，这会导致流量不均匀。如果你的场景需要负载均衡 + 故障转移兼顾，建议用 `ROUND`（轮询）或 `RANDOM`（随机）策略，把 Failover 作为兜底。

### 4.4 路由失败后的行为

当 `route()` 返回失败（所有节点都不通）时，在 `JobTrigger` 中：

```java
if (address != null) {
    triggerResult = doTrigger(triggerParam, address);  // 正常触发
} else {
    triggerResult = Response.of(HANDLE_CODE_FAIL, "Address Router Fail.");  // 路由失败
}
```

这条任务会被标记为**触发失败**，后续是否重试取决于任务配置的"失败重试次数"。

---

## 五、一句话总结

> **ExecutorRouteFailover 就是一个"挨个敲门，谁应门就找谁"的路由策略——它按顺序对执行器列表发心跳，返回第一个存活节点的地址，如果全挂了则返回失败。代码虽短，却覆盖了探活、日志收集、异常兜底三个核心关切。**
