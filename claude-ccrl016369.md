# xxl-job admin 端任务触发核心文件 `JobTrigger.java` 讲解

> 面向刚接手该项目的后端工程师。先给全局地图，再逐方法拆解 `JobTrigger.java`，最后点出几个新人最容易困惑的"为什么这么写"。
>
> 文件位置：`xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/trigger/JobTrigger.java`

---

## 一、它是谁：一句话定位

`JobTrigger` 是 admin 端**「把一次任务触发，真正发到某台执行器上」的核心逻辑**。它不负责"什么时候该触发"（那是调度线程的事），只负责"现在要触发了，该发给谁、怎么发、结果记到哪"。

它是个 Spring `@Component`，注入了三张表的 Mapper：

| 依赖 | 作用 |
|---|---|
| `XxlJobInfoMapper` | 读任务配置（handler、参数、路由/阻塞策略、超时、重试次数…） |
| `XxlJobGroupMapper` | 读执行器分组，关键是它的 `registryList`（在线执行器地址列表） |
| `XxlJobLogMapper` | 写调度日志（每触发一次写一行） |

---

## 二、它在调用链里的位置（先搞清上下游，再读代码）

```
触发来源（6种，见 TriggerTypeEnum）
  ├─ CRON     定时调度线程
  ├─ MANUAL   后台页面点"执行一次"
  ├─ API      OpenAPI 调用
  ├─ PARENT   父任务完成后触发子任务
  ├─ RETRY    失败重试
  └─ MISFIRE  调度阻塞后的补偿
        │
        ▼
JobTriggerPoolHelper.trigger(...)   ← 异步入口，丢进线程池
        │  (fast / slow 双线程池)
        ▼
JobTrigger.trigger(...)             ← 【本文件】真正的触发逻辑
        ▼
JobTrigger.processTrigger(...)      ← 单次分发（含写日志、选地址）
        ▼
JobTrigger.doTrigger(...)           ← HTTP 调远程执行器 ExecutorBiz.run()
```

**关键点 1**：所有上游都不会直接 new 出 `JobTrigger` 调用，而是统一走 `JobTriggerPoolHelper`（`scheduler/thread/JobTriggerPoolHelper.java:100`）扔进线程池异步执行。这样做的目的是**让"调度"和"触发 IO"解耦**——调度线程只管把任务塞进队列就返回，不会被某台慢执行器拖住。

**关键点 2（值得记住的设计）**：线程池有 **fast / slow 两个池**。某个 jobId 在 1 分钟内触发耗时 >500ms 超过 10 次，后续就被打到 slow 池（`JobTriggerPoolHelper.java:108-112, 137-141`）。这是一种**故障隔离**：防止个别慢任务占满线程池、把正常任务饿死。读 `JobTrigger` 之前知道这点，就能理解为什么这里的方法是同步阻塞写法——因为外层已经用线程池兜住了。

---

## 三、逐方法拆解

### 1）`trigger(...)`：入口与"扇出"决策（`JobTrigger.java:63`）

它有 6 个参数，**每个参数的 null 语义在 javadoc 里写得很清楚（第 45–62 行），建议你读代码前先看那段注释**，这是理解整个文件的钥匙：

- `failRetryCount`：`>=0` 用传入值；`<0` 用任务配置里的值（第 79 行）。
- `executorShardingParam`：`null` = 全新触发（广播时发所有节点）；非 null = 只发某个分片（**重试场景下，只重试失败的那个分片**）。
- `executorParam` / `addressList`：非 null 时**覆盖**任务原有配置（第 76–78、83–86 行）。

这个方法本身逻辑很短，核心就干一件事——**决定要调用几次 `processTrigger`**：

```java
if (路由策略 == SHARDING_BROADCAST && 有在线节点 && 没指定分片) {
    // 分片广播：每台在线执行器都发一次
    for (i = 0; i < registryList.size(); i++)
        processTrigger(..., i, registryList.size());   // 第 99-104 行
} else {
    // 其它所有策略：只发一次
    if (shardingParam == null) shardingParam = {0, 1};
    processTrigger(..., shardingParam[0], shardingParam[1]); // 第 105-110 行
}
```

> 新人易混点：**"广播"和"故障转移(FAILOVER)"是两回事**。广播 = 一次触发打到**所有**节点（每节点一条日志）；FAILOVER 是一种路由策略，只是"在多台里挑一台健康的"，仍然只发一次。

`index/total` 这对参数就是分片号：广播时 `(0/3, 1/3, 2/3)` 分别发给 3 台机器，执行器侧据此做数据分片处理。

---

### 2）`processTrigger(...)`：单次分发的全过程（`JobTrigger.java:134`）

这是**最该精读的方法**，6 个步骤一气呵成：

**① 先落一条日志、拿到自增 logId（第 147–152 行）**
```java
XxlJobLog jobLog = new XxlJobLog();
... // 只填了 jobGroup / jobId / triggerTime
xxlJobLogMapper.save(jobLog);   // 此时拿到 jobLog.getId()
```
> **为什么先存一条几乎是空的日志？** 这是 xxl-job 一个很重要的设计：执行器执行完业务后是**异步回调**汇报结果的，回调时要告诉 admin "我执行的是哪一条记录"。所以必须**在远程调用之前**先把日志行插出来、拿到 `logId`，再把这个 id 通过 `TriggerRequest` 带给执行器（第 162 行 `setLogId`）。后续回调（`TriggerCallbackThread`）就靠这个 id 回写执行结果。

**② 组装 `TriggerRequest`（第 155–168 行）**
把任务的一切打包：handler 名、参数、阻塞策略、超时、logId、GLUE 脚本信息、分片号。这就是发给执行器的"请求体"。

**③ 选地址（第 170–188 行）——路由的核心**
```java
if (没有在线节点)            → 直接失败："address empty"
else if (广播)              → 取 registryList[index]（越界兜底取[0]）
else                       → 用路由策略挑一台：
                             executorRouteStrategyEnum.getRouter().route(...)
```
每种路由策略背后是一个 `ExecutorRouter` 实现类（见下方策略表）。

**④ 真正触发（第 190–196 行）**
地址选到了就 `doTrigger(...)`；没选到就构造一个失败结果。**注意这里很克制**：即使路由失败也不抛异常，而是转成一个 `FAIL` 的 `Response`，保证流程能继续走到"写日志"。

**⑤ 拼接 triggerMsg（第 198–235 行）**
一大段 `StringBuilder` 拼 HTML，把"触发类型、admin 地址、执行器列表、路由策略、阻塞策略、超时、重试次数、最终地址、调用结果"全拼进去。**这就是你在后台「调度日志 → 调度备注」弹窗里看到的那段内容**。读起来啰嗦，但本质只是日志展示，不含业务逻辑，可快速略过。

**⑥ 回写日志（第 237–246 行）**
把地址、handler、分片号、重试次数、`triggerCode`、`triggerMsg` 更新回刚才那条日志行（`updateTriggerInfo`）。

---

### 3）`doTrigger(...)`：真正的远程调用（`JobTrigger.java:258`）

```java
ExecutorBiz executorBiz = XxlJobAdminBootstrap.getExecutorBiz(address); // 拿 HTTP 客户端
Response<String> runResult = executorBiz.run(triggerParam);             // 发 HTTP 请求
```
`getExecutorBiz`（`XxlJobAdminBootstrap.java:139`）会基于地址创建一个 `ExecutorBiz` 接口的 **HTTP 代理客户端**（带超时、`XXL_JOB_ACCESS_TOKEN` 鉴权头），并**按地址缓存**（`ConcurrentHashMap`），避免每次触发都重建连接。

整个方法用 `try/catch` 包住，执行器宕机 / 网络异常时记 `error` 日志并返回 `FAIL`——**绝不让一台挂掉的执行器把触发线程搞崩**。

---

## 四、两张策略表（路由 + 阻塞）

**路由策略**（`scheduler/route/ExecutorRouteStrategyEnum.java`，每个枚举绑一个 `ExecutorRouter`）：

| 策略 | 含义 |
|---|---|
| FIRST / LAST | 固定第一台 / 最后一台 |
| ROUND / RANDOM | 轮询 / 随机 |
| CONSISTENT_HASH | 一致性哈希（同 jobId 尽量固定一台） |
| LFU / LRU | 最不常用 / 最近最少使用 |
| FAILOVER | 故障转移：逐台心跳探测，选第一台健康的 |
| BUSYOVER | 忙碌转移：选第一台空闲的 |
| SHARDING_BROADCAST | 分片广播：发给**所有**节点（在 `trigger` 里特殊处理扇出） |

**阻塞策略**（`xxl-job-core/.../ExecutorBlockStrategyEnum.java`）：`SERIAL_EXECUTION`（串行，默认）/ `DISCARD_LATER`（丢弃后续）/ `COVER_EARLY`（覆盖之前）。

> 注意一个分工：**阻塞策略是在执行器侧生效的**，admin 这里只是把它读出来塞进 `TriggerRequest` 并写进日志（第 143、160 行），自己并不据此做任何拦截。

---

## 五、新人最该记住的 3 个"为什么"

1. **触发结果 ≠ 业务执行结果（两阶段模型）**
   `JobTrigger` 拿到的 `triggerCode`/`triggerResult` 只代表**"执行器是否接收了这次调度请求"**（HTTP 同步返回）。任务里业务逻辑跑成功还是失败，是执行器**后续异步回调**回来的 `handleCode`。所以你在日志页会看到"调度结果"和"执行结果"两列——**本文件只管前一列**。

2. **先插日志、再发请求**：顺序不能换。因为 logId 是异步回调的"对账依据"（见 ③ 的说明）。

3. **重试为什么要带分片号**：普通任务重试就是再触发一次；但**广播任务**如果第 2 个分片失败，重试时会带上 `executorShardingParam="1/3"`，让 `trigger` 走单次分发分支、**只补那一个失败分片**，而不是把三台全重发一遍（对应入口参数 javadoc 里 "not null: for retry, only one node"）。

---

## 建议的阅读路径

先读 `trigger` 的 javadoc（45–62 行）→ `trigger` 的扇出 if/else（99–110 行）→ `processTrigger` 的六步 → `doTrigger`。把"日志在远程调用前就插入""结果分两阶段"这两点理解透，这个文件就通了。

下一步可继续看**异步回调是怎么回写 `handleCode` 的**（`TriggerCallbackThread` + admin 侧的回调接口），那是这条链路的另一半。
