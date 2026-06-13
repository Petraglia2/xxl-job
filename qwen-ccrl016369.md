# `JobTrigger.java` 深入浅出讲解

## 一、它在架构中的位置

先用一张全局图定位这个文件：

```
谁调用 trigger？                        JobTrigger 做什么？                     结果去哪里？
─────────────                        ──────────────                        ──────────
JobScheduleHelper (cron定时)   ─┐
XxlJobServiceImpl (手动执行)    ─┤                                         ┌─ xxl_job_log 表（记录日志）
JobFailAlarmMonitorHelper(重试) ─┼──▶  JobTriggerPoolHelper(线程池)  ──▶ JobTrigger  ──┤
MisfireFireOnceNow (失火补偿)   ─┤       (fast/slow隔离)                     └─ HTTP → Executor（执行器）
API 调用                        ─┘
```

**一句话总结**：`JobTrigger` 是 admin 端「触发任务」的**唯一出口**。不管任务是定时触发、手动点击、失败重试还是 API 调用，最终都汇聚到这里，由它完成 **查配置 → 选机器 → 记日志 → 发请求** 这四步。

---

## 二、类结构概览

```java
@Component
public class JobTrigger {

    // 三个 Mapper，用于读取任务/执行器配置 + 写入触发日志
    @Resource XxlJobInfoMapper  xxlJobInfoMapper;
    @Resource XxlJobGroupMapper xxlJobGroupMapper;
    @Resource XxlJobLogMapper   xxlJobLogMapper;

    public void trigger(...)          { ... }   // 入口：参数解析 + 分片调度
    private void processTrigger(...)  { ... }   // 核心：一次完整的触发流程
    private Response<String> doTrigger(...) { ... } // 底层：向执行器发 HTTP 请求
}
```

---

## 三、入口方法 `trigger()` —— 参数解析 & 分片分发

```java
public void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount,
                    String executorShardingParam, String executorParam, String addressList)
```

### 3.1 参数含义速查

| 参数 | 含义 | 典型值 |
|---|---|---|
| `jobId` | 任务 ID | 数据库主键 |
| `triggerType` | 触发类型枚举 | `CRON` / `MANUAL` / `RETRY` / `MISFIRE` / `API` / `PARENT` |
| `failRetryCount` | 失败重试次数 | `>=0` 使用传入值；`<0` 用任务配置值 |
| `executorShardingParam` | 分片参数 `"index/total"` | 重试时指定单个节点，如 `"2/5"` |
| `executorParam` | 覆盖任务参数 | `null` 则用任务自带参数 |
| `addressList` | 覆盖执行器地址 | `null` 则用执行器组的注册列表 |

### 3.2 核心逻辑拆解

```java
// 1. 加载任务配置（如果 jobId 不存在，直接 warn 返回）
XxlJobInfo jobInfo = xxlJobInfoMapper.loadById(jobId);

// 2. 参数覆盖（外部传入 > 任务配置）
if (executorParam != null) jobInfo.setExecutorParam(executorParam);
int finalFailRetryCount = failRetryCount >= 0 ? failRetryCount : jobInfo.getExecutorFailRetryCount();

// 3. 加载执行器组
XxlJobGroup group = xxlJobGroupMapper.load(jobInfo.getJobGroup());

// 4. 地址覆盖（手动指定地址时，强制设为"手动录入"模式）
if (StringTool.isNotBlank(addressList)) {
    group.setAddressType(1);           // 1 = 手动录入
    group.setAddressList(addressList.trim());
}
```

### 3.3 分片广播 —— 最关键的路径分叉

```java
if (路由策略 == SHARDING_BROADCAST && 有注册节点 && 未指定分片参数) {
    // 🔑 分片广播：对每台机器各触发一次，index 从 0 到 N-1
    for (int i = 0; i < group.getRegistryList().size(); i++) {
        processTrigger(..., index=i, total=group.getRegistryList().size());
    }
} else {
    // 🔑 普通路由：只选一台机器
    processTrigger(..., index=shardingParam[0], total=shardingParam[1]);  // 默认 {0, 1}
}
```

> **新人重点**：分片广播是 xxl-job 的亮点功能。假设你有 3 台 executor，配了分片广播，那每次触发会对 3 台机器各调一次 `processTrigger`，每台拿到 `broadcastIndex=0/1/2, broadcastTotal=3`，业务代码可以据此只处理属于自己的那部分数据（比如按 userId % 3 取模分片）。

---

## 四、核心方法 `processTrigger()` —— 一次完整的触发流程

这是整个文件最重要的方法，一共做 **6 件事**：

### Step 1：保存日志记录

```java
XxlJobLog jobLog = new XxlJobLog();
jobLog.setJobGroup(jobInfo.getJobGroup());
jobLog.setJobId(jobInfo.getId());
jobLog.setTriggerTime(triggerTime);
xxlJobLogMapper.save(jobLog);   // INSERT → 拿到自增主键 logId
```

> 先写一条"空日志"占位，后续再 UPDATE 填充触发结果。这样即使触发中途异常，数据库里也有记录可查。

### Step 2：组装请求参数 `TriggerRequest`

```java
TriggerRequest triggerParam = new TriggerRequest();
triggerParam.setJobId(jobInfo.getId());
triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());  // 执行器端的 handler 名称
triggerParam.setExecutorParams(jobInfo.getExecutorParam());      // 传给 handler 的参数
triggerParam.setExecutorBlockStrategy(...);   // 阻塞策略
triggerParam.setExecutorTimeout(...);         // 超时时间(秒)
triggerParam.setLogId(jobLog.getId());        // 让执行器知道日志写到哪里
triggerParam.setGlueType(...) / setGlueSource(...);  // GLUE 脚本模式相关
triggerParam.setBroadcastIndex(index);        // 分片索引
triggerParam.setBroadcastTotal(total);        // 分片总数
```

### Step 3：路由选地址

```java
if (执行器组有注册节点) {
    if (分片广播) {
        // 按 index 直接取对应机器
        address = group.getRegistryList().get(index);
    } else {
        // 走路由策略选一台
        routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, registryList);
        address = routeAddressResult.getData();
    }
} else {
    // 没有可用执行器 → 触发失败
    routeAddressResult = Response.of(FAIL, "无可用的执行器地址");
}
```

**10 种路由策略**（在 `ExecutorRouteStrategyEnum` 中定义）：

| 策略 | 说明 |
|---|---|
| `FIRST` | 始终选第一台 |
| `LAST` | 始终选最后一台 |
| `ROUND` | 轮询（Round-Robin） |
| `RANDOM` | 随机 |
| `CONSISTENT_HASH` | 一致性哈希（同一 jobId 总落同一台） |
| `LEAST_FREQUENTLY_USED` | LFU，选使用次数最少的 |
| `LEAST_RECENTLY_USED` | LRU，选最久没用的 |
| `FAILOVER` | 故障转移（先心跳探测，选第一台活的） |
| `BUSYOVER` | 忙碌转移（先查空闲，选第一台闲的） |
| `SHARDING_BROADCAST` | 分片广播（特殊，不走 Router，上面单独处理了） |

### Step 4：远程调用执行器

```java
if (address != null) {
    triggerResult = doTrigger(triggerParam, address);   // 发 HTTP 请求
} else {
    triggerResult = Response.of(FAIL, "Address Router Fail.");
}
```

### Step 5 + 6：拼接触发信息 & 更新日志

```java
// 拼一段 HTML 格式的触发消息（前端详情页可直接展示）
StringBuilder triggerMsgSb = new StringBuilder();
triggerMsgSb.append("触发类型：CRON");
triggerMsgSb.append("<br>调度中心：192.168.1.100");
triggerMsgSb.append("<br>执行器注册方式：自动注册");
triggerMsgSb.append("<br>路由策略：轮询");
// ... 等等

// 更新日志
jobLog.setExecutorAddress(address);
jobLog.setTriggerCode(triggerResult.getCode());    // 200=成功，500=失败
jobLog.setTriggerMsg(triggerMsgSb.toString());      // 详细 HTML 消息
xxlJobLogMapper.updateTriggerInfo(jobLog);          // UPDATE
```

---

## 五、底层方法 `doTrigger()` —— RPC 调用

```java
private Response<String> doTrigger(TriggerRequest triggerParam, String address) {
    try {
        // 从缓存拿 / 新建 HTTP 客户端代理
        ExecutorBiz executorBiz = XxlJobAdminBootstrap.getExecutorBiz(address);

        // 调用执行器的 run 接口（底层是 HTTP POST）
        Response<String> runResult = executorBiz.run(triggerParam);

        // 包装返回信息
        return runResult;
    } catch (Exception e) {
        // 网络不通、执行器宕机等异常，返回 FAIL
        return Response.of(HANDLE_CODE_FAIL, ThrowableTool.toString(e));
    }
}
```

**`ExecutorBiz` 的本质**：这是一个通过 `HttpTool.createClient().proxy(ExecutorBiz.class)` 动态生成的 HTTP 代理。调用 `executorBiz.run(triggerParam)` 实际是在向执行器发送 `POST /run` 请求，请求体就是 `TriggerRequest` 的 JSON。客户端会被缓存在 `executorBizRepository`（`ConcurrentMap<String, ExecutorBiz>`）中，同一地址只创建一次。

---

## 六、谁调用 `JobTrigger`？—— 上游调用链

`JobTrigger` 并不直接被上层调用，中间隔了一层 **`JobTriggerPoolHelper`**（线程池隔离）：

```
调用方                      线程池                         JobTrigger
──────                     ──────                        ──────────
                      ┌─ fastTriggerPool (默认) ─┐
各触发源 ──▶ PoolHelper ┤                         ├──────▶ trigger()
                      └─ slowTriggerPool (降级)  ─┘
```

**快/慢线程池隔离机制**（这是一个很实用的设计）：

```java
// 每个 jobId 有一个 1 分钟滑动窗口的超时计数器
// 如果某 jobId 在 1 分钟内触发耗时 >500ms 超过 10 次
// → 后续触发自动丢进 slowTriggerPool
// → 防止一个慢任务拖垮整个调度系统
```

| 线程池 | 核心/最大 | 队列容量 | 用途 |
|---|---|---|---|
| `fastTriggerPool` | 10 / 可配(默认200) | 2000 | 正常任务 |
| `slowTriggerPool` | 10 / 可配(默认100) | 5000 | 频繁超时的"问题任务" |

**四大触发来源**：

| 来源 | 触发类型 | 场景 |
|---|---|---|
| `JobScheduleHelper` | `CRON` | 调度线程按 cron 表达式定时触发 |
| `XxlJobServiceImpl.trigger()` | `MANUAL` | 用户在管理后台点击"执行一次" |
| `JobFailAlarmMonitorHelper` | `RETRY` | 失败任务自动重试 |
| `MisfireFireOnceNow` | `MISFIRE` | 调度中心重启后补偿错过的任务 |

---

## 七、一张流程图总结

```
                    trigger(jobId, ...)
                         │
                    ┌────▼────┐
                    │ 查数据库  │  xxlJobInfoMapper.loadById()
                    │ 加载配置  │  xxlJobGroupMapper.load()
                    └────┬────┘
                         │
                    ┌────▼────────────┐
                    │ 参数覆盖 & 解析   │  executorParam / addressList / shardingParam
                    └────┬────────────┘
                         │
                ┌────────▼─────────┐
                │ 是否分片广播？      │
                └──┬───────────┬───┘
                  YES          NO
                   │            │
          ┌────────▼──┐   ┌────▼─────────┐
          │ for 循环    │   │ 路由策略选一台  │
          │ 每台机器各   │   │ (10种策略)     │
          │ 调一次      │   └────┬─────────┘
          └────────┬──┘        │
                   │     ┌─────▼──────┐
                   └─────▶processTrigger()
                         ┌────────────────────────────────────┐
                         │ 1. INSERT 日志占位                    │
                         │ 2. 组装 TriggerRequest               │
                         │ 3. 路由选地址                         │
                         │ 4. doTrigger() → HTTP POST 到执行器   │
                         │ 5. 拼接 HTML 触发消息                  │
                         │ 6. UPDATE 日志                        │
                         └────────────────────────────────────┘
```

---

## 八、新人需要记住的关键点

1. **所有触发汇聚于此**：无论 cron、手动、重试、API，最终都走 `JobTrigger.trigger()`。
2. **先写日志再触发**：`save → doTrigger → updateTriggerInfo`，保证有迹可循。
3. **分片广播 = 循环调用**：不是一次请求让执行器自己分片，而是 admin 端循环 N 次，每次带不同的 `index/total`。
4. **路由策略只选一台**：除了分片广播，其余策略都只选一台 executor 执行。
5. **快慢池隔离**：`JobTriggerPoolHelper` 会把频繁超时的任务降级到慢池，保护系统整体吞吐。
6. **`doTrigger` 是 HTTP 调用**：通过 `ExecutorBiz` 接口动态代理，实际是 `POST /run` 到执行器的 HTTP 服务。
7. **触发 ≠ 执行完成**：`doTrigger` 返回只代表执行器**接收了任务**，实际执行是异步的，执行器完成后会回调 admin 的 callback 接口。

---

## 延伸阅读

如果想继续深入，建议接下来看这几个文件：
- `JobScheduleHelper.java` —— cron 调度如何产生触发
- `ExecutorRouteStrategyEnum` 下的各个 Router 实现 —— 路由算法细节
- `JobCompleteHelper.java` —— 执行器回调后如何处理结果
- `JobFailAlarmMonitorHelper.java` —— 失败重试 & 告警的逻辑
