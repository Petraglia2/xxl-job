# xxl-job 执行器注册与调度链路解读

> **面向对象**:刚接手该项目的后端工程师
> **范围**:从 admin 端执行器注册文件 `JobRegistryHelper` 出发,深入浅出地讲清楚「执行器如何被发现」以及「注册出来的地址如何被用来选机器、发任务」这一整条链路。
> **基准**:以本仓库实际实现为准(本项目对官方原版做过重构,目录从 `core.thread` 迁到了 `scheduler.thread`,类名也有改动,网上原版教程不完全适用)。

## 目录

- [第一章 `JobRegistryHelper`:执行器是怎么「被发现」的](#第一章-jobregistryhelper执行器是怎么被发现的)
- [第二章 下游链路:`address_list` 如何被用来选机器并发任务](#第二章-下游链路address_list-如何被用来选机器并发任务)
- [附:可继续深入的方向](#附可继续深入的方向)

---

# 第一章 `JobRegistryHelper`:执行器是怎么「被发现」的

> 主文件:`xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/thread/JobRegistryHelper.java`

## 一句话定位

它是 admin 端的**执行器注册中心**。每个 executor 启动后会周期性地「上报心跳」,admin 把这些心跳落到 `xxl_job_registry` 表;`JobRegistryHelper` 负责**接收上报**和**维护「哪些执行器还活着」**,最终把活着的地址写回每个任务组的 `address_list`——调度时就是从这个地址列表里挑机器发任务的。

可以类比 Nacos / Eureka 的注册中心,只不过这里是 xxl-job 自带的、基于数据库的极简版。

## 它在整条链路里的位置

```
Executor 进程
  │  每 30s 发一次 HTTP 注册请求(OpenAPI)
  ▼
admin OpenAPI Controller
  ▼
AdminBizImpl.registry()                 ← service/impl/AdminBizImpl.java:24
  ▼
JobRegistryHelper.registry()            ← 本文件:158
  ▼
xxl_job_registry 表(异步落库)
                                         ┌─────────────────────────────┐
JobRegistryHelper 的监控线程(每 30s)──┤ 清理死实例 + 刷新 group 地址 │
                                         └─────────────────────────────┘
  ▼
xxl_job_group.address_list ← 调度器据此选机器
```

**生命周期**:它不是 Spring Bean,而是由单例 `XxlJobAdminBootstrap` 在 admin 启动时 `new` 出来并 `start()`,关闭时 `stop()`(见 `xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/config/XxlJobAdminBootstrap.java:88-89` 和 `:127`)。其他地方都通过 `XxlJobAdminBootstrap.getInstance().getJobRegistryHelper()` 拿到它。

## 核心:这个类里其实是「两台发动机」

读这个类最容易晕的地方是——它把**两件独立的事**塞在了一个类里。拆开看就清楚了。

### 发动机①:异步写入(处理上报)

对应 `registry()`(:158)和 `registryRemove()`(:193),以及一个线程池 `registryOrRemoveThreadPool`(:37)。

**为什么要异步?** 每个执行器每 30 秒上报一次,执行器一多,QPS 就不低。如果让接收 HTTP 请求的线程同步去写数据库,调用方就得干等。所以这里的套路是:**参数校验 → 扔进线程池 → 立刻返回 `Response.ofSuccess()`**,落库在后台慢慢做。

线程池的配置值得记一下:

```java
core=2, max=10, keepAlive=30s, 队列=LinkedBlockingQueue(2000)
拒绝策略:r.run()  // 满了就让"提交任务的线程"自己跑,并打 warn 日志
```

这个拒绝策略(:49-54)是个细节——它**不丢请求**,而是退化成「谁提交谁执行」(caller-runs),相当于一个天然的背压/限流:压力大时调用线程会被拖慢,从而反向限制上报速度,但数据不会丢。

`registry()` 真正干的事(:172):

```java
int ret = ...registrySaveOrUpdate(group, key, value, now);  // 一条 upsert SQL
// 返回值含义见注释:0-失败;1-新增成功;2-更新成功
```

对应 SQL 是一条 MySQL upsert(`xxl-job-admin/src/main/resources/mapper/business/XxlJobRegistryMapper.xml:42`):

```sql
INSERT INTO xxl_job_registry(registry_group, registry_key, registry_value, update_time)
VALUES(...)
ON DUPLICATE KEY UPDATE update_time = #{updateTime}
```

- **第一次注册** → 插入新行,返回 1;
- **后续心跳** → 命中唯一键,只更新 `update_time`,返回 2。

也就是说,「心跳」在数据库层面就是**不停地刷新 `update_time`**。这条 upsert 能成立的前提是表上对 `(registry_group, registry_key, registry_value)` 建了**唯一索引**(可以在建表 SQL 里确认一下),它也顺便保证了重复注册的幂等。

> 顺带一个会让你对照原版时困惑的点:`ret == 1` 时会调 `freshGroupRegistryInfo()`(:217),但这个方法在本仓库里**是空的**,注释写着「暂不实现,避免影响核心表」。所以**注册不会同步刷新 group 地址列表**——刷新这件事完全交给了下面的监控线程。这是本项目和官方原版的一处有意差异(原版还是两步 update-then-insert,这里的旧代码以注释形式留在 :177-183 和 mapper 里)。

### 发动机②:监控线程(维护「谁还活着」)

对应 `registryMonitorThread`(:58-130),一个 **daemon 线程**,起来后 `while(!toStop)` 死循环,**每 30 秒**(`Const.BEAT_TIMEOUT`)跑一轮。每一轮做三件事:

**第 0 步——只管「自动注册」的组**(:64)
```java
findByAddressType(0)  // addressType=0 才是自动发现的组
```
手动配置地址的组(addressType=1)直接跳过,整个后续逻辑都被这一步「门控」住。

**第 1 步——清理死实例**(:68-71)
```java
List<Integer> ids = findDead(DEAD_TIMEOUT, now);  // update_time < now-90s 的都算死
if (!ids.isEmpty()) removeDead(ids);              // 直接 DELETE 掉
```

**第 2 步——汇总当前在线地址 + 回写 group**(:74-110)
```java
findAll(DEAD_TIMEOUT, now)   // 取 update_time > now-90s 的存活记录
  → 过滤出 registryGroup == EXECUTOR 的
  → 按 appname(registryKey)聚合成:appname → [去重后的地址列表]

for each group:
  取出该 appname 对应的地址列表 → 排序 → 用逗号拼成 "addr1,addr2,..."
  写回 group.addressList,update group  (:106-109)
```
如果某个 group 当前没有任何存活执行器,它的 `address_list` 会被置成 `null`。

整个循环体和 `sleep` 各自包了 `try/catch`(:62-123),并且只有在非停机状态才记 error——意味着**一次数据库抖动不会把监控线程搞挂**,它会记日志、睡一觉、下一轮继续,是个很典型的「常驻线程要自愈」的写法。

## 两个关键数字(`xxl-job-core/src/main/java/com/xxl/job/core/constant/Const.java`)

| 常量 | 值 | 含义 |
|---|---|---|
| `BEAT_TIMEOUT` | **30s** | 执行器心跳间隔,**同时**也是监控线程每轮的睡眠间隔 |
| `DEAD_TIMEOUT` | **90s**(= 30×3) | 超过这么久没刷新 `update_time` 就判定「死亡」 |

记住这条规则就够了:**漏掉 3 次心跳 → 判死**。这也是为什么执行器进程被 `kill -9`(没机会发「下线」请求)后,大约 1~1.5 分钟才会从地址列表里消失。

## 串起来:一个执行器的完整一生

1. **启动**:executor 第一次上报 → `registry()` 异步插入一行,`registry_group=EXECUTOR, registry_key=应用名, registry_value=http://ip:port/`。
2. **保活**:之后每 30s 上报一次 → upsert 不停刷新 `update_time`(返回 2)。
3. **被发现**:监控线程下一轮(≤30s 内)扫到它在线 → 把它的地址拼进对应 group 的 `address_list` → 此后调度才会把任务发给它。
4. **优雅下线**:executor 正常关闭 → 发 `registryRemove` → `registryDelete` 删行(:206)→ 下一轮监控把它从 `address_list` 摘掉。
5. **崩溃**:executor 被强杀,不再上报 → 90s 后 `update_time` 过期 → 监控线程 `findDead` 把它删除 → 从 `address_list` 摘掉。

## 接手时最该注意的几个坑

1. **地址列表是「最终一致」,不是注册即生效。** 因为 `freshGroupRegistryInfo` 是空的,新执行器从注册到出现在 `address_list` 里,**最多有一轮(~30s)延迟**;下线/崩溃的摘除最多 ~90s。排查「任务没发到新机器」时先想到这一点,别误以为注册成功就能立刻被调度。
2. **只对 addressType=0 的组生效。** 如果某个组配的是手动地址(addressType=1),这套自动发现逻辑完全不碰它。
3. **拒绝策略是 caller-runs。** 上报洪峰时不会丢数据,但会拖慢调用线程并打 warn 日志;线上看到 `registry or remove too fast` 这条 warn,说明上报压力打满了那个 2~10 大小的线程池。
4. **判死靠的是数据库时间窗口**(`now - 90s`),所以 **admin 和数据库的时钟**、以及各执行器 `update_time` 写入的及时性会直接影响判死准确性。
5. **和官方原版的差异**:本仓库用单条 `INSERT ... ON DUPLICATE KEY UPDATE` 取代了原版「先 update 再 insert」两步;数据库访问从 `XxlJobAdminConfig.getXxlJobRegistryDao()` 换成了 `XxlJobAdminBootstrap.getInstance().getXxlJobRegistryMapper()`。对着网上教程看时别被这些差异绊住。

---

# 第二章 下游链路:`address_list` 如何被用来选机器并发任务

本章接着讲**注册信息是怎么被「消费」的**——也就是 `address_list` 如何变成一次真正打到某台执行器的调用。触发主类是 `xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/trigger/JobTrigger.java`(原版的 `XxlJobTrigger`)。

## 0. 先接上第一章:那根「桥」

第一章说监控线程把存活地址拼成逗号字符串写进了 `xxl_job_group.address_list`。它被消费的入口只有一个——`XxlJobGroup.getRegistryList()`(`xxl-job-admin/src/main/java/com/xxl/job/admin/model/XxlJobGroup.java:24-29`):

```java
public List<String> getRegistryList() {
    if (StringTool.isNotBlank(addressList)) {
        registryList = new ArrayList<>(Arrays.asList(addressList.split(",")));  // 逗号串 → List
    }
    return registryList;
}
```

**一句话:注册侧产出「逗号字符串」,这里把它懒加载地拆成「候选地址列表」,路由就在这个列表上做选择。** 触发链路里看不到 `getAddressList()`,看到的全是 `getRegistryList()`,原因就在这。

## 1. 触发主类 `JobTrigger`

它是个 Spring `@Component`,对外只有一个入口 `trigger(...)`(`JobTrigger.java:63`)。所有触发来源——Cron 调度、API 手动触发、父子任务、失败重试——最后都汇聚到它。

`trigger()` 做的是「准备 + 分流」:

```java
XxlJobInfo jobInfo = xxlJobInfoMapper.loadById(jobId);              // 任务配置          :71
XxlJobGroup group  = xxlJobGroupMapper.load(jobInfo.getJobGroup()); // 执行器组(带地址) :80

// 如果显式传了 addressList,就临时覆盖该组地址(一次性手动指定机器)        :83-86
// 路由策略 == 分片广播 ?
//   是 → 遍历每个地址,对每台都 processTrigger 一次(index=i,total=N)   :102-104
//   否 → 只 processTrigger 一次,默认分片 {0,1}                          :105-110
```

真正干活的是 `processTrigger(...)`(:134)。它的六步非常值得记住,因为这就是 admin 端「调度日志」里每一条记录的来历:

```
processTrigger(group, jobInfo, ...):
  ① 先 insert 一条 xxl_job_log,拿到 logId            :148-152
  ② 组装 TriggerRequest(handler/参数/超时/logId/分片下标…) :156-168
  ③ 选地址:                                             :170-188
       registryList 为空 → 失败"地址为空"
       分片广播 → 按 index 取第 index 台
       其它策略 → routeStrategy.getRouter().route(param, registryList)
  ④ 发起远程调用 doTrigger(param, address)              :190-196
  ⑤ 拼出那段 HTML 触发日志(你在 UI 上看到的)            :198-235
  ⑥ 用结果回写这条 log(地址/触发码/触发信息)            :237-246
```

两个细节对新人很关键:

- **第①步先落 log、拿 `logId`,再把 `logId` 塞进 `TriggerRequest`**。执行器执行完后回调 admin(`AdminBizImpl.callback` → `JobCompleteHelper`)时,就是靠这个 `logId` 精准更新到同一行——这是「触发记录」和「执行结果」能对上的关键。
- **真正的 RPC 在 `doTrigger`(:258)**:
  ```java
  ExecutorBiz executorBiz = XxlJobAdminBootstrap.getExecutorBiz(address); // 指向该地址的执行器客户端
  Response<String> runResult = executorBiz.run(triggerParam);             // 远程调用 executor 的 run
  ```
  调用失败会捕获异常并记成失败,日志里那句 `please check if the executor is running` 就是从这里来的。

## 2. 选机器:路由策略

第③步的 `route(...)` 来自抽象基类 `ExecutorRouter`(`xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/route/ExecutorRouter.java`),契约很干净:

```java
public abstract Response<String> route(TriggerRequest triggerParam, List<String> addressList);
// 入参:候选地址列表;返回:Response.data = 选中的那一个地址
```

`ExecutorRouteStrategyEnum`(`xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/route/ExecutorRouteStrategyEnum.java`)把 10 种策略和各自的实现绑在一起。按「思路」分组记最省力:

| 分组 | 策略 | 怎么选 |
|---|---|---|
| 位置固定 | `FIRST` / `LAST` | 永远第一台 / 永远最后一台 |
| 负载均衡 | `ROUND` / `RANDOM` | 轮询 / 随机 |
| 带「亲和性」的有状态 | `CONSISTENT_HASH` | 同一 jobId 稳定路由到同一台,扩缩容时只少量重映射 |
| | `LFU` / `LRU` | 最不常用 / 最久未用优先 |
| 健康感知 | `FAILOVER` | 逐台**心跳探测**,选第一台**能应答**的 |
| | `BUSYOVER` | 类似 Failover,但探测的是「是否**空闲**」,选第一台不忙的 |
| 广播 | `SHARDING_BROADCAST` | 不走 router(枚举里它的 router 是 `null`),由 `JobTrigger` 特判,**发给每一台** |

挑两个最有代表性的看实现:

**轮询 `ExecutorRouteRound`(`route/strategy/ExecutorRouteRound.java:16-44`)——典型的「admin 内存里有状态」**
```java
count(jobId) % addressList.size()   // 每个 jobId 一个自增计数器,取模选下标
```
计数器放在静态 `ConcurrentMap` 里,每天清一次、超过百万重置,首次还随机一个 0~99 的起点避免冷启动都打到第 0 台。**隐含坑**:这个计数器是**单 admin JVM 内**的,多 admin 高可用部署时,轮询不是全局协调的。

**故障转移 `ExecutorRouteFailover`(`route/strategy/ExecutorRouteFailover.java:18-47`)——和注册机制是绝配**
```java
for (String address : addressList) {
    beatResult = executorBiz.beat();      // 逐台发心跳探测
    if (beatResult.isSuccess()) return ... // 第一台活着的就用它
}
```
为什么说它和第一章互补?注册侧判死是**滞后**的——一台机器要静默满 90s 才会被监控线程从 `address_list` 摘掉。也就是说,一台 10 秒前刚崩的机器,此刻**仍在候选列表里**。`FAILOVER` 在路由时**实时探测**,当场跳过这台死机,换一台活的。两者一个「事后清理地址表」、一个「事中实时探活」,配合起来才把「发到死机」的概率压到很低。

## 3. ⚠️ 一个最容易混淆的点:FAILOVER ≠ 失败重试

新人极易把这两个当成一回事,其实是两套独立机制:

- **`FAILOVER`(路由策略)**:发出去**之前**,主动探活,避开死节点。是**预防**。
- **`executorFailRetryCount`(失败重试)**:发出去**之后**真的失败了,再重试 N 次。是**补救**。在 `JobTrigger` 里你只看到它把 `finalFailRetryCount` 算出来并**写进 log**(:242),**并没有在这里循环重试**;真正的重试由**独立的失败监控线程**读取「还有剩余重试次数」的 log,再回头调一次 `trigger(...)`。

所以一个任务完全可以「路由用 ROUND + 失败重试 3 次」,两者正交。

## 4. 把整条链路串起来

```
Cron/手动/重试 → JobTrigger.trigger()
   ├─ load jobInfo + group
   ├─ group.getRegistryList()  ← 这就是第一章监控线程写的 address_list
   ├─ processTrigger():
   │     ① 落 log 拿 logId
   │     ② 组装 TriggerRequest(含 logId)
   │     ③ 路由策略选 1 台(或分片广播选每一台)
   │     ④ doTrigger → executorBiz.run() 远程打到执行器
   │     ⑥ 回写 log(触发码/信息)
   ▼
执行器执行 handler → 回调 admin(callback,带 logId)→ 更新同一行 log 的执行结果
```

至此闭环:**注册侧负责「有哪些机器可用」,触发侧负责「挑一台/广播并把任务发过去」,回调负责「把结果填回那条日志」。**

## 5. 接手时值得留意的几点

1. **路由只看 `registryList`,而它来自 30s 一刷的 `address_list`**。新执行器从注册到能被路由到,有第一章说的最多 ~30s 延迟;排查「任务没发到新机器」,先确认它已经进了组的地址列表。
2. **手动触发可临时覆盖地址**(`trigger` 的 `addressList` 入参,`JobTrigger.java:83-86`),会把组当成「手动录入(addressType=1)」处理——调试单台机器时很有用。
3. **有状态策略(ROUND/LRU/LFU/一致性哈希计数)的状态都在单个 admin 的内存里**,HA 多实例下不是全局一致的;重启 admin 会丢这些状态。
4. **`getRegistryList()` 每次调用都重新 `split` 一遍**,`processTrigger` 里调了好几次——量大时是个微小的重复开销,知道即可,通常不用管。
5. **分片广播是在 `JobTrigger` 里特判的**(它的 router 为 `null`),给每台传 `index/total`,具体怎么分片是**执行器侧**用 `XxlJobHelper.getShardIndex()/getShardTotal()` 自己实现的——admin 只负责把下标发下去。

---

# 附:可继续深入的方向

顺着这条链路往下/往上,还有两个最自然的方向:

1. **回调链路**:`JobCompleteHelper` 怎么用 `logId` 落执行结果、怎么触发失败重试和告警。
2. **调度触发的源头**:Cron 是怎么被扫描、怎么进 `JobTriggerPoolHelper` 的快/慢线程池的。

---

> 本文档由对仓库源码的实际阅读整理而成,所有行号、SQL、常量值均来自当前分支代码,可直接对照源文件核对。
