# xxl-job admin 调度核心 `JobScheduleHelper` 讲解

> 面向刚接手该项目的后端工程师。目标:读完后既能建立**整体心智模型**,又能看懂每一处不那么直观的设计。
> 所有引用都标了 `文件:行号`,可对照源码:`xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/thread/JobScheduleHelper.java`

---

## 一句话定位

`JobScheduleHelper` 是 admin 端调度器的**心脏**:它负责回答"**哪个任务、在哪一秒该被触发**",然后把"触发"这个动作甩给线程池去执行。它本身**不**负责"怎么触发"(分片、路由、failover、发 HTTP 给执行器),那是 `XxlJobTrigger` 的事。

> 记住这条边界:**`JobScheduleHelper` 管 *when*,`XxlJobTrigger` 管 *how/where*。**

它由 `XxlJobAdminBootstrap` 在应用启动/关闭时调用 `start()` / `stop()`(类里所有依赖都通过 `XxlJobAdminBootstrap.getInstance().getXxx()` 拿到)。

---

## 先建立直觉:它要解决的三个难题

设想你有几千个任务,每个都有自己的 cron 表达式。最朴素的做法是"每秒扫一遍数据库,看谁到点了"。这会带来三个问题,而这个类的设计正是针对它们:

| 难题 | 朴素做法的毛病 | 本类的对策 |
|------|----------------|------------|
| **集群下重复调度** | 多个 admin 节点会把同一个任务触发多次 | **DB 分布式锁**(`FOR UPDATE`) |
| **数据库压力 / 整点风暴** | 每秒全表扫、整分钟一堆任务挤在一起 | **预读(pre-read)**:提前 5 秒批量捞 |
| **秒级精度** | 扫库有延迟,触发时刻不准 | **时间轮(time-ring)**:内存里按秒精确派发 |

这三招对应到代码,就是两条线程 + 一把锁。

---

## 核心架构:两条线程的"生产者 / 消费者"

整个类启动后跑两个 **daemon 线程**(`start()`,`JobScheduleHelper.java:45`):

```
  scheduleThread (生产者)                    ringThread (消费者)
  每秒扫库、预读未来 5 秒的任务      ──ringData──>   每秒取出"本秒该触发"的任务
  把任务放进 60 格的时间轮            (内存 Map)      调 triggerPool 真正触发
```

中间的纽带是一个内存结构 `ringData`(`JobScheduleHelper.java:40`):

```java
Map<Integer, List<Integer>> ringData   // key = 秒(0~59), value = 该秒要触发的 jobId 列表
```

这就是"时间轮":一个 60 格的环,第几秒该触发哪些任务,就挂在第几格里。**抓住这个生产者/消费者模型,后面全是细节。**

---

## scheduleThread 深入(生产者)

它是一个 `while (!scheduleThreadToStop)` 大循环,每一圈做这几件事:

### ① 启动时对齐到整秒

```java
TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis()%1000);   // :54
```

让循环踩在整秒边界上,后面所有"按秒"的逻辑才准。

### ② 开事务 + 抢分布式锁(HA 的关键)

```java
transactionStatus = ...getTransaction(...);                       // :75 开事务
String lockedRecord = ...getXxlJobLockMapper().scheduleLock();    // :77 抢锁
```

`scheduleLock()` 的 SQL 是:

```sql
SELECT * FROM xxl_job_lock WHERE lock_name = 'schedule_lock' FOR UPDATE
```

这是一把**行级悲观锁**,在事务提交前一直持有。集群里多个 admin 节点同时跑到这里,**只有一个能拿到锁往下走,其余的都阻塞在这一行**,直到持锁者提交事务释放。这就是 xxl-job 高可用调度"不重复触发"的根基——也是为什么整个"扫库+回写"被包在**一个事务**里(`finally` 里 commit 时的注释写着 `avoid schedule repeat`,`:163`)。

### ③ 预读:提前 5 秒捞任务

```java
List<XxlJobInfo> scheduleList =
    ...scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);   // :81
```

对应 SQL:

```sql
SELECT ... FROM xxl_job_info
WHERE trigger_status = 1                       -- 1 = RUNNING(运行中)
  AND trigger_next_time <= #{now + 5000}       -- 未来 5 秒内(含已过期)该触发的
ORDER BY id ASC LIMIT #{preReadCount}
```

- `PRE_READ_MS = 5000`(`:30`):**这就是"预读"二字的来源**——不是只捞"现在到点的",而是把未来 5 秒要触发的一起捞出来,交给时间轮去精确派发。这样扫库频率可以低(秒级甚至更低),却不影响触发精度。
- `preReadCount = (fastMax + slowMax) * 10`(`:63`):一次最多捞多少。注释给了算法——假设一次触发约耗 100ms,则一个线程每秒约处理 10 次,所以上限 ≈ 线程池总大小 × 10。**本质是:别捞超过线程池一秒能消化的量。**

### ④ 三分支:每个任务怎么处理(整个类最该看懂的地方)

捞出来的每个任务,按"**当前时间 `now`** 相对**它的应触发时刻 `next`**"落到三种情况之一。下面这张时间轴是理解的关键:

```
 过去  ←─────[ next ]────[ next+5s ]────[ now ]────────→  未来
                                                ↑ 你在这里扫库

 ┌─────────────────────────┬──────────────────┬───────────────────────┐
 │ 2.1  next+5s <  now      │ 2.2  next ≤ now   │ 2.3  now < next        │
 │      过期超过 5 秒        │      ≤ next+5s     │      (且 next ≤ now+5s) │
 │      → 走 misfire 策略    │  到点/过期≤5s      │      未来 5 秒内才到点   │
 │                          │  → 立即直接触发    │      → 放进时间轮       │
 └─────────────────────────┴──────────────────┴───────────────────────┘
```

**分支 2.1 —— 错过太久(misfire)**(`:88`)
任务过期超过 5 秒(比如 admin 宕机过、或本节点刚才长时间卡在锁上)。这时不能傻傻补触发,而是按用户配的**调度过期策略**处理:

```java
MisfireStrategyEnum.match(...).getMisfireHandler().handle(jobInfo.getId());  // :92-93
```

策略只有两种(`MisfireStrategyEnum`):`DO_NOTHING`(忽略,啥也不干)或 `FIRE_ONCE_NOW`(立刻补一次)。处理完刷新下次时间。

**分支 2.2 —— 到点了 / 刚过期一点点,立即触发**(`:98`)

```java
...getJobTriggerPoolHelper().trigger(jobInfo.getId(), TriggerTypeEnum.CRON, ...);  // :102
refreshNextTriggerTime(jobInfo, new Date());                                       // :106
```

这里有个**容易忽略的嵌套逻辑**(`:109`):触发完算出新的"下次时间"后,如果**新的下次时间仍落在 `now+5s` 内**且任务还在运行,就**额外把它压进时间轮、再刷新一次**。为什么?因为像"每 1~2 秒"这种高频任务,下一次触发可能在本轮扫库和下一轮之间就到了——直接挂上时间轮,就不必等下一次扫库,避免漏触发。

**分支 2.3 —— 未来 5 秒内才到点,放进时间轮**(`:123`)

```java
int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);  // :127 算出落在第几秒格
pushTimeRing(ringSecond, jobInfo.getId());                        // :130 挂进时间轮
refreshNextTriggerTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));  // :134
```

这是**最常态**的路径:任务还没到点,但在预读窗口内,于是按它真正的触发秒数挂进时间轮,交给 `ringThread` 到点精确触发。

### ⑤ 批量回写下次触发时间

三分支都调了 `refreshNextTriggerTime`(只改内存对象),最后统一批量写回 DB(`:144`):

```java
List<List<XxlJobInfo>> batches = CollectionTool.split(scheduleList, batchSize);
for (batch : batches) ...scheduleBatchUpdate(batch);   // 拆成 batchSize 一批,减少往返
```

用一条 `UPDATE ... SET x = CASE id WHEN ... THEN ...` 把一批任务一次更新掉,比逐条 update 高效得多(旧的逐条写法在 `:141` 被注释掉了)。

### ⑥ 提交事务(释放锁) + 智能休眠

```java
...commit(transactionStatus);   // :163 提交 → 释放 FOR UPDATE 锁,别的节点才能进
```

然后是一段**精心设计的休眠**(`:174`):

```java
if (cost < 1000) {   // 这一圈没超过 1 秒才睡
    sleep( (preReadSuc ? 1000 : 5000) - now%1000 );
}
```

- 这轮**捞到了**任务(`preReadSuc=true`)→ 睡到下一个整秒,**每秒扫一次**;
- 这轮**没捞到**任务 → 退避到 `5000`,**约每 5 秒扫一次**,空闲时省下数据库开销;
- 如果这轮扫库本身**超过 1 秒**(`cost >= 1000`)→ 干脆不睡,立刻进下一圈追赶。

---

## ringThread 深入(消费者)

逻辑短得多(`:196`),每秒做一次:

```java
sleep(1000 - now%1000);                    // 对齐到整秒
int nowSecond = ...get(Calendar.SECOND);
for (int i = 0; i <= 2; i++) {             // 当前秒 + 往前 2 秒,共 3 格
    List<Integer> list = ringData.remove((nowSecond+60-i)%60);   // 取出并清空该格
    ...distinct...                          // 去重
}
for (int jobId : ringItemData) {
    ...getJobTriggerPoolHelper().trigger(jobId, ...);   // 真正触发
}
```

两个细节,源码里用中文注释专门标了:

- **为什么取"当前秒 + 前 2 秒"3 格?**(`:217`)——**避免漏触发**。万一某一拍处理太久、跨过了刻度,下一拍把前两秒一并补上,确保不丢。
- **为什么 `distinct` 去重?**(`:221`)——**避免重复触发**。同一任务可能被重复压入同一刻度(比如 2.2 分支的二次推送),去重后只触发一次;若发现重复还会打 `warn` 日志。

---

## 三个支撑方法 + 一个"坑"

**`refreshNextTriggerTime`(`:262`)——算下次触发时间**
用调度类型策略(`ScheduleTypeEnum`:`CRON` 用 cron 表达式、`FIX_RATE` 固定频率)算出下一次时间。

> ⚠️ **新人最容易看懵的一行:`jobInfo.setTriggerStatus(-1)`(`:271`)**
> 算成功时,它把状态置成 `-1`。但 `-1` **永远不会被写进数据库**——这是个**内存哨兵值**。看 `scheduleBatchUpdate` 的 SQL:`trigger_status = CASE id WHEN ... THEN (triggerStatus >= 0 ? 该值 : 保持原值)`。也就是说 `-1` 表示"**我只更新时间,别动运行/停止状态**"。
> 反之,算**失败**时(cron 写错、或已无下次时间)会置成 `STOPPED`(值为 0,`:276`),0 ≥ 0 会被真正写库,从而**自动把这个任务停掉**。

**`pushTimeRing`(`:299`)**:`computeIfAbsent` 往 `ringData` 的对应秒格塞 jobId,就这么简单。

**`stop()`(`:313`)——优雅停机**:先停 scheduleThread(置标志 → 等 1 秒 → 没停就 `interrupt()` + `join()`);若时间轮里**还有没触发完的任务**,最多再等 `ELEGANT_SHUTDOWN_WAITING_SECONDS = 10` 秒让它们发出去;最后才停 ringThread。目的是**关闭时尽量不丢已经排好队的触发**。

---

## 触发之后去哪了?(顺带认识 `JobTriggerPoolHelper`)

两条线程最终都调 `jobTriggerPoolHelper.trigger(...)`。这个类(`JobTriggerPoolHelper.java`)做了一件聪明事——**快慢线程池隔离**:

- 正常任务进 `fastTriggerPool`;
- 如果某任务在**一分钟内触发耗时 > 500ms 累计超过 10 次**(`:110`、`:137`),就被判定为"慢任务",降级到 `slowTriggerPool`。

这样**慢任务不会把快任务的线程占满**,保护整体调度吞吐。真正"发请求给执行器"还要再往里一层 `getJobTrigger().trigger(...)`(`:123`),那才是 `XxlJobTrigger`,处理分片、路由、failover。

---

## 给你的"上手备忘录"

如果只记 6 条,记这些:

1. **两条线程**:`scheduleThread` 扫库预读做生产者,`ringThread` 按秒消费时间轮——靠内存 `ringData` 连接。
2. **`FOR UPDATE` 那把锁 = 集群 HA 的根**:整个扫库+回写包在一个事务里,提交才释放锁,保证多节点不重复调度。
3. **预读 5 秒(`PRE_READ_MS`)**:提前捞、用时间轮派发,兼顾低扫库频率与秒级精度。
4. **三分支按 `now` vs `next` 划分**:过期>5s 走 misfire / 到点±5s 直接触发 / 未来5s内进时间轮——这是全类的逻辑核心。
5. **`triggerStatus = -1` 是哨兵,不入库**:意思是"只更时间别动状态";失败时置 0 才会真的停任务。
6. **这里只决定"何时触发",不发远程请求**;真正的执行器调用在 `XxlJobTrigger`。
