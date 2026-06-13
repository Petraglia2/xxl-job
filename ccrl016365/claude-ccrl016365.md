# xxl-job admin 调度报表统计：`JobLogReportHelper.java` 讲解

> 面向刚接手项目的后端工程师。目标是让你既看懂 `JobLogReportHelper` 做什么，也理解它"为什么这么写"。
>
> 文件位置：`xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/thread/JobLogReportHelper.java`

---

## 一、一句话定位

`JobLogReportHelper` 是 admin 端的一个**后台守护线程**，每分钟干两件事：

1. **统计报表**：把最近 3 天每天的调度日志，按"运行中 / 成功 / 失败"汇总进 `xxl_job_log_report` 表；
2. **清理日志**：每天一次，把超过保留期的历史调度日志从 `xxl_job_log` 表里批量删除。

它是调度报表数据的**生产者**。你在管理台首页看到的那张"调度报表"趋势图，读的就是它写出来的这张汇总表——而不是每次都去扫几百万行的明细日志表。这就是它存在的核心价值：**用一张预聚合的小表，换掉对大表的实时扫描。**

---

## 二、它在系统里的位置（生命周期）

它不是 Spring Bean，而是被 `XxlJobAdminBootstrap` 在启动时手动 `new` 出来并 `start()` 的：

```java
// XxlJobAdminBootstrap.doStart()  —— 由 InitializingBean.afterPropertiesSet 触发
jobLogReportHelper = new JobLogReportHelper();
jobLogReportHelper.start();
// 关闭时（DisposableBean.destroy → doStop）
jobLogReportHelper.stop();
```

正因为它不是 Bean、拿不到 `@Resource` 注入，所以它访问数据库的方式是**走静态单例**：

```java
XxlJobAdminBootstrap.getInstance().getXxlJobLogMapper()...
XxlJobAdminBootstrap.getInstance().getXxlJobLogReportMapper()...
```

> 🔑 新人记忆点：admin 里这一批 `XxxHelper`（触发池、注册、报警、回调、调度、报表）都是这个套路——普通对象 + 自己起线程 + 通过 `XxlJobAdminBootstrap.getInstance()` 拿依赖。看到 `getInstance()` 别困惑，它就是这个项目的"全局上下文"。

线程本身的写法体现了几个工程习惯：

```java
logReportThread.setDaemon(true);                          // 守护线程，不会阻止 JVM 退出
logReportThread.setName("xxl-job, admin JobLogReportHelper"); // 命名，线程 dump 里一眼能认出
logReportThread.start();
```

主循环骨架是：

```java
while (!toStop) {
    try { ...统计报表... }  catch (Throwable e) { if (!toStop) log.error(...); }
    try { ...清理日志... }  catch (Throwable e) { if (!toStop) log.error(...); }
    try { TimeUnit.MINUTES.sleep(1); } catch (...) { ... }
}
```

这里有两个值得学的健壮性细节：

- **两件事各自独立 try/catch(Throwable)**：哪怕统计这一步因为数据库抖动抛异常，也只是这一分钟跳过、记一条错误日志，下一分钟照常继续；**绝不会因为一次异常把整个线程搞死**。
- **`if (!toStop)` 守卫**：关闭时 `stop()` 会 `interrupt()` 线程，正在 `sleep` 的线程会抛 `InterruptedException`。用这个判断避免在"正常关机"时打出一堆吓人的错误日志。

`stop()` 是标准的优雅停止三连：

```java
toStop = true;               // 1. 置标志位，让 while 自然退出
logReportThread.interrupt(); // 2. 打断 sleep，让它立刻醒来而不用等满 1 分钟
logReportThread.join();      // 3. 等线程真正跑完收尾
```

---

## 三、职责一：报表统计（每分钟刷新最近 3 天）

这是整个类的核心。代码逻辑：

```java
for (int i = 0; i < 3; i++) {       // i = 0,1,2 → 今天、昨天、前天
    // 用 Calendar 算出这一天的 [00:00:00.000, 23:59:59.999]
    Date todayFrom = ...;  // 当天 0 点
    Date todayTo   = ...;  // 当天 23:59:59.999

    XxlJobLogReport report = new XxlJobLogReport();
    report.setTriggerDay(todayFrom);  // 以"当天 0 点"作为这条统计的主键标识
    report.setUpdateTime(new Date());

    // 关键：聚合查询
    Map<String,Object> m = ...getXxlJobLogMapper().findLogReport(todayFrom, todayTo);
    int total   = m.get("triggerDayCount");
    int running = m.get("triggerDayCountRunning");
    int suc     = m.get("triggerDayCountSuc");
    int fail    = total - running - suc;   // ← 失败是"减"出来的

    report.setRunningCount(running);
    report.setSucCount(suc);
    report.setFailCount(fail);

    ...getXxlJobLogReportMapper().saveOrUpdate(report);  // 写入/更新
}
```

### 3.1 数据是怎么算出来的：`findLogReport` 这条 SQL

```sql
SELECT
  IFNULL(COUNT(handle_code),0) triggerDayCount,
  IFNULL(SUM(CASE WHEN (trigger_code in (0,200) and handle_code = 0) then 1 else 0 end),0) as triggerDayCountRunning,
  IFNULL(SUM(CASE WHEN handle_code = 200 then 1 else 0 end),0) as triggerDayCountSuc
FROM xxl_job_log
WHERE trigger_time BETWEEN #{from} and #{to}
```

理解这条 SQL 的钥匙是：**`trigger_code`（调度结果码）和 `handle_code`（执行结果码）都用类似 HTTP 状态码的约定**——`0` 表示"还没有结果/初始态"，`200` 表示"成功"，其它值表示"失败"。

| 指标 | 判定条件 | 含义 |
|---|---|---|
| **total** 总数 | 当天所有日志 | 当天一共触发了多少次 |
| **running** 运行中 | `trigger_code∈(0,200)` 且 `handle_code=0` | 调度没失败（已下发或待下发），但**执行结果还没回来** |
| **suc** 成功 | `handle_code = 200` | 执行器明确回调"成功" |
| **fail** 失败 | `total − running − suc`（Java 里算） | 剩下的全算失败 |

**为什么失败要用"减法"而不是直接 SQL 统计？** 因为"失败"的情况太杂——可能是调度阶段就失败（比如没有可用执行器）、可能是执行器回调了非 200 的错误码、也可能是结果丢失被判失败。与其在 SQL 里把所有失败码枚举一遍（容易漏），不如反过来：**凡是既不"运行中"也不"成功"的，一律计入失败**。这样口径稳定、不会漏。

### 3.2 为什么要重复刷"最近 3 天"，而不是只刷今天？

这是新人最容易疑惑的点。原因在于**一次调度的生命周期不是瞬时的**：

> admin 先触发任务 → 执行器去跑 → 跑完后**异步回调**把 `handle_code` 写回来。

所以一条昨天触发的日志，今天才收到回调、状态从"运行中"翻成"成功/失败"是很正常的（长任务、回调延迟、结果补偿等）。如果只刷今天，昨天那些"迟到结算"的记录就永远停留在错误的"运行中"状态了。

**做法**：每分钟把最近 3 天整体重算一遍——这 3 天的状态还可能变化，需要持续校正；超过 3 天的数据则认为已经"尘埃落定"，不再回头改，省掉无谓的重复计算。

### 3.3 为什么每分钟刷一次、且能反复刷不出错？

- **每分钟**：让首页报表接近实时，同时又不至于把数据库压垮。
- **可反复刷（幂等）**：靠的是 `saveOrUpdate` 这条 SQL + 表上的唯一键。

```sql
INSERT INTO xxl_job_log_report (trigger_day, running_count, suc_count, fail_count, update_time)
VALUES (...)
ON DUPLICATE KEY UPDATE
  running_count = #{runningCount}, suc_count = #{sucCount},
  fail_count   = #{failCount},     update_time = #{updateTime}
```

```sql
-- 表结构里的关键约束
UNIQUE KEY `i_trigger_day` (`trigger_day`)
```

`trigger_day`（当天 0 点）是唯一键，所以"当天第一次统计 → INSERT 新行；之后每分钟 → 命中唯一键走 UPDATE"。**一天对应一行，反复跑也只会刷新那一行**，这就是为什么可以放心地每分钟重算 3 天。

> 顺带一提：你会看到文件里有一大段被注释掉的 `save`/`update` 以及 `if (ret < 1) save(...)` 的旧代码。那是历史演进的痕迹——早期是"先 update、失败再 insert"两步，后来统一成一条 `ON DUPLICATE KEY UPDATE` 的 upsert。看到这些"死代码"知道是历史包袱即可。

### 3.4 谁来读这张表？

`XxlJobLogReportMapper` 里还有两个查询方法，供报表页面使用，和本类形成"写/读"闭环：

- `queryLogReport(from, to)`：按日期区间取出每天的统计 → 画趋势折线图；
- `queryLogReportTotal()`：`SUM` 出运行/成功/失败的总数 → 首页的总览数字。

记住：**`JobLogReportHelper` 只负责"写"，报表 Controller 负责"读"。**

---

## 四、职责二：清理过期日志（每天最多一次）

```java
if (getLogretentiondays() > 0
        && System.currentTimeMillis() - lastCleanLogTime > 24*60*60*1000) {

    Date clearBeforeTime = ...; // 当前往前推 logretentiondays 天的那天 0 点

    List<Long> logIds;
    do {
        logIds = ...findClearLogIds(0, 0, clearBeforeTime, 0, 1000); // 一次最多取 1000 个
        if (logIds != null && !logIds.isEmpty()) {
            ...clearLog(logIds);  // 删掉这一批
        }
    } while (logIds != null && !logIds.isEmpty()); // 直到没有过期日志为止

    lastCleanLogTime = System.currentTimeMillis();
}
```

要点：

1. **触发条件有两道闸**：保留天数配置开启（`>0`）**且**距上次清理已超过 24 小时。`lastCleanLogTime` 是线程内的局部变量，初始为 0——意味着 **admin 每次启动后第一轮循环就会先清一次**，之后每天一次。
2. **保留天数有个隐藏门槛**：看 `getLogretentiondays()`：

   ```java
   public int getLogretentiondays() {
       if (logretentiondays < 3) return -1; // 配置 < 3 天，直接当"关闭"处理
       return logretentiondays;
   }
   ```
   即 `xxl.job.logretentiondays` 配成小于 3 的值，会被视为 `-1`（关闭清理）。这是个容易踩的坑——想保留 2 天是不生效的，最小有效值是 3。

3. **为什么循环 + 每批 1000 删？** 过期日志可能成千上万，一条 `DELETE` 删几十万行会长时间锁表、撑大事务、甚至超时。所以用"每次捞 1000 个 id、删掉、再捞下一批"的方式**分批小步删**，把对线上的影响降到最低。

4. **`findClearLogIds(0, 0, clearBeforeTime, 0, 1000)` 的参数含义**（这条 SQL 是通用的，页面手动清理也复用它）：`jobGroup=0`、`jobId=0` 表示不限分组/任务（全量）；`clearBeforeTime` 表示删这个时间点之前的；`clearBeforeNum=0` 表示不启用"保留最近 N 条"的逻辑。这里走的是纯粹的"按时间过期删除"。

---

## 五、整体数据流（一张图记住）

```
执行器异步回调 handle_code
          │
          ▼
   xxl_job_log（海量明细）
          │  每分钟：findLogReport 聚合最近 3 天
          ▼
   xxl_job_log_report（每天一行的汇总）  ←── saveOrUpdate（按 trigger_day upsert）
          │  queryLogReport / queryLogReportTotal
          ▼
     管理台报表页（趋势图 + 总览）

   xxl_job_log（海量明细）
          │  每天一次：findClearLogIds + clearLog，分批 1000 删除
          ▼
     超过保留期的旧日志被清掉
```

---

## 六、接手时几个"要知道的点"

- **它是写报表的源头**：报表数字不对，先来这里和 `findLogReport` 的 SQL 看口径，而不是去查页面。
- **"失败数"是减出来的**：`fail = total − running − suc`。任何不是"明确成功"也不是"还在跑"的，都算失败——改判定逻辑时务必保持这个总账平衡。
- **只校正最近 3 天**：更早的历史数据是冻结的，不会因为后来的回调而改。如果业务上需要修订更久以前的统计，得另想办法。
- **清理保留天数最小是 3**：小于 3 等于关闭。
- **重启会立刻触发一次清理**（`lastCleanLogTime` 是内存里的局部变量，不持久化）。
- **改动这里要考虑幂等和大表压力**：upsert 的幂等性依赖 `trigger_day` 唯一键；删除的安全性依赖分批。这两条是它能"每分钟跑、跑很多年都稳"的根基，别轻易破坏。
