# JobLogReportHelper.java 深入浅出讲解

> 面向刚接手 xxl-job 项目的后端工程师

---

## 一句话总结

`JobLogReportHelper` 是 admin 端后台**常驻线程**，每分钟做两件事：
1. **统计近 3 天的任务执行报表**（运行中/成功/失败各多少条）
2. **每天清理一次过期日志**（按配置保留天数删除旧日志）

---

## 在整个系统中的位置

```
XxlJobAdminBootstrap.start()
    ├── JobTriggerPoolHelper.start()     // 触发线程池
    ├── JobRegistryHelper.start()        // 执行器注册
    ├── JobFailAlarmMonitorHelper.start()// 失败告警
    ├── JobCompleteHelper.start()        // 任务完成回调
    ├── JobLogReportHelper.start()       // ← 你在这里：报表统计 + 日志清理
    └── JobScheduleHelper.start()        // 调度触发（依赖上面的线程池）
```

它随 admin 启动而启动，随 admin 关闭而停止，是一个**守护线程**（`setDaemon(true)`），不会阻止 JVM 退出。

---

## 核心结构

```java
public class JobLogReportHelper {
    private Thread logReportThread;      // 工作线程
    private volatile boolean toStop;     // 停止标志，volatile 保证可见性

    public void start()  { ... }         // 启动线程，进入主循环
    public void stop()   { ... }         // 设置 toStop=true，interrupt + join 等待线程结束
}
```

---

## 主循环逻辑（每分钟一次）

### 第一步：刷新近 3 天的报表

```java
for (int i = 0; i < 3; i++) {
    // i=0 → 今天，i=1 → 昨天，i=2 → 前天
    // 构造当天 00:00:00.000 ~ 23:59:59.999 的时间窗口
    ...
    // 从 xxl_job_log 表按时间范围聚合统计
    Map<String, Object> triggerCountMap = xxlJobLogMapper.findLogReport(todayFrom, todayTo);

    // 写入/更新 xxl_job_log_report 表（saveOrUpdate，按 triggerDay 唯一）
    xxlJobLogReportMapper.saveOrUpdate(xxlJobLogReport);
}
```

**对应的 SQL（XxlJobLogMapper.xml）：**

```sql
SELECT
    IFNULL(COUNT(handle_code), 0)                                          triggerDayCount,
    IFNULL(SUM(CASE WHEN trigger_code IN (0,200) AND handle_code=0
                    THEN 1 ELSE 0 END), 0)                               triggerDayCountRunning,
    IFNULL(SUM(CASE WHEN handle_code=200 THEN 1 ELSE 0 END), 0)          triggerDayCountSuc
FROM xxl_job_log
WHERE trigger_time BETWEEN #{from} AND #{to}
```

**三个统计字段的含义：**

| 字段 | 含义 | 判断条件 |
|------|------|----------|
| `triggerDayCount` | 当天触发的总日志数 | 所有记录 |
| `triggerDayCountRunning` | 仍在运行中的数量 | trigger_code 为 0 或 200，且 handle_code=0（还没回调） |
| `triggerDayCountSuc` | 成功完成的数量 | handle_code=200 |
| `triggerDayCountFail`（代码计算） | 失败数量 | **总数 − 运行中 − 成功**（差值即失败，含超时未回调等） |

> `trigger_code` 是调度器触发时的状态码（200=成功触发）；`handle_code` 是执行器回调时的结果码（200=执行成功，0=还没回调）。这两个字段是理解整个日志状态机的关键。

### 第二步：清理过期日志

```java
// 开关：logretentiondays > 0 才开启清理
// 频率：距上次清理超过 24 小时
if (logretentiondays > 0 && now - lastCleanLogTime > 24h) {

    // 计算过期截止时间 = 今天零点 - N 天
    Date clearBeforeTime = ...;

    // 分批删除，每次 1000 条，直到删完
    do {
        logIds = xxlJobLogMapper.findClearLogIds(0, 0, clearBeforeTime, 0, 1000);
        if (logIds != null && !logIds.isEmpty()) {
            xxlJobLogMapper.clearLog(logIds);
        }
    } while (logIds != null && !logIds.isEmpty());

    lastCleanLogTime = System.currentTimeMillis();
}
```

**为什么要分批删除（每次 1000 条）？**
- 避免一次性 DELETE 大量数据导致**长事务、锁表、主从延迟**
- 是生产环境中处理大批量删除的标准做法

### 第三步：休眠 1 分钟

```java
TimeUnit.MINUTES.sleep(1);
```

整个循环每 60 秒重复一次。注意 sleep 在 try/catch 里，被 interrupt 时会捕获异常但**不退出循环**（靠 `toStop` 标志控制退出）。

---

## 启停机制

### 启动
```java
logReportThread.setDaemon(true);   // 守护线程，JVM 退出时自动终止
logReportThread.setName("xxl-job, admin JobLogReportHelper");
logReportThread.start();
```

### 停止
```java
public void stop() {
    toStop = true;                  // 1. 设置标志，循环下一轮判断时退出
    logReportThread.interrupt();    // 2. 中断当前 sleep，立即唤醒
    logReportThread.join();         // 3. 阻塞等待线程真正结束
}
```

这是 xxl-job 中所有后台线程的**标准停止模式**（flag + interrupt + join），理解它有助于阅读其他 Helper 类。

---

## 数据流全景

```
xxl_job_log（原始日志表，每次任务执行写入一条）
        │
        │  findLogReport() 聚合统计
        ▼
xxl_job_log_report（报表汇总表，按 triggerDay 一天一行）
        │
        │  前端 Dashboard 读取
        ▼
    管理员看到的调度统计图表
```

---

## 容易踩坑的地方

| 问题 | 说明 |
|------|------|
| 报表只保留近 3 天 | `for (i=0; i<3)` 写死了，更早的数据不会被刷新（但也不删除，仍留在表里） |
| 日志清理默认关闭 | `logretentiondays` 需 >0 才触发清理，默认值为 -1（不自动清理） |
| `triggerDayCountFail` 是差值 | 不是直接 COUNT，而是 `total - running - suc`，若有脏数据可能出现负数 |
| sleep 被中断不退出 | interrupt 只是跳出 sleep，真正退出靠 `toStop` 标志判断 |
| `saveOrUpdate` 幂等 | 每分钟会重复刷新同一天的数据，靠 `triggerDay` 做唯一键实现 upsert |

---

## 扩展：如果要加"统计近 7 天"怎么改？

只需把 `for (int i = 0; i < 3; i++)` 改成 `i < 7`——但要同步确认前端图表展示范围和 `xxl_job_log_report` 表的查询范围是否匹配。
