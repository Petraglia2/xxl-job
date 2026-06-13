# xxl-job admin 端 `JobFailAlarmMonitorHelper` 讲解

> 面向刚接手项目的后端工程师。目标是让你**既看懂这段代码做什么,又理解它为什么这么写**。内容基于真实源码(`xxl-job-admin/.../scheduler/thread/JobFailAlarmMonitorHelper.java` 及其协作类、关键 SQL)。

## 一句话定位

`JobFailAlarmMonitorHelper` 是 admin 端一个**常驻后台守护线程**,每隔 10 秒扫描一次"执行失败"的任务日志,对每条失败日志做两件事:**①按配置自动重试、②发失败告警(目前是邮件)**。

它和它的协作者构成这样一张图:

```
JobFailAlarmMonitorHelper  (扫描 + 调度,本文主角)
        │  对每条失败日志调用
        ▼
   JobAlarmer        (告警分发器,把一条告警广播给所有告警渠道)
        │  遍历调用
        ▼
   JobAlarm (接口)   ←── EmailJobAlarm (邮件实现,目前唯一渠道)
```

## 核心:那个 `while` 循环在干什么

主逻辑全在 `start()` 里起的那个线程的 `run()` 方法(`JobFailAlarmMonitorHelper.java:38-88`)。一轮循环的步骤:

**第 1 步 — 捞失败日志** (`:41`)

```java
List<Long> failLogIds = ...getXxlJobLogMapper().findFailJobLogIds(1000);
```

一次最多取 1000 条"失败且还没处理过告警"的日志 id。**什么叫"失败"是这段代码最容易看懵的地方,见下面专门一节。**

**第 2 步 — 抢锁(关键)** (`:46-49`)

```java
int lockRet = ...updateAlarmStatus(failLogId, 0, -1);
if (lockRet < 1) { continue; }
```

把这条日志的 `alarm_status` 从 `0`(默认)原子地改成 `-1`(锁定)。**抢不到就跳过**。这是分布式安全的核心,见下面专门一节。

**第 3 步 — 加载数据** (`:50-51`)

拿到这条日志 `XxlJobLog`,再用 `log.getJobId()` 反查任务定义 `XxlJobInfo`。注意:任务可能已经被删了,所以 `info` 可能为 `null`,后面要判空。

**第 4 步 — 失败自动重试** (`:53-59`)

```java
if (log.getExecutorFailRetryCount() > 0) {
    ...getJobTriggerPoolHelper().trigger(log.getJobId(), TriggerTypeEnum.RETRY,
            (log.getExecutorFailRetryCount()-1), ...);
    // 给 triggerMsg 追加一行"重试"标记并落库
}
```

要点:

- 剩余重试次数读的是**这条日志上记录的** `executorFailRetryCount`,触发新一次执行时传入 `次数-1`。所以重试是**逐次递减、有上限的**,不会无限重试。
- 重试是另起一次完整调度(走 `JobTriggerPoolHelper`),不是在这里同步执行。
- 重试和下面的告警是**两件独立的事**,都会做。

**第 5 步 — 失败告警** (`:61-68`)

```java
int newAlarmStatus = 0;
if (info != null) {
    boolean alarmResult = ...getJobAlarmer().alarm(info, log);
    newAlarmStatus = alarmResult ? 2 : 3;   // 2=告警成功, 3=告警失败
} else {
    newAlarmStatus = 1;                      // 1=任务已删,无需告警
}
```

**第 6 步 — 回写最终状态** (`:70`)

```java
...updateAlarmStatus(failLogId, -1, newAlarmStatus);  // 从"锁定(-1)"改成最终状态
```

处理完整批后 **sleep 10 秒** (`:80-81`),再进入下一轮。

`alarm_status` 这一列的取值含义(代码 `:62` 的注释):

| 值 | 含义 |
|----|------|
| `0` | 默认 / 待处理(`findFailJobLogIds` 只捞这种) |
| `-1` | 锁定中(已被某个 admin 节点抢走,正在处理) |
| `1` | 无需告警(任务已删) |
| `2` | 告警成功 |
| `3` | 告警失败 |

## 难点一:到底什么样的日志算"失败"?

这是 `findFailJobLogIds` 的 SQL(`XxlJobLogMapper.xml:230-240`),新人第一次看几乎必懵,因为它用的是**双重否定**:

```sql
SELECT id FROM xxl_job_log
WHERE !(
    (trigger_code in (0, 200) and handle_code = 0)   -- A: 还没出最终结果
    OR
    (handle_code = 200)                              -- B: 执行成功
)
AND alarm_status = 0
ORDER BY id ASC
LIMIT #{pagesize}
```

先理解两个 code(都遵循"200=成功"的约定):

- `trigger_code`:**调度下发**结果。`0`=还没下发,`200`=下发成功,其它(如 500)=下发失败(比如执行器全挂了,根本没派出去)。
- `handle_code`:**执行器执行**结果。`0`=还没回结果(在跑 / 在途),`200`=执行成功,其它=执行失败。

把双重否定翻译成人话,`!(A OR B)` = `非A 且 非B`,也就是**既不是"还在跑",也不是"成功"** —— 剩下的就是失败:

| 场景 | trigger_code | handle_code | 会被捞出来告警吗 |
|------|-------------|-------------|------------------|
| 还没下发 / 下发成功,但执行器还没回结果(在途) | 0 或 200 | 0 | ❌ 命中 A,跳过 |
| 执行成功 | 200 | 200 | ❌ 命中 B,跳过 |
| **下发就失败了**(没派出去) | 非 0/200 | 0 | ✅ 失败,告警 |
| **执行失败** | 200 | 非 0/200 | ✅ 失败,告警 |

关键认知:**这个监控只对"已经盖棺定论的失败"出手,绝不打扰还在运行中的任务**(`handle_code=0` 且下发正常的那类被刻意排除)。配合 `alarm_status = 0`,保证每条失败日志只会被处理一次。

> 顺带一提:同一个 Mapper 里还有个 `findLostJobIds`(`:249`),那是处理另一类问题——执行器在途中掉线、回调永远回不来的"丢失"日志,由别的线程兜底,别和这里的"失败"混为一谈。

## 难点二:多个 admin 节点为什么不会重复告警?

生产上 admin 一般是**集群多实例**,每个实例都跑着这个监控线程,都会捞到同一批失败日志。防止"一条失败发好几封邮件 / 重试好几次"靠的就是第 2 步那个 CAS(比较并交换)。

看 `updateAlarmStatus` 的 SQL(`XxlJobLogMapper.xml:242-247`):

```sql
UPDATE xxl_job_log
SET alarm_status = #{newAlarmStatus}
WHERE id = #{logId} AND alarm_status = #{oldAlarmStatus}   -- ← 关键在这个 AND
```

它返回**受影响行数**。所以 `updateAlarmStatus(id, 0, -1)` 的含义是:**"只有当 alarm_status 还是 0 时,才把它改成 -1"**。数据库行级锁保证这个判断+更新是原子的:

- A 节点先执行 → 命中 1 行 → 抢锁成功 → 继续处理;
- B 节点随后执行同一条 → 此时 `alarm_status` 已是 `-1`,`WHERE` 不匹配 → 影响 0 行 → `lockRet < 1` → `continue` 跳过。

**这就是整个类不需要任何额外分布式锁组件、靠一条 SQL 就实现集群互斥的精髓。** `0 → -1`(抢占)和 `-1 → 最终值`(收尾)两次都走同一个 CAS 方法。

## 告警是怎么发出去的(扩展点在这里)

主角只负责"决定要告警",具体怎么告警委托给 `JobAlarmer`。

**`JobAlarmer.alarm()`** (`JobAlarmer.java:28-47`):

```java
@Autowired
private List<JobAlarm> jobAlarmList;   // Spring 自动注入所有 JobAlarm 实现

public boolean alarm(XxlJobInfo info, XxlJobLog jobLog) {
    boolean result = true;             // 注意:空集合时返回 false
    for (JobAlarm alarm : jobAlarmList) {
        boolean item = false;
        try { item = alarm.doAlarm(info, jobLog); }
        catch (Exception e) { logger.error(...); }   // 单个渠道异常不影响其它渠道
        if (!item) result = false;     // "全部成功"才算成功
    }
    return result;
}
```

设计上是**策略 + 自动收集**:Spring 把所有实现 `JobAlarm` 接口的 `@Component` 注入成一个 List,逐个调用。

**这对你接手后最重要的一点**:想新增告警渠道(钉钉、企业微信、Webhook……),**不用改这个监控类,也不用改 `JobAlarmer`**,只要新写一个类:

```java
@Component
public class DingTalkJobAlarm implements JobAlarm {
    public boolean doAlarm(XxlJobInfo info, XxlJobLog jobLog) { ... }
}
```

Spring 会自动把它纳入告警列表。目前仓库里唯一的实现是 `EmailJobAlarm`。

**`EmailJobAlarm.doAlarm()`** (`EmailJobAlarm.java:36-85`) 做的事很直白:

- 只有任务配了告警邮箱(`info.getAlarmEmail()` 非空)才发;
- 拼一段 HTML 内容,把失败日志的 `triggerMsg` / `handleMsg` 带上,套用一个表格邮件模板;
- 邮箱支持逗号分隔多个,逐个发送;
- 任意一封发送异常 → 返回 `false`(进而让上面第 5 步把状态写成 `3-告警失败`)。

## 生命周期:它什么时候启动 / 停止

它不是 Spring Bean(注意类上没有 `@Component`),而是由 `XxlJobAdminBootstrap` 在 admin 启动时**手动 new 出来并 start**(`XxlJobAdminBootstrap.java:64、92`),admin 关闭时调 `stop()`。

- `start()`:线程被设为 **daemon(守护线程)** 且命名为 `xxl-job, admin JobFailMonitorHelper`,不会阻止 JVM 退出。
- `stop()` (`:102-110`):置 `toStop = true` → `interrupt()` 打断 sleep → `join()` 等线程真正结束,做到优雅停机。`toStop` 用 `volatile` 保证跨线程可见性。
- 容错:循环体最外层 `catch (Throwable)`,任何单轮异常都只打日志、不会让线程死掉,保证监控长期存活。

## 接手后值得记住的几个"坑 / 注意点"

1. **告警延迟最长约 10 秒**:由 `sleep(10s)` 决定,失败发生到收到邮件不是实时的,排障时别误以为告警丢了。
2. **每条失败日志只告警一次**:`alarm_status` 一旦离开 `0` 就不会被重新捞起。如果你想"持续提醒直到处理",当前机制做不到。
3. **(重要)进程在"锁定中"崩溃会留下僵尸状态**:如果某节点在第 2 步把状态改成 `-1` 之后、第 6 步回写之前崩了,这条日志会**永远停在 `-1`**——既不会被 `findFailJobLogIds`(只认 `alarm_status=0`)重新捞到,也没有任何逻辑把 `-1` 复位成 `0`。结果就是这条失败**既没成功告警、也不会再重试/补告警**。这是 xxl-job 这套设计已知的边界情况,概率低但真实存在;排查"某次失败为什么没告警"时,可以去库里看看是不是有 `alarm_status = -1` 的残留。
4. **重试和告警相互独立**:重试成功**不会**取消这次告警——只要这条日志判定为失败,该告警照样发。这是符合预期的行为,不是 bug。
5. **类名 vs 线程名对不上**:类叫 `JobFailAlarmMonitorHelper`,但日志和线程名里写的是老名字 `JobFailMonitorHelper`(`:76、90、95`)。grep 排查时两个名字都要搜。

## 后续常见可扩展方向

- **新增告警渠道**(钉钉 / 企业微信 / Webhook 等):新建一个实现 `JobAlarm` 接口的 `@Component` 即可,监控类与 `JobAlarmer` 都无需改动。
- **告警延迟可配置 / 改小**:当前固定 `TimeUnit.SECONDS.sleep(10)`,可抽成配置项以适配对时效更敏感的场景。
