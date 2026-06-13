# CronExpression.java 深入浅出讲解

## 一、这个文件是做什么的？

一句话概括：**它把一个 Cron 表达式字符串解析成内存中的"集合"，然后基于这些集合计算"下一次触发时间"。**

在 xxl-job 中，当你在调度中心创建一个定时任务并选择 **CRON 调度模式** 时，最终就是靠这个类来算出"下一次该几点执行"。调用链非常短：

```
CronScheduleType.generateNextTriggerTime()
    → new CronExpression("0 0 12 * * ?")
    → .getNextValidTimeAfter(fromTime)
    → 返回一个 Date
```

此外，在 `XxlJobServiceImpl` 中新增/更新任务时，也会调用 `CronExpression.isValidExpression()` 做合法性校验。

> **来源**：文件头部注释写着 `Borrowed from quartz v2.5.2`，即从 Quartz 调度框架移植而来，Apache 2.0 协议。

---

## 二、Cron 表达式的 7 个字段

Cron 表达式是空格分隔的 6~7 个字段：

```
┌───────────── 秒 (0-59)
│ ┌───────────── 分 (0-59)
│ │ ┌───────────── 时 (0-23)
│ │ │ ┌───────────── 日 (1-31)
│ │ │ │ ┌───────────── 月 (1-12 / JAN-DEC)
│ │ │ │ │ ┌───────────── 周 (1-7 / SUN-SAT, 1=SUN)
│ │ │ │ │ │ ┌───────────── 年 (可选, 1970-2199)
│ │ │ │ │ │ │
* * * * * * *
```

支持的**特殊字符**：

| 字符 | 含义 | 示例 |
|------|------|------|
| `*` | 匹配所有（每个值） | `*` 在秒字段 = 每秒 |
| `?` | 不指定值（仅用于"日"和"周"） | 日=`?`, 周=`MON` = 不管几号，只要周一 |
| `-` | 范围 | `10-12` 在时字段 = 10点、11点、12点 |
| `,` | 列举 | `MON,WED,FRI` = 周一、周三、周五 |
| `/` | 步进 | `0/15` 在秒字段 = 0,15,30,45 |
| `L` | "Last"（仅用于"日"和"周"） | 日=`L` = 月末最后一天；周=`6L` = 本月最后一个周五 |
| `W` | 最近工作日（仅用于"日"） | `15W` = 离15号最近的工作日 |
| `#` | 第N个（仅用于"周"） | `6#3` = 本月第3个周五 |

---

## 三、类的整体结构

把这个类想象成**两个阶段**：

```
┌────────────────────────────────────────────────────────┐
│              阶段一：解析 (Parse)                       │
│                                                        │
│  "0 0 12 * * ?"                                        │
│       ↓  buildExpression()                             │
│       ↓  storeExpressionVals()                         │
│       ↓  addToSet()                                    │
│                                                        │
│  结果：seconds={0}, minutes={0}, hours={12},           │
│        daysOfMonth={ALL_SPEC}, months={ALL_SPEC},      │
│        daysOfWeek={NO_SPEC}                            │
├────────────────────────────────────────────────────────┤
│              阶段二：计算 (Compute)                      │
│                                                        │
│  getTimeAfter(date)                                    │
│       ↓  逐字段推进 Calendar                            │
│       ↓  秒→分→时→日→月→年 逐级匹配                     │
│                                                        │
│  结果：返回下一个满足条件的 Date                          │
└────────────────────────────────────────────────────────┘
```

---

## 四、阶段一：解析流程详解

### 4.1 入口——构造函数

```java
public CronExpression(String cronExpression) throws ParseException {
    this.cronExpression = cronExpression.toUpperCase(Locale.US); // 统一大写
    buildExpression(this.cronExpression);
}
```

构造时立刻解析，如果表达式不合法，直接抛 `ParseException`。这意味着 **一个 CronExpression 对象一旦创建，其内部状态就是不可变的**。

### 4.2 buildExpression() —— 按字段拆分

```java
StringTokenizer exprsTok = new StringTokenizer(expression, " \t", false);
// 校验：最多 7 个 token
while (exprsTok.hasMoreTokens() && exprOn <= YEAR) {
    String expr = exprsTok.nextToken().trim();
    // 按逗号拆分每个子表达式
    StringTokenizer vTok = new StringTokenizer(expr, ",");
    while (vTok.hasMoreTokens()) {
        storeExpressionVals(0, vTok.nextToken(), exprOn);
    }
    exprOn++; // 移到下一个字段
}
```

**关键设计**：用 `exprOn`（一个 int 计数器，从 `SECOND=0` 到 `YEAR=6`）追踪当前正在解析第几个字段。

解析完成后，有一个重要的**互斥校验**：

```java
// "日"和"周"不能同时指定具体值，必须有一个是 '?'
if (!dayOfMSpec || dayOfWSpec) {
    if (!dayOfWSpec || dayOfMSpec) {
        throw new ParseException("Support for specifying both ...");
    }
}
```

> **实际含义**：你不能写 `0 0 12 15 * MON`（既要15号又要周一），必须写成 `0 0 12 15 * ?` 或 `0 0 12 ? * MON`。

### 4.3 storeExpressionVals() —— 字符级状态机

这是整个解析的核心，本质是一个**字符级状态机**。它根据首字符决定走哪条分支：

```
storeExpressionVals(pos, s, type)
    │
    ├─ 首字符是字母 (A-Z) → 解析月份/星期的英文名称 (JAN, MON...)
    │                         处理 '-' 范围、'#' 第N个、'L' 最后一个
    │
    ├─ '?' → 标记 NO_SPEC（仅日和周可用）
    │
    ├─ '*' 或 '/' → 处理通配 + 步进
    │               '*' → ALL_SPEC
    │               '*/15' 或 '0/15' → 步进增量
    │
    ├─ 'L' → 处理"最后"语义
    │         日字段：L, L-3, LW（最后工作日）
    │         周字段：直接加 7（SAT）
    │
    └─ '0'-'9' → 解析数字
                  然后进入 checkNext() 处理后续可能的 '-', '/', 'L', 'W', '#'
```

### 4.4 checkNext() —— 处理复合表达式

当一个数字后面跟着特殊字符时，`checkNext` 负责处理：

| 模式 | 示例 | 行为 |
|------|------|------|
| `数字L` | `6L` | 标记 `lastDayOfWeek=true`，周集合加入6 |
| `数字W` | `15W` | 加入 `nearestWeekdays` 集合 |
| `数字#数字` | `6#3` | 记录 `nthDayOfWeek=3`，周集合加入6 |
| `数字-数字` | `10-12` | 调用 `addToSet(10, 12, 1, type)` |
| `数字/数字` | `0/15` | 调用 `addToSet(0, -1, 15, type)` |
| `数字-数字/数字` | `10-20/3` | 范围 + 步进：10, 13, 16, 19 |

### 4.5 addToSet() —— 填充 TreeSet

这是解析的终点，负责把值真正存入对应字段的 `TreeSet<Integer>` 中。

**核心逻辑**：

```java
// 情况1：单个值，无增量 → 直接加入
if ((incr == 0 || incr == -1) && val != ALL_SPEC_INT) {
    set.add(val);
    return;
}

// 情况2：有增量或通配 → 循环填充
for (int i = startAt; i <= stopAt; i += incr) {
    set.add(i % max); // 取模处理溢出
}
```

**溢出处理（Overflow）**：当范围的结束值小于起始值时（如 `NOV-FEB`、`22-2`），自动"绕回"：

```java
if (stopAt < startAt) {
    // 例如 NOV(10)-FEB(1): stopAt += 12 → stopAt=13
    // 然后循环: 10, 11, 12, 13%12=1 → 得到 {10, 11, 12, 1}
    stopAt += max;
}
```

### 4.6 "L" 的特殊编码

`L` 系列语法（L、L-1、L-3...）使用了一套巧妙的**编码偏移**：

```java
LAST_DAY_OFFSET_START = 32    // "L-30" 编码为 32
LAST_DAY_OFFSET_END   = 62    // "L"   编码为 62（= 32 + 30）
```

所以 `daysOfMonth` 中如果看到值 ≥ 32，就知道这是一个"距月末倒数第N天"的标记。在计算阶段，`findSmallestDay()` 负责把这个编码还原为实际日期。

---

## 五、阶段二：时间计算详解

### 5.1 核心方法 getTimeAfter()

这是整个文件最重要、也最复杂的方法（约 350 行）。它的任务是：**给定一个时间点，找到下一个满足 Cron 表达式的时刻。**

**算法思路**：使用一个 `while (!gotOne)` 循环，**从最小粒度到最大粒度逐级推进 Calendar**：

```
                    ┌─ 秒：在 seconds 中找 ≥ 当前秒的最小值
                    │  找不到 → 取最小值，分钟 +1
                    │
                    ├─ 分：在 minutes 中找 ≥ 当前分的最小值
                    │  找不到 → 取最小值，小时 +1
                    │  若分钟变化 → 重置秒=0，回到循环顶部
                    │
                    ├─ 时：在 hours 中找 ≥ 当前时的最小值
                    │  找不到 → 取最小值，日 +1
                    │  若小时变化 → 重置秒=0, 分=0，回到顶部
                    │
     while 循环 ──→│
                    ├─ 日：根据"日字段"或"周字段"规则匹配
                    │  ├── dayOfMSpec: findSmallestDay()
                    │  ├── dayOfWSpec + lastDayOfWeek: 找本月最后一个指定星期
                    │  ├── dayOfWSpec + nthDayOfWeek: 找第N个指定星期
                    │  └── dayOfWSpec 普通: 找最近的匹配星期
                    │  若日变化 → 重置时分秒，回到顶部
                    │
                    ├─ 月：在 months 中找 ≥ 当前月的最小值
                    │  找不到 → 取最小值，年 +1
                    │  若月变化 → 重置日时分秒，回到顶部
                    │
                    └─ 年：在 years 中找 ≥ 当前年的最小值
                       找不到 → 返回 null
                       若年变化 → 重置月日时分秒，回到顶部

                    所有字段都匹配 → gotOne = true → 返回 Date
```

**关键理解**：每次低位字段"溢出"（找不到合适值需要进位），都会 `continue` 跳回循环顶部，从秒开始重新匹配。这保证了**所有字段的组合一定是一致的**。

### 5.2 "日"字段的三种计算模式

这是 `getTimeAfter` 中最复杂的部分：

**模式 A：按"日"字段匹配（`dayOfMSpec`）**
```java
Optional<Integer> smallestDay = findSmallestDay(day, mon, year, daysOfMonth);
Optional<Integer> smallestDayForWeekday = findSmallestDay(day, mon, year, nearestWeekdays);
```
- `daysOfMonth` 存普通日期值和 `L` 编码值
- `nearestWeekdays` 存带 `W` 标记的日期值
- 两者取较小值
- `W` 还需要额外调整：周六→退到周五，周日→进到下周一

**模式 B：按"周"字段匹配（`dayOfWSpec`），三种子情况**

| 条件 | 含义 | 算法 |
|------|------|------|
| `lastDayOfWeek` | `6L` = 本月最后一个周五 | 从当前日推进到目标星期，然后每次+7直到月末 |
| `nthDayOfWeek != 0` | `6#3` = 本月第3个周五 | 先对齐到目标星期，再按周数偏移 |
| 普通 | `MON,WED` | 找最近的匹配星期几 |

### 5.3 安全阀

```java
if (cl.get(Calendar.YEAR) > 2999) { return null; }  // 防止死循环
if (year > MAX_YEAR) { return null; }                 // 超出支持范围
```

### 5.4 夏令时处理

```java
protected void setCalendarHour(Calendar cal, int hour) {
    cal.set(Calendar.HOUR_OF_DAY, hour);
    if (cal.get(Calendar.HOUR_OF_DAY) != hour && hour != 24) {
        cal.set(Calendar.HOUR_OF_DAY, hour + 1); // DST 跳过了一小时
    }
}
```

---

## 六、其他公开方法速查

| 方法 | 用途 | 项目中的调用位置 |
|------|------|-----------------|
| `isValidExpression(String)` | 校验表达式是否合法 | `XxlJobServiceImpl` 新增/更新任务时校验 |
| `getNextValidTimeAfter(Date)` | 计算下一次触发时间 | `CronScheduleType.generateNextTriggerTime()` |
| `isSatisfiedBy(Date)` | 判断某个时刻是否满足表达式 | 目前项目内未直接使用 |
| `getTimeBefore(Date)` | 二分查找上一个触发时间 | 目前项目内未直接使用 |
| `getNextInvalidTimeAfter(Date)` | 找下一个不满足的时刻 | 目前项目内未直接使用 |

---

## 七、数据流全景图

```
用户在 xxl-job 管理后台填写: "0 0/30 9-17 ? * MON-FRI"
                                │
                     ┌──────────┘
                     ▼
        XxlJobServiceImpl.update()
          → CronExpression.isValidExpression("0 0/30 9-17 ? * MON-FRI")
          → 通过校验，存入数据库
                     │
                     ▼  (调度线程触发时)
        CronScheduleType.generateNextTriggerTime()
          → new CronExpression("0 0/30 9-17 ? * MON-FRI")
              解析结果:
                seconds    = {0}
                minutes    = {0, 30}
                hours      = {9, 10, 11, 12, 13, 14, 15, 16, 17}
                daysOfMonth= {NO_SPEC}
                months     = {ALL_SPEC}
                daysOfWeek = {2, 3, 4, 5, 6}   (MON=2 ... FRI=6)
                years      = {ALL_SPEC}
          → .getNextValidTimeAfter(now)
              秒→分→时→日(按周)→月→年 逐级匹配
          → 返回 Date("2026-06-15 09:00:00")  // 假设下周一
```

---

## 八、你作为新接手工程师需要关注的几个"坑"

1. **"日"和"周"互斥**：不能同时指定，必须有一个是 `?`。这是最容易犯的错误，代码在 `buildExpression` 末尾有显式校验。

2. **步进不是"每N个"而是"从起始值开始每隔N个"**：`7/6` 在月份字段不是"每6个月"，而是"从7月开始每隔6个月" → 只有 `{7}` （因为 7+6=13 > 12）。

3. **`L` 编码偏移**：`daysOfMonth` 中 ≥ 32 的值不是真正的日期，而是"L系列"编码，阅读代码时不要误解。

4. **`getTimeAfter` 的 `continue` 机制**：每次低位进位都从头循环，看似效率不高，但实际上最多循环几次就能收敛（每个字段最多回退一次），这是刻意的设计取舍。

5. **年份上限**：`MAX_YEAR = 当前年 + 100`，超出返回 `null`。`getTimeAfter` 还有一个硬编码的 2999 年上限。

6. **`W` 不跨月**：`1W` 如果1号是周六，不会跳到上个月的周五，而是顺延到下周一（3号）。

---

如果你后续需要修改或调试这个文件，建议从单元测试 `CronExpressionTest` 入手，它展示了最基本的用法：创建表达式对象 → 循环调用 `getNextValidTimeAfter` 验证结果。
