# `ExecutorRouteConsistentHash.java` 深入浅出讲解

---

## 一、它在哪里？它是什么角色？

```
xxl-job-admin/scheduler/route/
├── ExecutorRouter.java              ← 抽象父类，定义统一接口
├── ExecutorRouteStrategyEnum.java   ← 枚举，注册 10 种路由策略
└── strategy/
    ├── ExecutorRouteFirst.java      ← 第一个
    ├── ExecutorRouteRound.java      ← 轮询
    ├── ExecutorRouteRandom.java     ← 随机
    ├── ExecutorRouteConsistentHash.java  ← ★ 你正在看的这个
    └── ... (LFU / LRU / Failover 等)
```

它负责回答一个核心问题：

> **「一个 Job 要被调度执行了，执行器集群有好几台机器，我该选哪一台？」**

---

## 二、一致性哈希到底要解决什么问题？

假设你有 3 台执行器 `A1, A2, A3`，有 1000 个 Job。

**朴素的取模方式**：`jobId % 3` 选机器
- 问题 1：如果机器变成 4 台，几乎所有 Job 的 `jobId % 4` 结果都变了，**全部重新分配**——大量任务迁移，缓存失效。
- 问题 2：`String.hashCode()` 容易冲突，分布也不均匀。

**一致性哈希的目标**：
1. 同一个 Job **永远路由到同一台机器**（保持亲和性、利用本地缓存）。
2. 机器增减时，**只有少量 Job 需要迁移**。
3. 各台机器分到的 Job 数量尽量**均匀**。

---

## 三、核心代码逐行拆解

### 3.1 哈希函数：为什么用 MD5 而不用 `String.hashCode()`？

```java
private static long hash(String key) {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    byte[] digest = md5.digest(key.getBytes(StandardCharsets.UTF_8));

    // 取 digest 的前 4 个字节，拼成一个 32 位整数
    long hashCode = ((long)(digest[3] & 0xFF) << 24)
                  | ((long)(digest[2] & 0xFF) << 16)
                  | ((long)(digest[1] & 0xFF) << 8)
                  |  (long)(digest[0] & 0xFF);

    return hashCode & 0xffffffffL;   // 确保是无符号 32 位正数
}
```

| 设计点 | 解释 |
|--------|------|
| **用 MD5** | `String.hashCode()` 是 32 位的，不同字符串很容易产生相同值（碰撞）。MD5 产生 128 位摘要，取其前 32 位分布更均匀 |
| **`& 0xFF`** | Java 的 `byte` 是有符号的（-128~127），用 `& 0xFF` 转成无符号 0~255 |
| **`& 0xffffffffL`** | 把结果映射到 `[0, 2^32-1]` 的无符号范围，相当于一个首尾相接的「环」 |

> 🎯 **一句话**：把任意字符串变成一个在 `0 ~ 42亿` 之间均匀分布的数字，当作这个节点在「环」上的位置。

### 3.2 构建哈希环 + 虚拟节点

```java
TreeMap<Long, String> addressRing = new TreeMap<>();
for (String address : addressList) {
    for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {  // VIRTUAL_NODE_NUM = 100
        long addressHash = hash("SHARD-" + address + "-NODE-" + i);
        addressRing.put(addressHash, address);
    }
}
```

**为什么要虚拟节点？**

如果 3 台机器各只算 1 个哈希值，环上只有 3 个点，它们之间的间隔很可能不均匀：

```
---A1----------------------------A2-----------A3---
```

这导致 A1 要负责环上巨大的一段弧，大量 Job 都会落到 A1，**负载不均**。

引入虚拟节点后，每台机器在环上有 **100 个点**，3 台机器就是 300 个点，它们在环上交叉分布，**每个实节点实际管辖的弧长趋于相等**：

```
--A1-v0--A2-v3--A3-v1--A1-v7--A2-v0--A3-v5--A1-v2--... (混合交叉)
```

> 🎯 **一句话**：虚拟节点 = 每台机器的「分身」，分身越多，负载越均匀。

**为什么用 `TreeMap`？**

`TreeMap` 内部是红黑树，key 有序，查找某个 hash 值「右侧第一个节点」的时间复杂度是 O(log n)——正好适合做哈希环。

### 3.3 路由 Job：顺时针找最近节点

```java
long jobHash = hash(String.valueOf(jobId));

// ceilingEntry: 返回 ≥ jobHash 的最小 entry（即顺时针方向最近节点）
Map.Entry<Long, String> ceilingEntry = addressRing.ceilingEntry(jobHash);
if (ceilingEntry != null) {
    return ceilingEntry.getValue();
}

// 如果 jobHash 比环上所有节点都大，回到环的起点（firstEntry）
return addressRing.firstEntry().getValue();
```

图解：

```
           jobHash
              ↓
---A1---------●---------A2-----------A3---(wrap)---
                        ↑
              ceilingEntry → 选 A2
```

如果 Job 的哈希落在 A3 之后（即比所有节点都大），则**环绕**回到 A1——这就是「环」的含义。

### 3.4 对外接口：`route()`

```java
@Override
public Response<String> route(TriggerRequest triggerParam, List<String> addressList) {
    String address = hashJob(triggerParam.getJobId(), addressList);
    return Response.ofSuccess(address);
}
```

父类 `ExecutorRouter` 定义了统一的 `route()` 接口，`ExecutorRouteStrategyEnum` 枚举通过名字匹配到 `CONSISTENT_HASH` 时就会调用这个实现。

---

## 四、整体流程串起来

```
┌──────────────────────────────────────────────────────────────┐
│                  调度器触发一个 Job                            │
│                          │                                   │
│    ┌─────────────────────▼───────────────────────┐           │
│    │ ExecutorRouteStrategyEnum.match("CONSISTENT_HASH") │    │
│    └─────────────────────┬───────────────────────┘           │
│                          │                                   │
│    ┌─────────────────────▼───────────────────────┐           │
│    │     ExecutorRouteConsistentHash.route()     │           │
│    │                                             │           │
│    │  1. 遍历 addressList                        │           │
│    │     每个 address × 100 虚拟节点              │           │
│    │     用 MD5 算 hash → 填入 TreeMap 环         │           │
│    │                                             │           │
│    │  2. 用 MD5 算 jobId 的 hash                  │           │
│    │                                             │           │
│    │  3. ceilingEntry 顺时针找最近节点             │           │
│    │     (找不到则 wrap 到 firstEntry)            │           │
│    │                                             │           │
│    │  4. 返回该 address                           │           │
│    └─────────────────────────────────────────────┘           │
└──────────────────────────────────────────────────────────────┘
```

---

## 五、你必须知道的几个关键特性

| 特性 | 说明 |
|------|------|
| **确定性** | 同一个 `jobId` + 同一个 `addressList`，无论调用多少次，结果完全相同 |
| **亲和性** | 同一个 Job 始终路由到同一台执行器，适合有状态/本地缓存的场景 |
| **均衡性** | 虚拟节点保证了各执行器分到的 Job 数量大致相等 |
| **低迁移成本** | 新增/下线一台机器时，只有「该机器管辖的那段弧」上的 Job 需要重新路由，其他 Job 不受影响 |
| **无状态** | 每次调用都重新构建哈希环（没有缓存），代码简单但有一定性能开销；在路由策略被频繁调用的热路径上如果成为瓶颈，可考虑缓存环 |

---

## 六、与其他路由策略的对比（何时选它？）

| 场景 | 推荐策略 |
|------|----------|
| Job 无状态，随便分 | `RANDOM` / `ROUND` |
| Job 有本地缓存/需要粘性分配 | **`CONSISTENT_HASH`** ✓ |
| 希望忙闲均衡 | `LFU` / `LRU` / `BUSYOVER` |
| 高可用，优先找活着的节点 | `FAILOVER` |
| 所有机器都要执行一遍 | `SHARDING_BROADCAST` |

---

## 七、容易踩的坑 & 注意事项

1. **地址列表为空时** `addressRing` 为空，`firstEntry()` 会返回 `null` 导致 NPE——调用方应保证 `addressList` 非空。
2. **每次 route 都重建环**：100 个虚拟节点 × N 台机器次 MD5 运算。在 Job 触发频率极高时是一个优化点（可缓存 ring）。
3. **虚拟节点数 `VIRTUAL_NODE_NUM = 100`** 是写死的。节点数极少时（如 2 台）分布可能仍不够均匀，但 100 在绝大多数场景够用。
4. **代码中注释掉的 `tailMap` 逻辑**（第 84-87 行）是旧实现，已被更简洁的 `ceilingEntry` 替代——两者语义相同，`ceilingEntry` 更直接。
