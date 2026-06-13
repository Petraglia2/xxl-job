# xxl-job admin 端一致性哈希路由 `ExecutorRouteConsistentHash.java` 讲解

> 面向刚接手该项目的后端工程师。从「它在系统里扮演什么角色」讲到「每一行在做什么」,再到「维护时要注意什么」。
>
> 文件位置:`xxl-job-admin/src/main/java/com/xxl/job/admin/scheduler/route/strategy/ExecutorRouteConsistentHash.java`

---

## 1. 它在系统里的位置

xxl-job 的一次任务触发,大致是:**调度中心(admin)决定「这个任务该发给哪台执行器机器」→ 把任务请求发过去**。

「该发给哪台」这一步就叫**路由(route)**。同一个执行器分组下往往挂了多台机器,xxl-job 提供了一整套路由策略(同目录可见):

- `ExecutorRouteFirst` / `Last` —— 永远第一台 / 最后一台
- `Random` / `Round` —— 随机 / 轮询
- `LRU` / `LFU` —— 最近最少 / 最不经常使用
- `Failover` / `Busyover` —— 故障转移 / 忙碌转移
- **`ExecutorRouteConsistentHash`** —— 一致性哈希(本文主角)

它们都继承自抽象基类 `ExecutorRouter`,约定了唯一一个方法:

```java
public abstract Response<String> route(TriggerRequest triggerParam, List<String> addressList);
```

输入是「任务参数」和「当前在线机器地址列表」,输出是「选中的那一台地址」。这是典型的**策略模式**:每种策略是一个类,调度时按任务配置挑一个用。

---

## 2. 这个策略想解决什么问题

一致性哈希在这里的核心目标,类文件开头注释说得很准:

> 分组下机器地址相同,不同 JOB 均匀散列到不同机器;**且每个 JOB 固定调度其中一台机器**。

拆成两点理解:

1. **粘性(stickiness)**:同一个 `jobId`,只要机器列表不变,**永远落到同一台机器**。这对「希望同一任务总在同一台机器跑」的场景很有用(比如该机器上有本地缓存、本地文件、预热好的连接等)。
2. **平滑扩缩容**:当机器增加/减少一台时,**只有约 1/N 的任务**会被重新分配到别的机器,其余任务的归属不变。

第 2 点正是「一致性哈希」相对于「取模路由(`jobId % 机器数`)」的最大优势。取模路由一旦机器数从 3 变 4,几乎**所有**任务的归属都会变;而一致性哈希只动一小部分。

---

## 3. 代码逐段拆解

整个类只有三个方法,从下往上看更容易理解:`route` → `hashJob` → `hash`。

### 3.1 入口 `route`

```java
public Response<String> route(TriggerRequest triggerParam, List<String> addressList) {
    String address = hashJob(triggerParam.getJobId(), addressList);
    return Response.ofSuccess(address);
}
```

非常薄,只做一件事:拿 `jobId` 和地址列表,交给 `hashJob` 算出地址,包装成成功响应返回。真正的算法在 `hashJob`。

### 3.2 核心 `hashJob` —— 一致性哈希的「环」

这是精华。它分四步:

**第一步:构建哈希环**

```java
TreeMap<Long, String> addressRing = new TreeMap<>();
for (String address : addressList) {
    for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {          // VIRTUAL_NODE_NUM = 100
        long addressHash = hash("SHARD-" + address + "-NODE-" + i);
        addressRing.put(addressHash, address);
    }
}
```

把每台机器映射到 `0 ~ 2³²-1` 这个环上。注意**每台机器不是放 1 个点,而是放 100 个「虚拟节点」**(`SHARD-地址-NODE-0` 到 `NODE-99`),它们都指回同一台真实机器。

数据结构用 `TreeMap<Long, String>`,key 是环上的位置(哈希值),value 是机器地址。`TreeMap` 底层是红黑树,**天然按 key 有序**,这正是「环」要的特性——能快速找到「某个位置顺时针方向的下一个节点」。

> 为什么要虚拟节点?见 §4.1。

**第二步:计算任务在环上的位置**

```java
long jobHash = hash(String.valueOf(jobId));
```

把 `jobId` 也哈希到同一个环上,得到一个点 `J`。

**第三步:顺时针找最近的节点**

```java
Map.Entry<Long, String> ceilingEntry = addressRing.ceilingEntry(jobHash);
if (ceilingEntry != null) {
    return ceilingEntry.getValue();
}
```

`ceilingEntry(jobHash)` 返回**「key ≥ jobHash 的第一个条目」**,几何上就是「从任务点 `J` 出发,顺时针走,遇到的第一个机器节点」。文件里那张 ASCII 图画的就是这件事:

```
// ------A1------A2-------A3------
// -----------J1------------------    J1 顺时针撞到 A2,就路由到 A2
```

**第四步:环的「回绕」**

```java
return addressRing.firstEntry().getValue();
```

如果 `ceilingEntry` 返回 `null`,说明 `jobHash` 比环上所有节点都大(它落在了最大节点和「2³² 回到 0」之间的那段弧上)。环是首尾相接的,所以顺时针再往前就**绕回到环的起点**,即最小的那个节点 `firstEntry()`。这是一致性哈希必须处理的边界,少了它这段弧上的任务就无处可去。

(源码里那段被注释掉的 `tailMap` 写法是早期实现,效果和 `ceilingEntry` 等价,现在留作参考。)

### 3.3 底层 `hash` —— 用 MD5 算哈希值

```java
md5.update(key.getBytes(StandardCharsets.UTF_8));
byte[] digest = md5.digest();                 // 16 字节

long hashCode = ((long)(digest[3] & 0xFF) << 24)
              | ((long)(digest[2] & 0xFF) << 16)
              | ((long)(digest[1] & 0xFF) << 8)
              |       (digest[0] & 0xFF);
return hashCode & 0xffffffffL;                 // 截成无符号 32 位
```

它取 MD5 结果的**前 4 个字节**,拼成一个 32 位整数,再用 `& 0xffffffffL` 当作无符号数,落到 `0 ~ 2³²-1` 的环上。

几个细节:

- `digest & 0xFF`:Java 的 `byte` 是有符号的,直接用会变负数,`& 0xFF` 把它当无符号字节处理,这是 Java 位运算的常规写法。
- 只用了 16 字节里的 4 字节:够了。这里要的不是密码学安全,只是要一个**分布均匀**的 32 位数。
- 为什么用 MD5 而不是 `String.hashCode()`?见 §4.2。

---

## 4. 两个关键设计决策(注释里点了名)

### 4.1 虚拟节点:解决「不均衡」

如果每台机器只在环上放 1 个点,当机器很少时(比如 3 台),这 3 个点可能恰好挤在环的一小段,导致环上大片区域都指向同一台机器 —— 任务分布严重倾斜。

放 100 个虚拟节点后,每台机器被「打散」到环的各处,任务分布就接近均匀了。**代价**是环上有 `100 × 机器数` 个节点,构建和查找开销更大(但 100 是经验上的平衡值)。

### 4.2 用 MD5 替代 hashCode:解决「哈希冲突 / 分布差」

`String.hashCode()` 返回 32 位 int,但对短字符串(比如 `"SHARD-ip:port-NODE-1"`)分布很差、容易聚集甚至碰撞。虚拟节点如果都堆在相近的哈希值上,§4.1 的均衡努力就白费了。MD5 有良好的「雪崩效应」—— 输入差一点,输出天差地别,分布非常均匀,正好适合撒虚拟节点。

---

## 5. 接手维护时要特别注意的几点

1. **环是「每次调用都重建」的**。每次 `route` 都会做 `100 × 机器数` 次 MD5 + 重建一棵红黑树。任务触发频率高、机器多时,这是个不小的 CPU 开销。社区一直保持这种「无状态、每次重算」的写法是为了**始终反映最新的在线机器列表**、实现简单且无需缓存失效逻辑。如果未来要优化,可以考虑按 `addressList` 做缓存,但要谨慎处理机器上下线时的失效。

2. **无状态 = 天然支持 admin 集群**。这个策略的结果只取决于入参(`jobId` + 地址列表),不依赖任何实例内存状态。所以多台 admin 同时算,结果一致,HA 部署无压力。这点和 `LRU`/`LFU`/`Failover` 不同(那些是有状态的)。

3. **「粘性」依赖地址字符串本身**。环上的节点 key 由 `"SHARD-" + address + "-NODE-" + i` 算出。如果某台执行器换了 `host:port` 重新注册,在算法眼里它就是一台**全新的机器**,原本固定调度到它的任务会被重新分配。容器化、IP 漂移的环境尤其要留意这点。

4. **扩缩容会迁移约 1/N 的任务**。这是一致性哈希的固有特性,通常是优点。但如果你的任务**强依赖**「永远在同一台机器跑」(本地状态不可迁移),那么扩缩容时这部分任务换机器就可能引发问题——这是业务层面要评估的。

5. **顺序无关、内容相关**。`addressList` 的元素顺序不影响结果(TreeMap 按哈希值排序),但元素内容(地址字符串集合)一旦变化,映射就会变。这是符合预期的行为。

---

## 一句话总结

`ExecutorRouteConsistentHash` 用「MD5 哈希 + TreeMap 环 + 每台机器 100 个虚拟节点」实现一致性哈希,让**同一个 jobId 稳定落到同一台执行器**,并在机器增减时把影响范围控制在约 1/N。它是一个**无状态、每次调用重建环**的纯函数式路由策略——理解了「环 + 顺时针找最近节点 + 回绕」这三件事,整个文件就通了。

> 继续往下挖的建议:接着看触发流程里**谁调用了 `route`、addressList 从哪来(执行器注册表)**,把「路由」嵌进完整的调度链路里。
