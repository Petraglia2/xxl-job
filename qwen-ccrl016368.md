# xxl-job Admin 端 —— `JobRegistryHelper.java` 深入浅出

> 面向刚接手项目的后端工程师，从"为什么需要它"讲到"每一行在做什么"。

---

## 一、先理解业务背景：执行器注册是什么？

xxl-job 是一个分布式任务调度平台，架构上有两个角色：

| 角色 | 说明 |
|------|------|
| **Admin（调度中心）** | 负责管理和触发任务 |
| **Executor（执行器）** | 真正执行业务逻辑的 Worker 节点 |

调度中心要把任务发给某个执行器执行，**首先得知道它在哪**（IP + 端口）。但执行器可能随时上下线、扩缩容，所以需要一个"注册发现"机制——这就是**执行器注册**要解决的问题。

### 注册表 `xxl_job_registry` 的数据模型

```
┌──────────────────────────────────────────────────────────┐
│  xxl_job_registry                                        │
├──────────────┬───────────────────────────────────────────┤
│  id          │  主键                                     │
│  registry_group │  注册类型："EXECUTOR" 或 "ADMIN"        │
│  registry_key   │  执行器的 appname（如 "order-executor"）│
│  registry_value │  执行器的地址（如 "http://10.0.0.1:9999"）│
│  update_time    │  最后一次心跳时间                        │
└──────────────┴───────────────────────────────────────────┘
```

执行器每隔 **30 秒**向 Admin 发送一次心跳（beat），Admin 就把 `update_time` 刷新为当前时间。如果超过 **90 秒**没有收到心跳，Admin 就认为这个执行器已经"死了"。

这两个关键常量定义在 `Const.java` 里：

```java
public static final int BEAT_TIMEOUT = 30;           // 心跳间隔 30s
public static final int DEAD_TIMEOUT = BEAT_TIMEOUT * 3;  // 死亡阈值 90s
```

---

## 二、`JobRegistryHelper` 的职责总览

这个类只有 **222 行**，但承担了三大职责：

```
┌─────────────────────────────────────────────────────┐
│              JobRegistryHelper                       │
│                                                     │
│  ① registry()      ── 接收执行器的心跳注册            │
│  ② registryRemove() ── 接收执行器的下线注销            │
│  ③ registryMonitorThread ── 后台线程：                 │
│      · 定期清理"已死"的注册记录                        │
│      · 根据存活记录刷新每个执行器组的地址列表             │
└─────────────────────────────────────────────────────┘
```

---

## 三、逐段剖析源码

### 3.1 类的成员变量

```java
private ThreadPoolExecutor registryOrRemoveThreadPool = null;
private Thread registryMonitorThread;
private volatile boolean toStop = false;
```

- `registryOrRemoveThreadPool`：用于**异步处理**注册/注销请求的线程池
- `registryMonitorThread`：后台**监控线程**，周期性巡检
- `toStop`：优雅停止的标志位，用 `volatile` 保证线程可见性

### 3.2 `start()` —— 启动资源（第 34–131 行）

Admin 启动时调用，做两件事：

#### （A）初始化线程池

```java
registryOrRemoveThreadPool = new ThreadPoolExecutor(
    2,                    // 核心线程 2 个
    10,                   // 最大线程 10 个
    30L, TimeUnit.SECONDS, // 空闲回收 30s
    new LinkedBlockingQueue<Runnable>(2000),  // 队列容量 2000
    ...,                  // 自定义线程命名
    new RejectedExecutionHandler() {
        public void rejectedExecution(Runnable r, ...) {
            r.run();      // 拒绝策略：调用者线程直接执行（CallerRunsPolicy 的手写版）
        }
    });
```

**要点理解**：

- 执行器心跳是**高频操作**（假设有 100 个执行器实例，每 30s 就有一次心跳），所以用线程池异步处理，避免阻塞 HTTP 请求线程。
- 拒绝策略选择了**调用者运行**而非丢弃——保证心跳不丢失，最差情况也只是当前 HTTP 线程同步执行。

#### （B）启动监控线程

这是 `start()` 中最核心的部分。这个 `registryMonitorThread` 做的工作可以用一张流程图来理解：

```
┌────────────────────────────────────────────────────────────────┐
│  registryMonitorThread 循环体（每 30s 一轮）                      │
│                                                                │
│  Step 1: 查出所有 addressType=0（自动注册）的执行器组             │
│          ┌──────────────────────────────────┐                  │
│          │ findByAddressType(0) → groupList │                  │
│          └──────────────────────────────────┘                  │
│                        │                                       │
│                        ▼                                       │
│  Step 2: 清理死亡注册                                            │
│          ┌──────────────────────────────────────────────┐      │
│          │ findDead(DEAD_TIMEOUT=90s, now)              │      │
│          │   → SQL: WHERE update_time < now - 90秒       │      │
│          │   → 返回已死的 id 列表                          │      │
│          │ removeDead(ids) → DELETE FROM xxl_job_registry │      │
│          └──────────────────────────────────────────────┘      │
│                        │                                       │
│                        ▼                                       │
│  Step 3: 查出所有存活的注册记录，按 appname 聚合                   │
│          ┌──────────────────────────────────────────────┐      │
│          │ findAll(DEAD_TIMEOUT=90s, now)               │      │
│          │   → SQL: WHERE update_time > now - 90秒       │      │
│          │                                              │      │
│          │ 遍历结果，只取 registryGroup = "EXECUTOR" 的    │      │
│          │ 按 appname 分组，构建:                         │      │
│          │   Map<appname, List<address>>                 │      │
│          └──────────────────────────────────────────────┘      │
│                        │                                       │
│                        ▼                                       │
│  Step 4: 刷新每个执行器组的 addressList                          │
│          ┌──────────────────────────────────────────────┐      │
│          │ for each group in groupList:                 │      │
│          │   addresses = appAddressMap.get(group.appname)│      │
│          │   group.addressList = addresses.join(",")    │      │
│          │   UPDATE xxl_job_group SET address_list=...   │      │
│          └──────────────────────────────────────────────┘      │
│                        │                                       │
│                        ▼                                       │
│          sleep(30s) 然后进入下一轮循环                             │
└────────────────────────────────────────────────────────────────┘
```

**几个关键细节**：

- **`addressType=0`** 表示"自动注册"的执行器组。`addressType=1` 是"手动录入"的——手动录入的不需要自动维护地址，所以跳过。
- 地址列表存成逗号分隔的字符串，排序后再拼接，保证输出稳定一致。
- 这个线程设为**守护线程**（`setDaemon(true)`），意味着当所有用户线程结束时，JVM 不会因它而阻止退出。

### 3.3 `registry()` —— 接收心跳注册（第 158–188 行）

当执行器向 Admin 发送 `/api/registry` 请求时，最终会调用到这里。

```java
public Response<String> registry(RegistryRequest registryParam) {
    // 1. 参数校验
    if (参数为空) return Response.ofFail("Illegal Argument.");

    // 2. 提交到线程池异步执行
    registryOrRemoveThreadPool.execute(() -> {
        // registrySaveOrUpdate = INSERT ... ON DUPLICATE KEY UPDATE update_time
        int ret = mapper.registrySaveOrUpdate(group, key, value, now);
        //   ret=1 → 新增（之前没有这条记录）
        //   ret=2 → 更新（记录已存在，刷新 update_time）
        if (ret == 1) {
            freshGroupRegistryInfo(registryParam);  // 当前是空方法
        }
    });

    return Response.ofSuccess();  // 立即返回，不等异步执行完
}
```

**对应的 SQL（MyBatis XML）**：

```sql
INSERT INTO xxl_job_registry(registry_group, registry_key, registry_value, update_time)
VALUES(#{registryGroup}, #{registryKey}, #{registryValue}, #{updateTime})
ON DUPLICATE KEY UPDATE update_time = #{updateTime}
```

这是一个 MySQL 的 **upsert** 操作：
- 如果 `(registry_group, registry_key, registry_value)` 这个唯一键不存在 → **INSERT**，返回 `ret=1`
- 如果已存在 → **UPDATE update_time**，返回 `ret=2`

> `freshGroupRegistryInfo()` 目前是个空方法，注释写着"Under consideration, prevent affecting core tables"。原本可能想做即时刷新，但考虑到性能影响改为由监控线程统一维护了。

### 3.4 `registryRemove()` —— 接收下线注销（第 193–215 行）

执行器优雅停机时会主动发注销请求，逻辑和 `registry()` 几乎对称：

```java
public Response<String> registryRemove(RegistryRequest registryParam) {
    // 1. 参数校验
    // 2. 异步执行 DELETE
    registryOrRemoveThreadPool.execute(() -> {
        int ret = mapper.registryDelete(group, key, value);
        // DELETE FROM xxl_job_registry WHERE group=? AND key=? AND value=?
        if (ret > 0) {
            freshGroupRegistryInfo(registryParam);  // 同样是空方法
        }
    });
    return Response.ofSuccess();
}
```

### 3.5 `stop()` —— 优雅停止（第 137–150 行）

```java
public void stop() {
    toStop = true;                              // 通知监控线程退出循环
    registryOrRemoveThreadPool.shutdownNow();    // 关闭线程池
    registryMonitorThread.interrupt();           // 中断 sleep
    registryMonitorThread.join();                // 等待监控线程真正结束
}
```

标准的 Java 多线程优雅停止模式：标志位 + interrupt + join。

---

## 四、整体时序——从执行器启动到任务调度

```
  Executor                          Admin (JobRegistryHelper)          DB
    │                                       │                           │
    │──── registry(group,key,value) ────────▶│                           │
    │                                       │── registrySaveOrUpdate ──▶│
    │                                       │◀── ret=1 (saved) ────────│
    │                                       │                           │
    │     ... 30s later ...                 │                           │
    │──── registry(group,key,value) ────────▶│── UPDATE update_time ───▶│
    │                                       │                           │
    │     ... 30s later ...                 │                           │
    │──── registry(group,key,value) ────────▶│── UPDATE update_time ───▶│
    │                                       │                           │
    │                            ┌──────────│  monitorThread(每30s)     │
    │                            │  清理死亡  │── findDead / removeDead ▶│
    │                            │  聚合存活  │── findAll ──────────────▶│
    │                            │  刷新组地址│── update group ─────────▶│
    │                            └──────────│                           │
    │                                       │                           │
    │  ┌─── 调度器查 group 得到地址列表 ──────│                           │
    │──│── 选择一个地址发送任务 ──────────────│                           │
    │  └────────────────────────────────────│                           │
```

---

## 五、几个容易踩坑的点

| # | 坑 | 说明 |
|---|---|---|
| 1 | **`freshGroupRegistryInfo` 是空方法** | 注册/注销后不会立刻刷新执行器组地址，要等下一轮 monitorThread 巡检（最多 30s）才能生效。新上线的执行器不会"秒级可用"。 |
| 2 | **死亡判定基于 `update_time`** | 如果数据库和应用服务器**时间不同步**，可能出现误判。确保 NTP 同步。 |
| 3 | **`registrySaveOrUpdate` 依赖唯一索引** | `xxl_job_registry` 表必须在 `(registry_group, registry_key, registry_value)` 上建唯一索引，否则 `ON DUPLICATE KEY UPDATE` 会退化成每次都 INSERT，造成重复数据。 |
| 4 | **线程池拒绝策略是调用者运行** | 极端高并发下，如果队列（2000）满了且 10 个线程全忙，HTTP 请求线程会同步执行 DB 操作，可能导致接口变慢。正常情况下不会触发。 |
| 5 | **`addressType=1` 的组不走自动注册** | 手动录入的执行器组，即使有注册心跳到达，监控线程也不会覆盖其 `addressList`。 |

---

## 六、涉及的文件速查

| 文件 | 作用 |
|------|------|
| `JobRegistryHelper.java` | 本文主角，注册核心逻辑 |
| `XxlJobRegistry.java` | 注册记录实体类 |
| `XxlJobGroup.java` | 执行器组实体类（`addressType`、`addressList`） |
| `XxlJobRegistryMapper.xml` | 注册表的 SQL 映射（upsert、findDead、removeDead） |
| `XxlJobGroupMapper.java` | 执行器组 CRUD |
| `RegistryRequest.java` | 注册/注销的请求参数对象 |
| `RegistType.java` | 枚举：`EXECUTOR` / `ADMIN` |
| `Const.java` | `BEAT_TIMEOUT=30`、`DEAD_TIMEOUT=90` |

---

**一句话总结**：`JobRegistryHelper` 就是调度中心和执行器之间的"通讯录管理员"——接收心跳来更新通讯录，定期撕掉过期名片，并且把每个部门（执行器组）当前在岗人员的地址整理好，供调度时使用。
