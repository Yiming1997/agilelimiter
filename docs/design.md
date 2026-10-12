# 单进程出站自适应限流器设计

**目标**：保护下游依赖。用低开销、可预测的方式把发往下游的速率压到"下游还能健康服务"的水平，同时保留一个**可承诺的静态硬上限**。

**约束（已确认）**

| 维度 | 选择 | 带来的设计后果 |
|---|---|---|
| 作用方向 | 出站，保护下游 | 信号取下游 RTT；控制器感知的是**下游**的健康，不是本机负载 |
| 作用范围 | 单进程/单机 | 无跨节点协调；每个副本各自自适应，全局总量 = 副本数 × 单机值 |
| 观测信号 | 下游 RTT / 服务端耗时；从计划发送时刻计时 | 见 §2 的测量纪律 |
| 硬上限 | 需要 | 静态 ceiling 独立于控制器强制生效 |

---

## 1. 核心思路：分层——漏桶当执行器，反馈当设定点

漏桶擅长的是**"给定速率下，精确、平滑、低开销地放行"**；它不擅长**"决定速率应该是多少"**。把这两件事拆开，各用各的长处：

```mermaid
flowchart LR
    A["下游 RTT 采样<br/>(从计划发送时刻计)"] --> B["信号处理<br/>快/慢 EWMA + 分位数"]
    B --> C["控制器<br/>目标窗口 / AIMD"]
    C --> D["clamp<br/>[R_min, R_max]"]
    D --> E["漏桶执行器<br/>SetRate(R_dyn)"]
    E --> F["实际发往下游"]
    F -.->|响应耗时| A
```

- **执行层（actuator）** = 现有的 leaky bucket，速率设为 `R_dyn`。它把"要发多快"变成精确、平滑的节拍。
- **决策层（controller）** = 反馈模型，根据 RTT 信号输出 `R_dyn`。
- **硬上限** = `clamp(R_dyn, R_min, R_max)`，且**在 actuator 内部再兜一层**（见 §5）。控制器即便出 bug，也越不过 `R_max`。

这么做直接命中漏桶的两个痛点：**"速率该定多少"不再靠猜**（控制器给），**"对下游健康一无所知"被补上**（RTT 反馈）。而漏桶原本的优点——平滑、低开销、可预测——全部保留。

> 关键点：**别把漏桶换掉，把它降级成"执行器"来用。**

---

## 2. 测量：如何避免协调遗漏（Coordinated Omission）

这是整个方案最容易翻车的地方，先讲。

**问题**：如果你只测"请求实际发出 → 收到响应"的时间，那么当系统变慢时，你的发送循环也会跟着卡住、发得更少——于是**最慢的那段反而被采样得最少**，控制器看到的是"还行"，永远不降速，直到彻底崩溃。这就是协调遗漏。

**做法**：从**计划发送时刻**计时。这正是本库 `Take()` 返回计划发牌时刻的价值所在：

```go
t0    := rl.Take()          // 计划发送时刻（若被限流会阻塞到这里）
start := time.Now()         // 实际发出
resp, err := doRequest(ctx)
done  := time.Now()

downstreamRTT := done.Sub(start)  // ① 纯下游信号：实际发出→响应
coLatency     := done.Sub(t0)     // ② 用户体感：计划时刻→响应，含自身节流排队
```

**两个指标的用途要分清：**

- **驱动控制器用 ① `downstreamRTT`**。它是"下游自己花了多久"的干净信号。**不要拿 ② 喂控制器**——`coLatency` 里含着你自己的限流排队：你越是收紧速率，`coLatency` 越高，控制器就会进一步收紧……形成**自反馈跑飞**，一路掉到 `R_min`。这是 latency 驱动限流器最常见的死法之一。
- **② `coLatency` 只用于 SLO 观测/报表**，它才是用户真正体感、且 CO 安全的口径。

**采样纪律**（比公式更重要）：

1. **别让采样停摆**。如果发送由固定数量的 worker 驱动，下游一变慢 worker 就被占满、`Take()` 都调不到，此刻你其实已经"少发了"。要么保证持续按计划节奏投递，要么把这"本该发却没发"的 stall 按计划时刻计入——否则又是 CO。
2. 采样丢弃要计数（channel 满、背压），丢了多少心里要有数。

---

## 3. 控制器：快/慢 EWMA 基线对比 + 分位数

### 3.1 信号处理

对 `downstreamRTT` 维护几条统计量：

- **`fast`**：快速 EWMA（半衰期 ~2–5s），反映**当前**状况。
- **`slow`**：慢速 EWMA（半衰期 ~30–60s），作为**健康基线**。
- **分位数**：滑动窗口（~30–60s）的 `p50` 与 `p99`。用固定对数直方图（每周期原子桶计数后复位）即可，零分配。

**比较要"同类比同类"**，避免量纲错配：

- **控制器主信号**：`fast(p50)` vs `slow(p50)` —— 中位数抬升是**最干净的拥塞信号**。
- **p99 作守卫**：`p99` 用于尾延迟安全带，越过 SLO 时无论主信号如何，直接乘性下压。

### 3.2 调节律（本项目落地实现）

核心是一个**目标窗口 + AIMD**：

```text
high ← slow · (1 + TargetInflation)      // 可容忍的延迟上界
low  ← high · (1 - Deadband)             // 上界之下再留死区

若 fast ≤ low            → R_dyn ← min(R_dyn·(1+UpStep),  R_max)          // 健康：加性增（探测）
若 fast ≥ high           → R_dyn ← max(R_dyn·(1-Gain·excess/high), R_min)  // 拥塞：比例降，封顶 DownStep
否则（死区内）           → R_dyn 不变                                     // 保持
尾延迟守卫: 若 p99 > SLO → R_dyn ← max(R_dyn·(1-DownStep), R_min)         // 无视主信号，直接下压
```

`excess = (fast - high) / high`。**关键点**：不是拿 `fast` 和"空闲基线"直接比，而是允许延迟在基线之上抬升 `TargetInflation` 倍——**容忍一定排队**才不会一有排队就收手、把利用率压到极低。`TargetInflation = 1.0` 表示"容忍到 2× 基线"。这也正是 Netflix Gradient2 里 queue-size target 的思路。

**非对称是关键**：**降得快、升得慢**（乘性减 `DownStep=0.30`、加性增 `UpStep=0.05`）。既对下游友好（一旦苗头不对立刻收手），又能避免在拐点附近来回震荡时反复冲击下游。

**死区/迟滞**：`low < fast < high` 之间不动作，滤掉噪声导致的抖动。

> 备注：设计早期还考虑过纯梯度律 `R_dyn ← R_dyn·(slow/fast)^γ`，但它缺少"容忍排队"的目标点，容易在低利用率处锁死；最终采用上面的目标窗口律。

### 3.3 稳定性旋钮（按重要性）

1. **慢信号 / 长半衰期**——越慢越稳，但越滞后。这是"慢速 EWMA"的全部意义。
2. **单周期变化上限**——无论公式算出多少，每周期 `R_dyn` 变化不超过 ±x%（如 20%），防止阶跃。
3. **死区**（§3.2）。
4. **增益 `Gain` / 步长 `UpStep`·`DownStep`**——调大响应快但易振荡。

### 3.4 ⚠️ 最重要的一条：**基线不能被拥塞污染**

如果用 `slow` 当基线，而下游**长时间**处于拥塞，`slow` 会慢慢爬向拥塞值 → `slow/fast → 1` → 控制器**停止降速，卡在拥塞态出不来**。这是 latency 驱动控制器的头号暗坑。对策（至少做一条）：

- 基线取**低位分位数**或"近期最小值"的慢速平滑，而非均值/中位数——它代表"健康时的样子"。
- 限制基线每单位时间**向上的漂移速度**。
- **周期性向下探测**：偶尔小幅降速，看延迟是否随之改善，以验证"现在真的没余量"还是"基线被污染了"。
- 外部事件（发版、下游重启、错误率跳变）时**重置基线**。

---

## 4. 与现有代码的集成

### 4.1 执行器：给 `atomicInt64Limiter` 加 `SetRate`

现有 `limiter_atomic_int64.go` 里 `perRequest` 和 `maxSlack` 是普通字段。改成原子，运行时热更新：

```go
type adaptiveAtomicInt64 struct {
    prepadding  [64]byte
    state       int64 // 下次放行时刻（unix ns），与速率无关
    postpadding [56]byte
    perRequest  int64 // atomic ns
    maxSlack    int64 // atomic ns（负数）
    clock       Clock
}

func (t *adaptiveAtomicInt64) SetRate(rate int, per time.Duration, slack int) {
    pr := int64(per / time.Duration(rate))
    atomic.StoreInt64(&t.perRequest, pr)
    atomic.StoreInt64(&t.maxSlack, -int64(slack)*pr)
}
```

`Take()` 里把 `t.perRequest` / `t.maxSlack` 换成 `atomic.LoadInt64(...)` 即可。

**为什么改速率不会引起突发或卡顿**：`state` 存的是**绝对时刻**，与速率无关；改速率只影响下一次的**增量** `state + perRequest`。所以热更新**天然不重置状态**，是无缝的。（唯一提醒：自适应场景把 `slack` 设小——slack 的突发缓冲会削弱控制器对速率变化的响应锐度。）

### 4.2 包装与热路径

```go
type AdaptiveLimiter struct {
    inner *adaptiveAtomicInt64 // 执行器
    // 采样用原子/无锁，热路径零分配
    hist  *logHistogram        // 分位数（原子桶）
}

func (a *AdaptiveLimiter) Take() time.Time { return a.inner.Take() }

// 调用方在请求前后喂样本：
func (a *AdaptiveLimiter) Observe(rtt time.Duration) { a.hist.Record(rtt) }
```

### 4.3 控制循环

独立 ticker（如每 1s）跑控制逻辑，与热路径解耦：

```go
func (c *Controller) loop() {
    tk := time.NewTicker(c.tick)          // 1s
    for range tk.C {
        sig := c.hist.SnapshotAndReset()   // 取本周期 p50/p99（原子桶，复位）
        c.updateEWMA(sig)                  // fast / slow / 基线
        next := c.adjust()                 // §3.2 的梯度或 AIMD
        next = clamp(next, c.minR, c.maxR) // 硬上限（第一层）
        c.rate.Store(next)
        c.inner.SetRate(int(next), c.per, c.slack)
    }
}
```

热路径只做一次 `Record`（一个原子加），零锁零分配。

---

## 5. 硬上限与兜底（防御纵深）

硬上限要在**两层**独立生效，任何一层出问题都不会突破：

1. 控制器输出后 `clamp(R_dyn, R_min, R_max)`。
2. actuator 自身：即使被喂了超过 `R_max` 的速率，也不越界。

`R_max` 就是你要求承诺的那个静态值（合同/配额）。`R_min` 建议留一小股**探测流量**（不为 0），这样下游恢复时能立刻被感知。

**强烈建议再加一条错误/超时兜底**（即使你没勾这个信号）：RTT 只反映"慢"，不反映"贵"——下游**直接报错/超时**时，延迟样本可能看起来正常。连续超时 → 直接打到低位，这是安全阀，和延迟信号并行工作。

**冷启动**：没有基线时**从低速起步、慢慢爬升**（对下游友好），而不是从 `R_max` 往下退——后者在启动瞬间就可能压垮下游。

---

## 6. 陷阱清单（按踩坑概率排序）

1. **协调遗漏**——测量纪律，见 §2。
2. **基线被拥塞污染**——长时间过载后卡死，见 §3.4。
3. **自反馈跑飞**——把含自身节流的端到端延迟喂回控制器，见 §2。
4. **进入震荡**——调增益/死区/单步上限；用非对称（慢升快降）。
5. **快慢不匹配**——慢基线滞后，突发事件需要**快速信号**（如 p99 守卫、错误率）做安全阀。
6. **单信号局限**——RTT + 错误率一起看；只看 RTT 会漏掉"快速失败"。
7. **黑盒化**——必须暴露 `R_dyn / R_min / R_max / baseline / fast / p99 / 升降事件计数`。一个不可观测的自适应限流器，出事时无法复盘。

---

## 7. 验证方法

- **确定性单测**：本仓库已经依赖 `github.com/benbjohnson/clock`（mock 时钟）和 `go.uber.org/atomic`——直接拿来做无 flaky 的控制器测试。喂入**构造好的 RTT 序列**（健康→阶跃变慢→恢复），断言 `R_dyn` 的轨迹：能爬升、遇拥塞及时回退、永不超 `R_max`、恢复后能回升。
- **阶跃/混沌测试**：下游 RTT 突增，测量"多快回退到安全值"；恢复后测量"多快爬回 `R_max`"。
- **不变量测试**：任意序列下 `R_min ≤ R_dyn ≤ R_max` 恒成立。
- **CO 验证**：构造"下游变慢导致采样变少"的场景，确认控制器**仍然**能降速。

---

## 8. 一句话总结

**把 `R_dyn` 交给 `Take()` 之外的另一个循环来计算，让漏桶只负责按这个速率平滑放行。** 决策来自"快 EWMA 对比慢 EWMA 基线"（配 p99 守卫、错误兜底），执行复用手上的 leaky bucket，硬上限在控制器和执行器两层强制。测量是一切的地基——**必须从计划发送时刻计时，且采样不能在变慢时停摆**。

---

## 参考

- Netflix `concurrency-limits`（Gradient / Vegas：短 RTT EWMA 对比长 RTT EWMA）
- TCP Vegas / BBR（延迟驱动的拥塞控制）
- AIMD（Jacobson 的 TCP 拥塞控制）
- CoDel（基于排队延迟的主动队列管理）
- Gil Tene — "How NOT to Measure Latency"（协调遗漏）
