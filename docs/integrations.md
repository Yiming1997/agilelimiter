# HTTP / gRPC 工程化集成可行性评估

**问题**：能否把 agilelimiter 分别接入 HTTP 和 gRPC？二者各自做一件事即可。

**先说结论**：能，但「http handler 和 grpc」其实是**四个**集成，落在两个本质不同的方向上：

| | 出站（客户端，保护**下游**） | 入站（服务端，保护**自身**） |
|---|---|---|
| HTTP | `http.RoundTripper` 包装 | `func(http.Handler) http.Handler` 中间件 |
| gRPC | `UnaryClientInterceptor` | `UnaryServerInterceptor` |
| 被保护的资源 | 下游依赖 | 本服务的 CPU / 内存 / goroutine |
| 控制动作 | **阻塞**到节拍放行 | **快速拒绝**（429 / ResourceExhausted） |
| 可行性 | 高 | 中（需要新 API 与语义改造） |

出站方向与当前设计（见 `design.md`）完全一致，是纯粹的接入工作；入站方向语义不同，是**另一种产品**，需要改造。下面逐个展开。

---

## 0. 为什么出站 ≠ 入站（必须先分清）

**出站**：`Take()` 阻塞是**对的**。等待的是你自己 worker 的节拍，`Take()` 的等待天然不计入 `Observe`——因为 `Observe` 只测「真正发出→收到」的那段。这正是 `design.md §2` 的测量纪律，客户端接缝天然满足它。

**入站**：`Take()` 阻塞是**危险的**。如果每个入站请求都在中间件里 `Take()` 等放行，你其实是在**建队**：请求连同它占用的 goroutine、socket、连接一起堆在那里。一遇到突发流量，队列本身就会耗尽你正要保护的那批资源——保护机制变成了放大器。入站的正确动作是**快速失败**（HTTP 429 / gRPC `ResourceExhausted`），把过载挡在门外，而不是让它们排队。

由此引出一个更根本的差异：**限速 vs 限并发**。本库的执行器控制的是**速率**（req/s），而保护自身资源、业界（Netflix `concurrency-limits`）更常用的是**并发数**（in-flight）。由 Little's Law `in-flight = 速率 × 处理耗时`，速率固定时，耗时代理变大并发就失控。所以「入站 = 把本库当中间件」只是**能跑但次优**的版本；做得对，执行器要从「漏桶」换成「并发信号量」，那更接近一个兄弟项目，而不是一个包装。

**两条硬结论**：出站限流器和入站限流器**不能是同一个实例**（保护的是两种不同资源）；入站要新 API。

---

## 1. 出站 HTTP：`http.RoundTripper`

**接缝正确性**：`RoundTrip` 是「一个请求真正发出」和「响应返回」之间**唯一**的接缝，恰好夹住一次网络往返。

```go
type limitTransport struct {
	base http.RoundTripper
	lim  *agilelimiter.Limiter
}

func (t *limitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.lim.Take()                      // 阻塞至放行；这段等待不计入下面
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	t.lim.Observe(time.Since(start))  // 纯下游 RTT，符合测量纪律
	return resp, err
}
```

**可行性：高。** 代码约 30 行。真正的工作量在细节：

1. **一个 client 可能打多个 host**。`Transport` 是 per-`http.Client` 的，但你要的是**每个下游依赖一个 limiter**。所以需要一张 `host → *Limiter` 的表（见 §6）。最简单的约定是「一个 Client 对一个下游」，但真实项目往往做不到。
2. **`Observe` 量到的是 TTFB，不是整个 body**。`RoundTrip` 在响应头到达时就返回，body 由调用方惰性读取。多数 API 场景下「头到达」就等于「服务端处理完了」，是**正确**的信号；但如果你要的是下载总耗时，得再包一层 body reader。这一点必须写进文档，否则会误读指标。
3. **错误的延迟语义**（重要，见 §5.3）：连接被拒、立即 5xx 会在**近乎 0 时间**内返回，`Observe(~0)` 会让下游显得**极其健康**，控制器于是**加速**——方向完全反了。

**重试**：若在 `http.Client` 之上还有重试传输，应把 limiter 放在**靠线缆的内层**，让每次重试**都**被节流；放在外层则一次逻辑调用只过一次闸。

---

## 2. 出站 gRPC：`UnaryClientInterceptor`

```go
func UnaryClient(lim *agilelimiter.Limiter) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		lim.Take()
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		lim.Observe(time.Since(start))
		return err
	}
}
```

**可行性：高（unary）/ 中（stream）。**

- **Unary 干净**，与出站 HTTP 同理。区别是**路由粒度**：gRPC 的方法之间代价差异极大，通常要**按 method 分实例**（`method` 参数就是完整方法名），而不是按连接。
- **Stream 难**：`StreamClientInterceptor` 拿到的是一个长期存活的 `grpc.ClientStream`，**没有「一次 RTT」这个量**。可选项：测「发出→收到第一个 header / 第一条消息」（近似 TTFB），或者干脆只 `Take()` 不 `Observe()`。这块要单独设计，不能套用 unary 的公式。
- 同样的**错误语义**问题（`Unavailable`、连接失败返回极快）。

---

## 3. 入站 HTTP：中间件

```go
func Middleware(lim *agilelimiter.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !lim.TryTake() {   // 需要新增的非阻塞 API，见 §5.1
				http.Error(w, "overloaded", http.StatusTooManyRequests)
				return
			}
			start := time.Now()
			next.ServeHTTP(w, r)
			lim.Observe(time.Since(start))  // 信号 = 本服务处理耗时
		})
	}
}
```

**可行性：中。** 中间件本身是标准写法，难点在语义：

1. **信号换了对象**。出站保护下游时 `Observe` 喂的是下游 RTT；入站保护自身时喂的是 **handler 处理耗时**。当本机 CPU / 内存 / 调度发生拥塞，handler 变慢，这个信号**确实**能反映出来，所以映射成立——但它的**物理含义完全不同**，配置与调参都要重新想。
2. **必须用非阻塞**。`Take()` 会阻塞排队，正是 §0 说的「建队」问题。入站要用 `TryTake()` + 拒绝。
3. **粒度**：`/search` 和 `/healthz` 的代价差几个数量级。要么全局一个 limiter（简单，但一个重接口会拖累整体），要么**按路由**分（更准，工作量更大）。
4. **速率 vs 并发**（见 §0）。如果目标是「保护自身」，更稳的是限制**并发数**而非速率。用本库当入站闸门，得到的是「一个会随处理耗时浮动的并发上限」，能用，但不如直接限并发干净。真要做对，执行器得换成并发信号量——那是一个新模式，不是包装。

---

## 4. 入站 gRPC：`UnaryServerInterceptor`

```go
func UnaryServer(lim *agilelimiter.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {
		if !lim.TryTake() {
			return nil, status.Error(codes.ResourceExhausted, "overloaded")
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		lim.Observe(time.Since(start))
		return resp, err
	}
}
```

**可行性：中（unary）/ 中下（stream）。** 与入站 HTTP 同构：拒绝用 `codes.ResourceExhausted`（gRPC 版的 429），按 method 分实例。`StreamServerInterceptor` 同 §2 的 stream 问题。

---

## 5. Limiter 目前缺的 API（改造点）

要做上述集成，需要动一点库本身。按重要性：

**5.1 非阻塞 `TryTake() bool`（入站必需）。** 现有 `Take()` 只会阻塞。入站要的是「拿不到就立刻拒」，所以需要一个不阻塞、失败即返回的准入接口。最小改动；如果要更细，可返回一个 `func()` 释放（并发信号量模式）。

**5.2 上下文感知的 `Take(ctx)`（两个方向都受益）。** 现在 `Take()` 内部是 `time.Sleep`，**不可中断**：如果调用方的 `ctx` 已经被取消，goroutine 仍会把剩下的节流时间睡完。出站 worker 通常无所谓（延时短），但入站遇突发时等待可能很长，应当尊重 `ctx.Done()`。

**5.3 错误 vs 延迟的显式策略（两个方向都必需）。** 「快速失败」不计入延迟信号——一个立刻返回的错误（连接被拒、立即 5xx / `Unavailable`、`ctx` 取消）会让 `Observe` 记到接近 0 的值，于是下游/本服务显得**健康**，控制器**加速**，与意图相反。需要一条明确规则：**没有拿到真实响应时，要么跳过 `Observe`，要么喂一个合成的「坏」值**，绝不能喂接近 0 的延迟。这一条 `design.md §5` 的「错误/超时兜底」其实已经点到，但**还没有落到 API 上**。

> 已于本轮落地：判据不是「有没有 err」而是「**有没有到达下游**」。新增
> `Outcome{Reached, Unreachable, Aborted}` 与 `Config.Classify(error) Outcome`
> （`ConcurrencyConfig` 同款），以及手动路径上的 `Unavailable()`。默认行为不变
> （所有错误按 `Reached` 处理，即喂真实 RTT）；把「连接被拒 / DNS 失败 / 熔断打开」
> 判为 `Unreachable` 时，改为喂一条 ≥ `SLOTarget` 的合成样本，让控制器退到
> `MinRate`/`MinLimit` 继续探活。实测：下游每调用必拒时，默认策略 10s 内把速率从
> 50 抬到 335（危险），分类后 10s 内退到 20（= `MinRate`），恢复后重新爬升。
> 尚未落地的是 5.1 的 `httpserver` 版本（中间件目前对所有被接纳请求一律 `Observe`，
> 需要在 `Wrap` 里捕获状态码才能按 503 分类，涉及包装 `ResponseWriter`，
> 会牵动 streaming 处理器，单独做）。

---

## 6. 横切关注点

- **路由 / 注册表**：出站按 `host`（HTTP）或 `method`（gRPC）键；入站按路由/method。需要一张并发安全的 `key → *Limiter` 表。注意**键会无限增长**（任意 host/method 都会产生新条目），要有淘汰或白名单，否则慢慢泄漏。
- **生命周期**：每个 limiter 都要 `Start()` / `Stop()`；中间件/拦截器卸载时要能停掉后台 tick。
- **可观测性**：把每个 limiter 的 `Rate()` / `Stats()`（rate、baseline、fast）导出到指标系统。一个看不见的自适应限流器无法排障。
- **流式**：HTTP 的 `Observe` = TTFB；gRPC stream 无「单次 RTT」。两处都要在文档里讲清口径。
- **配置面**：`MinRate / MaxRate / InitialRate` 是**每个下游/路由的策略**，需要从配置来，不能硬编码。

---

## 7. 可行性结论

| 集成 | 接缝 | 可行性 | 主要工作量 |
|---|---|---|---|
| 出站 HTTP | `http.RoundTripper` | **高** | 每 host 注册表；错误延迟策略 |
| 出站 gRPC | `UnaryClientInterceptor` | **高**（unary）/ 中（stream） | 每 method 路由；stream 另设 |
| 入站 HTTP | `http.Handler` 中间件 | **中** | 需 `TryTake`；速率→并发语义改造 |
| 入站 gRPC | `UnaryServerInterceptor` | **中** | 同入站 HTTP |

**建议顺序**：先做**出站 HTTP**（最贴设计、改动最小、能立刻验证价值）→ 再做**出站 gRPC**（复用同一套测量纪律）。**入站**如果要做，先补 `TryTake` 和错误策略，并且**单独开一个「自适应并发」模式**（执行器换成信号量），而不是把出站的桶直接套到服务端。

**一句话**：出站是「把现有 limiter 包一层」，可行性高；入站是「把 limiter 的思想换一个执行器用在反方向」，可行性中等，本质上是另一个模式。
