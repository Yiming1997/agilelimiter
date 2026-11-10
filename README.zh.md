# agileLimiter

[![CI](https://github.com/Yiming1997/agilelimiter/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Yiming1997/agilelimiter/actions/workflows/ci.yml)
[![Go >= 1.24](https://img.shields.io/badge/go-%3E%3D1.24-00ADD8)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tag](https://img.shields.io/github/v/tag/Yiming1997/agilelimiter?label=tag)](https://github.com/Yiming1997/agilelimiter/tags)
[![Go Reference](https://pkg.go.dev/badge/github.com/Yiming1997/agilelimiter.svg)](https://pkg.go.dev/github.com/Yiming1997/agilelimiter)

给**出站依赖**做自适应限速，给**入站服务**做自适应并发限制。

`agileLimiter` 观察延迟，自动调整放行量。它是一个零依赖的小型 Go 模块：

- **出站**：用 `Limiter` 给出站调用限速
- **入站**：用 `ConcurrencyLimiter` 限制本服务的并发
- **有安全地板**：正常流量不会低于 `FloorRate`；故障时可以降到更低的 `ProbeRate`

英文版：[README.md](README.md)

## 安装

```bash
go get github.com/Yiming1997/agilelimiter
```

需要 Go 1.24+，无外部依赖。

## 出站：保护下游依赖

```go
import "github.com/Yiming1997/agilelimiter"

lim := agilelimiter.New(agilelimiter.Config{
    MaxRate:   500,                    // 硬上限
    FloorRate: 250,                    // 正常地板：你信得过的静态速率
    ProbeRate: 10,                     // 故障地板：继续探测，但不要猛打
    SLOTarget: 200 * time.Millisecond, // 可选：延迟红线
})
lim.Start()
defer lim.Stop()

// Do 会取一个配额，计时，并把结果反馈给控制器
err := lim.Do(func() error {
    return callDownstream(ctx)
})
```

`MaxRate` 是唯一必填项。  
不知道 Floor/Probe 时先只设 `MaxRate`；知道静态安全速率后再加 `FloorRate`。

### HTTP client

```go
import "github.com/Yiming1997/agilelimiter/httpclient"

client := &http.Client{
    Transport: httpclient.Wrap(http.DefaultTransport, lim),
}
```

`httpclient.Wrap` 会自动限速、计时并反馈。  
缓存命中、重定向、快速错误等不该计入容量的响应，用 `httpclient.WithVerdict` 标出来。

### 手动 Take / Observe

```go
lim.Take()
start := time.Now()
err := callDownstream(ctx)
lim.ObserveOutcome(time.Since(start), err)
```

如果调用**根本没到达下游**，让 `Config.Classify` 返回 `agilelimiter.Unreachable`，  
或在手动路径调用 `lim.Unavailable()`。不要把拒连当成“下游很快”。

## 入站：保护自己的服务

入站时，客户端决定请求什么时候来；你能控制的是**同时处理多少**。

```go
lim := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
    MaxLimit:   500, // 硬上限
    FloorLimit: 100, // 正常地板
    ProbeLimit: 10,  // 故障地板
    SLOTarget:  200 * time.Millisecond,
})
lim.Start()
defer lim.Stop()

err := lim.Do(func() error {
    return handleRequest(w, r)
})
if errors.Is(err, agilelimiter.ErrOverloaded) {
    http.Error(w, "overloaded", http.StatusTooManyRequests)
}
```

入站准入是**非阻塞**的：到上限时直接返回 `ErrOverloaded`，不会排队。  
如果要跨多个调用持有配额，用 `TryTake` / `Release`。

### HTTP 中间件

```go
import "github.com/Yiming1997/agilelimiter/httpserver"

root := http.NewServeMux()
root.Handle("/debug/limiter", httpserver.StatsHandler(lim))
root.Handle("/", httpserver.Wrap(mux, lim,
    httpserver.WithRetryAfter(time.Second)))

http.ListenAndServe(":8080", root)
```

达到上限时 `httpserver.Wrap` 返回 `429`，并且只记录真正进入 handler 的请求。  
`StatsHandler` 放在 wrapper 外面，服务饱和时也能读到状态。

## 配置

大多数服务只需要这几个字段：

| 字段 | 含义 |
|---|---|
| `MaxRate` / `MaxLimit` | 硬上限，必填 |
| `FloorRate` / `FloorLimit` | 正常运行地板，接近你信得过的静态速率 |
| `ProbeRate` / `ProbeLimit` | 下游不可达时的地板，尽量低 |
| `SLOTarget` | 可选延迟目标，启用尾延迟保护 |
| `Classify` | 哪些错误算“没到达下游” |

`MinRate` / `MinLimit` 仍可作为兼容 fallback：  
`FloorRate` 或 `ProbeRate` 为 0 时，会继承 `MinRate`。

Preset 是可选快捷方式：

```go
Preset: agilelimiter.PresetLatencyFirst
```

建议先用零值 preset + 明确的 `MaxRate`/`FloorRate`；  
只有确认策略匹配时再加 preset。

## 它是怎么动的

控制器每个 tick 看中位数和尾延迟：

- 延迟健康：尝试多放一点
- 延迟超过目标窗口：回退
- 在窗口内：保持
- 尾延迟超过 SLO：回退

它会维护一个缓慢变化的正常延迟基线，所以短时抖动不会立刻改写目标。  
下游不可达时，速率可以降到 `ProbeRate`；收到正常响应后回到正常地板。

这里不需要你调公式。完整控制律和实测权衡在 [docs/design.md](docs/design.md)。

## 可观测性

```go
log.Printf("rate=%.0f stats=%+v", lim.Rate(), lim.Stats())
log.Printf("config=%+v", lim.Config())
```

建议至少导出：

- `Rate()` / `Limit()`
- `Stats().Samples`：为 0 表示控制器“瞎了”
- `Stats().P50` / `Stats().P99`
- `Config()`：实际生效的配置

## 注意事项

- **不是熔断器**。需要“完全停止调用”时，配合 circuit breaker。
- **只在单进程内生效**。多实例各自学习、各自限制。
- **不排队**。入站过载直接拒绝，不缓冲。
- **不做分布式配额**。
- `FloorRate` 是对容量的判断。真实容量跌破 Floor 时，地板不再安全；  
  持续在 floor 运行要告警。
- 测的是**下游往返时间**，不是 limiter 内部等待时间；`Do` 已经处理好了。

## 示例和测试

```bash
go run ./example                 # 出站
go run ./example/concurrency     # 入站
go run ./example/httpserver      # HTTP 中间件

go test ./...
go test -race ./...
```

更多：

- [docs/design.md](docs/design.md) — 控制律与设计权衡
- [docs/integrations.md](docs/integrations.md) — HTTP/gRPC 集成
- [httpclient](httpclient/) / [httpserver](httpserver/) — 开箱适配器
- [sim/](sim/) — 虚拟时钟仿真与对比

## License

MIT。见 [LICENSE](LICENSE)。
