# agilelimiter 性能测试工具(插件式)

agilelimiter 的性能与场景测试工具。结构沿用 `go-agile-pool/test`：**每个能力
是一个独立插件，宿主只负责「切段、排依赖、按生命周期调度」**。新增能力 =
新增一个文件 + `plugins.go` 登记一行，框架零改动。

`test/` 是独立 Go 模块，`replace agilelimiter => ../` 强制编译本地库，绝不下发
网络版本。

## 已实现插件

| 段 | 作用 | 关键选项(默认) | 生命周期 |
|---|---|---|---|
| `--limiter` | 创建限流器并持有 | `kind`(rate) `preset`(none) `anchor`(unset) `min`(5) `max`(500) `initial`(0) `slo`(100ms) `tick`(250ms) 以及全部控制律旋钮 | Start 创建并 Start / End Stop |
| `--downstream` | 模拟下游 | `base`(20ms) `capacity`(150/s) `curve`(mm1) `slo`(100ms) `cache`(0) `cache-latency`(1ms) `origin-up`(true) `phases`(空) | — |
| `--load` | 驱动并发负载并汇总 | `workers`(256) `settle`(3s) `dur`(6s) `refuse-sleep`(2ms) | — |
| `--metrics` | 周期采样 + 汇总 | `interval`(1s) `format`(csv) `file`(metrics.csv) `wait-exit`(0) | Start 起采样 / End 收尾 |
| `--profile` | pprof 包络会话 | `cpu`(false) `mem`(false) | Start 开 CPU / End 停 CPU+写 heap |
| `--hcache` | 场景:源站全挂、CDN 仍命中;断言命中不被限流 | `hit`(0.95) `min`(5) `max`(500) `settle`(3s) `dur`(4s) `min-good`(0) | — |

`--limiter` 支持两种执行器：

- `kind=rate` → `agilelimiter.Limiter`(出站,`Take()` 阻塞取令牌);
- `kind=concurrency` → `agilelimiter.ConcurrencyLimiter`(入站,`TryTake()` 非阻塞)。

`--load` 通过统一的 `limiterHandle` 驱动两者；rate 模式用限流器的 target 作为
下游利用率，concurrency 模式用实测吞吐，避免把「并发上限」误当速率。

## 依赖关系

`--load ← --limiter + --downstream`，`--metrics ← --limiter`。
漏写依赖段时宿主按默认参数自动补全(只写 `--load` 会自动带出 limiter/downstream)，
依赖乱序、重复段、成环都会以退出码 2 报错。

## 快速使用

```text
# 查看插件与帮助
go run . --list
go run . --help load

# 稳态基线和自适应收敛
go run . --downstream base=20ms capacity=150 \
         --load workers=256 settle=3s dur=6s \
         --metrics interval=1s format=csv file=metrics.csv

# 容量阶跃:先 150/s,后降到 60/s
go run . --downstream base=20ms phases="0,150;4,60" \
         --load workers=256 settle=4s dur=6s

# 用 preset + SLO 锚
go run . --limiter preset=latency-first slo=100ms max=300 \
         --downstream base=20ms capacity=150 \
         --load workers=256 settle=3s dur=6s

# CDN 场景(当前应 FAIL,复现 TODO.md 的问题)
go run . --hcache
```

`--hcache` 的场景:源站全挂(未命中一律 `Unreachable`),95% 请求由 CDN 命中。
正确的准入策略应让命中继续以边缘容量放行,good/s 接近 `hit x max`;当前实现每个
命中都占一个限流槽位,退到 `MinRate` 后连命中也被挡住,good/s 塌到 `MinRate`
附近。`--min-good` 默认是 `0.5 x hit x max`,失败信息会直接指出原因。

## 退出码

- `0` 成功;
- `1` 运行错误(插件 Start/Run/End 失败,或场景断言 FAIL);
- `2` 用法错误(命令行/依赖规划错误)。

错误带 `[插件名]` 前缀。

## 目录

```text
test/
  README_zh-CN.md  本文档
  README.md        英文版
  main.go          宿主:切段/规划/生命周期调度
  plugin.go        接口(Plugin/Optioned/Lifecycle/Depender)+ Runtime
  option.go        key=value 解析与类型化取值
  store.go         会话级共享 Store(Provide/Get/Require)
  registry.go      注册表(重名/依赖注册校验)
  plan.go          依赖规划(自动补全/顺序/环检测)
  cli.go           段切割 + --list/--help
  plugins.go       集中注册表
  limiter.go       限流器插件 + rate/concurrency 统一句柄
  downstream.go    下游模型(容量/曲线/缓存/源站状态)
  load.go          负载驱动 + 速率估算 + 统计
  metrics.go       周期采样插件
  profile.go       pprof 插件
  hcache.go        CDN/源站宕机场景(PASS/FAIL)
  run_test.bat     Windows 回归脚本
  run_test.sh      Linux/macOS 回归脚本
```

代码注释为英文，与库内其他包风格一致；本 README 为中文。
