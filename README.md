# pooler

worker pool

## 目标

- [x] 用 eventcount 事件通知消除空闲轮询。
- [ ] 提供 `PoolBuilder`、运行指标和可重复的基准测试。
- [ ] 提供有界队列与 `try_execute`，在过载时实现背压。
- [ ] 提供 `execute_batch` 或 `par_for_each`，提高小任务吞吐量。
- [ ] 提供 `execute_affine(key, job)`，让相同 key 的任务优先在同一 worker 上执行，同时保留 work stealing。
- [ ] 提供 `scope` 或 `TaskHandle`，支持等待子任务、返回值、panic 传播与取消语义。
