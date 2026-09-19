# taskflow

内部任务编排服务（任务队列 + DAG 工作流）。

- 运行环境：Python 3.11（运行时只用标准库）
- 开发/测试依赖见 `requirements-dev.txt`
- 跑测试：`python3.11 -m pytest -q`

## 能力

- 用 DAG 描述任务依赖：多个上游全部成功才放行；上游失败/死信，下游标记 `blocked` 不再调度
- 任务即一条 shell 命令，失败按 `base_seconds * 2**(n-1)` 指数退避重试，耗尽进死信（`dead`）
- 死信可手工重放：`retry` 重置为 pending 并级联解锁下游
- 幂等键：同一 `state_dir` 下重复的 `idempotency_key` 不会执行第二遍（标记 `skipped`）
- 崩溃恢复：状态每次变更原子落盘（写临时文件 + fsync + os.replace），kill -9 / 断电后重启，
  已成功的不重跑，中断的按失败一次处理继续重试
- 优雅停机：收到 SIGTERM/SIGINT 后不再派发新任务，在手任务跑完、落盘后退出
- 并发上限：`concurrency` 限制同时在跑的任务数；同一 `state_dir` 有文件锁，防止两个 run 并行

## 配置（JSON）

```json
{
  "state_dir": "state",
  "concurrency": 4,
  "retry": {"base_seconds": 1, "max_attempts": 3},
  "dag_file": "dag.json"
}
```

`state_dir` / `dag_file` 相对路径基于配置文件所在目录解析。

## 任务集合（JSON）

```json
[
  {"id": "extract", "run": "python3 extract.py", "needs": []},
  {"id": "transform", "run": "python3 transform.py", "needs": ["extract"],
   "idempotency_key": "batch-20260919-transform"}
]
```

字段：`id`（必填）、`run`（必填，shell 命令）、`needs`（上游 id 列表，可空）、
`idempotency_key`（可选）。任务一旦进入状态库，其定义即以状态库为准。

## 命令

```bash
python3.11 -m taskflow run    --config <配置.json>            # 全部成功退出码 0，否则非 0
python3.11 -m taskflow status --config <配置.json>            # JSON 输出，可直接给脚本读
python3.11 -m taskflow retry  --config <配置.json> --task <任务id>
```

任务状态：`pending`（等依赖）/ `blocked`（被挡）/ `running`（在跑）/
`success`（成功）/ `failed`（失败待重试）/ `dead`（死信）/ `skipped`（幂等跳过）。

任务 stdout/stderr 追加写入 `<state_dir>/logs/<任务id>.log`。
