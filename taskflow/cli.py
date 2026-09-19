"""命令行入口：run / status / retry。"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from . import state as st
from .dag import DagError, load_dag
from .runner import Runner, RunnerError, replay_task


class ConfigError(Exception):
    pass


@dataclass
class Config:
    state_dir: Path
    concurrency: int
    base_seconds: float
    max_attempts: int
    dag_file: Path


def load_config(path: str) -> Config:
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"配置文件不存在: {path}")
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"配置文件不是合法 JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("配置文件必须是 JSON 对象")

    base_dir = config_path.resolve().parent

    def _path(key: str) -> Path:
        value = raw.get(key)
        if not value or not isinstance(value, str):
            raise ConfigError(f"配置缺少字符串字段: {key}")
        p = Path(value)
        return p if p.is_absolute() else (base_dir / p)

    state_dir = _path("state_dir")
    dag_file = _path("dag_file")

    concurrency = raw.get("concurrency")
    if not isinstance(concurrency, int) or concurrency < 1:
        raise ConfigError("配置字段 concurrency 必须是 >= 1 的整数")

    retry = raw.get("retry")
    if not isinstance(retry, dict):
        raise ConfigError("配置缺少 retry 对象（含 base_seconds / max_attempts）")
    base_seconds = retry.get("base_seconds")
    max_attempts = retry.get("max_attempts")
    if not isinstance(base_seconds, (int, float)) or base_seconds < 0:
        raise ConfigError("retry.base_seconds 必须是非负数字")
    if not isinstance(max_attempts, int) or max_attempts < 1:
        raise ConfigError("retry.max_attempts 必须是 >= 1 的整数")

    return Config(
        state_dir=state_dir,
        concurrency=concurrency,
        base_seconds=float(base_seconds),
        max_attempts=max_attempts,
        dag_file=dag_file,
    )


def _cmd_run(config: Config) -> int:
    try:
        return Runner(config).run()
    except (RunnerError, DagError) as exc:
        print(f"taskflow: {exc}", file=sys.stderr)
        return 2


def _cmd_status(config: Config) -> int:
    store = st.StateStore(config.state_dir)
    store.load()
    if not store.exists:
        # 还没跑过：按 DAG 输出全 pending，方便脚本预判
        try:
            specs = load_dag(config.dag_file)
        except DagError as exc:
            print(f"taskflow: {exc}", file=sys.stderr)
            return 2
        for spec in specs:
            store.add_task(st.new_task_record(spec))
    tasks = [st.public_task(rec) for rec in store.ordered_tasks()]
    summary: dict = {}
    for task in tasks:
        summary[task["status"]] = summary.get(task["status"], 0) + 1
    payload = {
        "state_dir": str(store.state_dir),
        "summary": summary,
        "tasks": tasks,
    }
    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


def _cmd_retry(config: Config, task_id: str) -> int:
    store = st.StateStore(config.state_dir)
    store.load()
    if not store.exists:
        print("taskflow: 还没有任何运行状态，无可重放的任务", file=sys.stderr)
        return 1
    try:
        record = replay_task(store, task_id)
    except RunnerError as exc:
        print(f"taskflow: {exc}", file=sys.stderr)
        return 1
    print(
        f"任务 {record['id']} 已重置为 pending，重新执行 run 后将被调度",
        file=sys.stderr,
    )
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="taskflow", description="本地 DAG 任务编排服务"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="调度执行 DAG 中的任务")
    p_run.add_argument("--config", required=True, help="配置文件路径（JSON）")

    p_status = sub.add_parser("status", help="输出任务状态（JSON）")
    p_status.add_argument("--config", required=True, help="配置文件路径（JSON）")

    p_retry = sub.add_parser("retry", help="重放死信/失败任务")
    p_retry.add_argument("--config", required=True, help="配置文件路径（JSON）")
    p_retry.add_argument("--task", required=True, help="要重放的任务 id")

    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"taskflow: {exc}", file=sys.stderr)
        return 2

    if args.command == "run":
        return _cmd_run(config)
    if args.command == "status":
        return _cmd_status(config)
    if args.command == "retry":
        return _cmd_retry(config, args.task)
    return 2  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
