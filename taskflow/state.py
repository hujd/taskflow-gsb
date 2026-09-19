"""本地文件状态存储。

状态保存在 ``<state_dir>/state.json``，每次变更通过
「写临时文件 + fsync + os.replace」原子落盘，保证进程被
kill -9 或断电时状态文件不会写坏，只会丢失最后一次变更。
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

STATUS_PENDING = "pending"    # 等依赖
STATUS_BLOCKED = "blocked"    # 被挡（上游失败/死信）
STATUS_RUNNING = "running"    # 在跑
STATUS_SUCCESS = "success"    # 成功
STATUS_FAILED = "failed"      # 失败（等待重试）
STATUS_DEAD = "dead"          # 死信（重试耗尽）
STATUS_SKIPPED = "skipped"    # 幂等键重复，未执行

ALL_STATUSES = (
    STATUS_PENDING,
    STATUS_BLOCKED,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_DEAD,
    STATUS_SKIPPED,
)

# 「好」的终态：依赖它们的下游可以放行
SATISFIED = (STATUS_SUCCESS, STATUS_SKIPPED)
# 会让下游被挡住的状态
BLOCKING = (STATUS_DEAD, STATUS_BLOCKED)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def epoch_to_iso(epoch):
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="milliseconds")


def new_task_record(spec: dict) -> dict:
    return {
        "id": spec["id"],
        "run": spec["run"],
        "needs": list(spec.get("needs") or []),
        "idempotency_key": spec.get("idempotency_key"),
        "status": STATUS_PENDING,
        "attempts": 0,
        "started_at": None,
        "finished_at": None,
        "next_retry_at": None,  # epoch 秒，仅内部使用
        "last_exit_code": None,
        "duplicate_of": None,
    }


class StateStore:
    """state.json 的加载与原子保存。"""

    def __init__(self, state_dir):
        self.state_dir = Path(state_dir)
        self.path = self.state_dir / "state.json"
        self.lock_path = self.state_dir / "taskflow.lock"
        self.logs_dir = self.state_dir / "logs"
        self.data = {"version": 1, "order": [], "tasks": {}, "idempotency": {}}

    @property
    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> None:
        if self.path.exists():
            with open(self.path, "r", encoding="utf-8") as fh:
                self.data = json.load(fh)
            self.data.setdefault("order", [])
            self.data.setdefault("tasks", {})
            self.data.setdefault("idempotency", {})

    def save(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.state_dir / f".state.{os.getpid()}.tmp"
        payload = json.dumps(self.data, ensure_ascii=False, indent=2)
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    @property
    def tasks(self) -> dict:
        return self.data["tasks"]

    @property
    def idempotency(self) -> dict:
        return self.data["idempotency"]

    def ordered_tasks(self) -> list:
        order = self.data["order"]
        tasks = self.data["tasks"]
        known = [tasks[tid] for tid in order if tid in tasks]
        extra = [rec for tid, rec in tasks.items() if tid not in set(order)]
        return known + extra

    def add_task(self, record: dict) -> None:
        self.data["tasks"][record["id"]] = record
        if record["id"] not in self.data["order"]:
            self.data["order"].append(record["id"])


def public_task(record: dict) -> dict:
    """对外（status 输出）的任务视图。"""
    return {
        "id": record["id"],
        "status": record["status"],
        "attempts": record["attempts"],
        "started_at": record["started_at"],
        "finished_at": record["finished_at"],
        "needs": list(record["needs"]),
        "idempotency_key": record["idempotency_key"],
        "run": record["run"],
        "last_exit_code": record["last_exit_code"],
        "next_retry_at": epoch_to_iso(record["next_retry_at"]),
        "duplicate_of": record.get("duplicate_of"),
    }
