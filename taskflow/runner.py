"""调度执行器。

设计要点：
- 单进程事件循环 + subprocess，并发数受 config.concurrency 限制；
- 每次状态变更立即原子落盘：kill -9 / 断电后重启可恢复，
  已成功的不重跑，中断的（running）按失败一次处理并继续重试；
- 失败按 base_seconds * 2**(attempts-1) 指数退避，耗尽进死信；
- 上游死信/被挡 -> 下游标记 blocked，不再调度；
- 收到 SIGTERM/SIGINT 后不再派发新任务，等在手任务跑完、落盘后退出；
- 幂等键：同一 state_dir 下重复提交的幂等键不会被执行第二遍。
"""

from __future__ import annotations

import fcntl
import os
import signal
import subprocess
import time

from . import state as st
from .dag import load_dag


class RunnerError(Exception):
    pass


class Runner:
    def __init__(self, config):
        self.config = config
        self.store = st.StateStore(config.state_dir)
        self._shutdown = False
        self._procs = {}      # task_id -> Popen
        self._log_fds = {}    # task_id -> 日志文件句柄
        self._dirty = False

    # -- 对外入口 ---------------------------------------------------------

    def run(self) -> int:
        self.store.state_dir.mkdir(parents=True, exist_ok=True)
        self.store.logs_dir.mkdir(parents=True, exist_ok=True)
        lock_fd = self._acquire_lock()
        try:
            self.store.load()
            specs = load_dag(self.config.dag_file)
            self._merge_dag(specs)
            self._recover_interrupted()
            self.store.save()

            signal.signal(signal.SIGTERM, self._on_signal)
            signal.signal(signal.SIGINT, self._on_signal)

            self._loop()
            self.store.save()
            return self._exit_code()
        finally:
            for fh in self._log_fds.values():
                try:
                    fh.close()
                except OSError:
                    pass
            os.close(lock_fd)

    # -- 启动准备 ---------------------------------------------------------

    def _acquire_lock(self) -> int:
        fd = os.open(self.store.lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise RunnerError(
                f"state_dir {self.store.state_dir} 已被另一个 taskflow 进程占用"
            ) from exc
        return fd

    def _on_signal(self, _signum, _frame) -> None:
        # 只置标志位，主循环在下一次迭代时停止派发新任务
        self._shutdown = True

    def _merge_dag(self, specs) -> None:
        """把 DAG 合并进状态：已有记录保持不变；幂等键重复的标记 skipped。"""
        for spec in specs:
            if spec["id"] in self.store.tasks:
                continue
            key = spec.get("idempotency_key")
            if key and key in self.store.idempotency:
                record = st.new_task_record(spec)
                record["status"] = st.STATUS_SKIPPED
                record["finished_at"] = st.now_iso()
                record["duplicate_of"] = self.store.idempotency[key]
                self.store.add_task(record)
                continue
            self.store.add_task(st.new_task_record(spec))
            if key:
                self.store.idempotency[key] = spec["id"]

    def _recover_interrupted(self) -> None:
        """上次进程死掉的现场：running 状态的任务按失败一次处理，立即重试。"""
        for record in self.store.tasks.values():
            if record["status"] != st.STATUS_RUNNING:
                continue
            if record["attempts"] >= self.config.max_attempts:
                record["status"] = st.STATUS_DEAD
                record["finished_at"] = st.now_iso()
            else:
                record["status"] = st.STATUS_FAILED
                record["next_retry_at"] = time.time()

    # -- 主循环 -----------------------------------------------------------

    def _loop(self) -> None:
        while True:
            self._reap_finished()
            self._propagate_blocked()
            if not self._shutdown:
                self._start_ready()
            if self._dirty:
                self.store.save()
                self._dirty = False
            if self._is_done():
                return
            time.sleep(0.05)

    def _is_done(self) -> bool:
        if self._procs:
            return False
        if self._shutdown:
            # 优雅停机：在手任务已跑完，剩下的留给下次 run
            return True
        active = {st.STATUS_PENDING, st.STATUS_FAILED, st.STATUS_RUNNING}
        return not any(rec["status"] in active for rec in self.store.tasks.values())

    def _reap_finished(self) -> None:
        for task_id, proc in list(self._procs.items()):
            rc = proc.poll()
            if rc is None:
                continue
            del self._procs[task_id]
            fh = self._log_fds.pop(task_id, None)
            if fh is not None:
                fh.close()
            record = self.store.tasks[task_id]
            record["last_exit_code"] = rc
            if rc == 0:
                record["status"] = st.STATUS_SUCCESS
                record["finished_at"] = st.now_iso()
                record["next_retry_at"] = None
            elif record["attempts"] >= self.config.max_attempts:
                record["status"] = st.STATUS_DEAD
                record["finished_at"] = st.now_iso()
                record["next_retry_at"] = None
            else:
                record["status"] = st.STATUS_FAILED
                delay = self.config.base_seconds * (2 ** (record["attempts"] - 1))
                record["next_retry_at"] = time.time() + delay
            self._dirty = True

    def _propagate_blocked(self) -> None:
        """上游死信/被挡 -> 下游 pending 任务标记 blocked（沿 DAG 传递）。"""
        changed = True
        while changed:
            changed = False
            for record in self.store.tasks.values():
                if record["status"] != st.STATUS_PENDING:
                    continue
                deps = (self.store.tasks[d] for d in record["needs"])
                if any(d["status"] in st.BLOCKING for d in deps):
                    record["status"] = st.STATUS_BLOCKED
                    record["finished_at"] = st.now_iso()
                    changed = True
                    self._dirty = True

    def _deps_satisfied(self, record) -> bool:
        return all(
            self.store.tasks[d]["status"] in st.SATISFIED for d in record["needs"]
        )

    def _start_ready(self) -> None:
        now = time.time()
        for record in self.store.ordered_tasks():
            if len(self._procs) >= self.config.concurrency:
                return
            status = record["status"]
            if status == st.STATUS_PENDING:
                if not self._deps_satisfied(record):
                    continue
            elif status == st.STATUS_FAILED:
                retry_at = record["next_retry_at"]
                if retry_at is None or retry_at > now:
                    continue
            else:
                continue
            self._launch(record)

    def _launch(self, record) -> None:
        record["attempts"] += 1
        record["status"] = st.STATUS_RUNNING
        if record["started_at"] is None:
            record["started_at"] = st.now_iso()
        log_path = self.store.logs_dir / f"{record['id']}.log"
        fh = open(log_path, "ab")
        fh.write(
            f"\n===== attempt {record['attempts']} @ {st.now_iso()} =====\n".encode()
        )
        fh.flush()
        proc = subprocess.Popen(
            record["run"],
            shell=True,
            stdout=fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self._procs[record["id"]] = proc
        self._log_fds[record["id"]] = fh
        self._dirty = True

    def _exit_code(self) -> int:
        ok = {st.STATUS_SUCCESS, st.STATUS_SKIPPED}
        if all(rec["status"] in ok for rec in self.store.tasks.values()):
            return 0
        return 1


def replay_task(store: st.StateStore, task_id: str) -> dict:
    """手工重放：把 dead/failed/blocked 任务重置为 pending，
    并级联解锁因它而被挡住的下游 blocked 任务。"""
    record = store.tasks.get(task_id)
    if record is None:
        raise RunnerError(f"任务不存在: {task_id!r}")
    replayable = (st.STATUS_DEAD, st.STATUS_FAILED, st.STATUS_BLOCKED)
    if record["status"] not in replayable:
        raise RunnerError(
            f"任务 {task_id!r} 当前状态为 {record['status']}，"
            f"只有 dead/failed/blocked 状态可以重放"
        )
    _reset_record(record)
    changed = True
    while changed:
        changed = False
        for rec in store.tasks.values():
            if rec["status"] != st.STATUS_BLOCKED:
                continue
            deps = (store.tasks[d] for d in rec["needs"])
            if not any(d["status"] in st.BLOCKING for d in deps):
                _reset_record(rec)
                changed = True
    store.save()
    return record


def _reset_record(record: dict) -> None:
    record["status"] = st.STATUS_PENDING
    record["attempts"] = 0
    record["started_at"] = None
    record["finished_at"] = None
    record["next_retry_at"] = None
    record["last_exit_code"] = None
