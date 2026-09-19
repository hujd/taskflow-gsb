"""DAG 任务集合文件的加载与校验。"""

from __future__ import annotations

import json
from pathlib import Path


class DagError(Exception):
    pass


def load_dag(path) -> list:
    """读取任务集合并做结构校验，返回任务 spec 列表（保持文件顺序）。"""
    path = Path(path)
    if not path.exists():
        raise DagError(f"任务集合文件不存在: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DagError(f"任务集合不是合法 JSON: {exc}") from exc

    if isinstance(raw, dict):
        raw = raw.get("tasks")
    if not isinstance(raw, list):
        raise DagError("任务集合必须是 JSON 数组，或包含 tasks 数组的对象")

    specs = []
    seen_ids = set()
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise DagError(f"第 {i} 个任务不是对象")
        task_id = item.get("id")
        run = item.get("run")
        if not task_id or not isinstance(task_id, str):
            raise DagError(f"第 {i} 个任务缺少字符串 id")
        if not run or not isinstance(run, str):
            raise DagError(f"任务 {task_id!r} 缺少字符串 run")
        if task_id in seen_ids:
            raise DagError(f"任务 id 重复: {task_id!r}")
        seen_ids.add(task_id)
        needs = item.get("needs") or []
        if not isinstance(needs, list) or not all(isinstance(n, str) for n in needs):
            raise DagError(f"任务 {task_id!r} 的 needs 必须是字符串数组")
        specs.append(
            {
                "id": task_id,
                "run": run,
                "needs": needs,
                "idempotency_key": item.get("idempotency_key"),
            }
        )

    for spec in specs:
        for dep in spec["needs"]:
            if dep not in seen_ids:
                raise DagError(f"任务 {spec['id']!r} 依赖了不存在的任务 {dep!r}")
            if dep == spec["id"]:
                raise DagError(f"任务 {spec['id']!r} 依赖了自身")

    _check_cycles(specs)
    return specs


def _check_cycles(specs) -> None:
    """Kahn 拓扑排序，排不完说明有环。"""
    indegree = {s["id"]: len(s["needs"]) for s in specs}
    queue = [tid for tid, deg in indegree.items() if deg == 0]
    visited = 0
    while queue:
        tid = queue.pop()
        visited += 1
        for spec in specs:
            if tid in spec["needs"]:
                indegree[spec["id"]] -= 1
                if indegree[spec["id"]] == 0:
                    queue.append(spec["id"])
    if visited != len(specs):
        raise DagError("任务依赖存在环，无法调度")
