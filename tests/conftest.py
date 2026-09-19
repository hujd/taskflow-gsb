import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


class Env:
    """一套独立的 taskflow 运行环境（临时目录 + 配置 + DAG）。"""

    def __init__(self, tmp_path: Path):
        self.dir = tmp_path
        self.state_dir = tmp_path / "state"
        self.dag_file = tmp_path / "dag.json"
        self.config_file = tmp_path / "config.json"

    def write_dag(self, tasks) -> None:
        self.dag_file.write_text(json.dumps(tasks), encoding="utf-8")

    def write_config(self, concurrency=2, base_seconds=0.05, max_attempts=2) -> None:
        config = {
            "state_dir": str(self.state_dir),
            "concurrency": concurrency,
            "retry": {"base_seconds": base_seconds, "max_attempts": max_attempts},
            "dag_file": str(self.dag_file),
        }
        self.config_file.write_text(json.dumps(config), encoding="utf-8")

    def cli(self, *args, **kwargs):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT)
        return subprocess.run(
            [sys.executable, "-m", "taskflow", *args],
            capture_output=True,
            text=True,
            env=env,
            **kwargs,
        )

    def run(self, **kwargs):
        return self.cli("run", "--config", str(self.config_file), **kwargs)

    def status(self) -> dict:
        proc = self.cli("status", "--config", str(self.config_file))
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout)

    def retry(self, task_id: str):
        return self.cli(
            "retry", "--config", str(self.config_file), "--task", task_id
        )

    def task_status(self, task_id: str) -> str:
        for task in self.status()["tasks"]:
            if task["id"] == task_id:
                return task["status"]
        raise AssertionError(f"任务 {task_id} 不在 status 输出里")

    def read_state(self) -> dict:
        return json.loads((self.state_dir / "state.json").read_text())


@pytest.fixture
def env(tmp_path) -> Env:
    return Env(tmp_path)
