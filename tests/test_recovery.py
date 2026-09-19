"""强杀恢复：kill -9 后重启，未完成的接着跑，已成功的不重跑。"""

import json
import os
import signal
import subprocess
import sys
import time

from conftest import REPO_ROOT


def _wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _kill_tree(pid):
    """递归强杀整棵进程树，模拟 kill -9 / 断电（子进程也不会幸存）。"""
    try:
        out = subprocess.check_output(["pgrep", "-P", str(pid)], text=True)
        children = [int(x) for x in out.split()]
    except subprocess.CalledProcessError:
        children = []
    for child in children:
        _kill_tree(child)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def test_sigkill_restart_resumes_and_never_reruns_success(env):
    a_log = env.dir / "a.log"
    b_log = env.dir / "b.log"
    env.write_dag(
        [
            # a 很快成功；每次执行都会追加一行，重跑会被数出来
            {"id": "a", "run": f"echo a >> {a_log}", "needs": []},
            # b 睡得足够久，保证被杀时一定还在 running
            {"id": "b", "run": f"sleep 5 && echo b >> {b_log}", "needs": ["a"]},
        ]
    )
    env.write_config(concurrency=2, base_seconds=0.05, max_attempts=3)

    # 第一轮：跑到 b 进入 running 后，模拟 kill -9 / 断电（整棵进程树一起杀）
    proc = subprocess.Popen(
        [sys.executable, "-m", "taskflow", "run", "--config", str(env.config_file)],
        cwd=REPO_ROOT,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    state_file = env.state_dir / "state.json"

    def b_is_running():
        if not state_file.exists():
            return False
        tasks = json.loads(state_file.read_text())["tasks"]
        return tasks.get("b", {}).get("status") == "running"

    assert _wait_for(b_is_running), "b 没有进入 running 状态"
    _kill_tree(proc.pid)
    proc.wait()

    # 被杀现场：a 已成功且只跑过一次；b 没跑完
    assert a_log.read_text().strip().splitlines() == ["a"]
    assert not b_log.exists()

    # 第二轮：重启恢复
    result = env.run()
    assert result.returncode == 0, result.stderr

    # a 已成功，绝不重跑；b 被恢复并最终跑完
    assert a_log.read_text().strip().splitlines() == ["a"]
    assert b_log.read_text().strip().splitlines() == ["b"]

    status = env.status()
    assert status["summary"] == {"success": 2}
    by_id = {t["id"]: t for t in status["tasks"]}
    assert by_id["a"]["attempts"] == 1
    # b 第一次被杀掉算一次失败尝试，恢复后又跑了一次
    assert by_id["b"]["attempts"] == 2
    assert by_id["b"]["started_at"] is not None
    assert by_id["b"]["finished_at"] is not None
