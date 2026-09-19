"""幂等：相同 idempotency_key 重复提交，不会执行第二遍。"""


def test_duplicate_idempotency_key_runs_once(env):
    log = env.dir / "calls.log"
    env.write_dag(
        [
            {"id": "t1", "run": f"echo one >> {log}", "needs": [],
             "idempotency_key": "batch-42"},
            # 与 t1 相同的幂等键：重复提交，绝不能执行
            {"id": "t2", "run": f"echo two >> {log}", "needs": [],
             "idempotency_key": "batch-42"},
            {"id": "t3", "run": f"echo three >> {log}", "needs": ["t1"]},
        ]
    )
    env.write_config()

    first = env.run()
    assert first.returncode == 0, first.stderr
    assert log.read_text().strip().splitlines() == ["one", "three"]
    assert env.task_status("t2") == "skipped"

    # 整个批次再提交一遍：已成功的不重跑，重复键依然不执行
    second = env.run()
    assert second.returncode == 0, second.stderr
    assert log.read_text().strip().splitlines() == ["one", "three"]

    # 新一轮提交里又带了同一个幂等键：仍然不执行
    env.write_dag(
        [
            {"id": "t4", "run": f"echo four >> {log}", "needs": [],
             "idempotency_key": "batch-42"},
        ]
    )
    third = env.run()
    assert third.returncode == 0, third.stderr
    assert log.read_text().strip().splitlines() == ["one", "three"]
    assert env.task_status("t4") == "skipped"
