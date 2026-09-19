"""死信与手工重放：重试耗尽进死信、下游被挡、retry 后恢复。"""


def test_dead_letter_then_manual_replay(env):
    allow = env.dir / "allow"
    out = env.dir / "out.log"
    env.write_dag(
        [
            {"id": "flaky", "run": f"test -f {allow}", "needs": []},
            {"id": "down", "run": f"echo ok >> {out}", "needs": ["flaky"]},
        ]
    )
    env.write_config(base_seconds=0.05, max_attempts=2)

    # 第一轮：flaky 一直失败，重试耗尽进死信，下游被挡
    result = env.run()
    assert result.returncode == 1
    status = env.status()
    by_id = {t["id"]: t for t in status["tasks"]}
    assert by_id["flaky"]["status"] == "dead"
    assert by_id["flaky"]["attempts"] == 2
    assert by_id["down"]["status"] == "blocked"
    assert not out.exists()

    # 修好环境问题，手工重放死信
    allow.touch()
    replay = env.retry("flaky")
    assert replay.returncode == 0, replay.stderr

    # 第二轮：重放的任务重新调度成功，下游自动解除被挡
    result = env.run()
    assert result.returncode == 0, result.stderr
    assert out.read_text().strip().splitlines() == ["ok"]
    assert env.status()["summary"] == {"success": 2}
