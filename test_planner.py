import unittest
from datetime import datetime, timedelta

from planner import (
    Blackout,
    Planner,
    ServiceCapacity,
    Task,
)

T0 = datetime(2026, 11, 18, 0, 0)  # 规划起点（已对齐 5 分钟粒度）


def hours(n):
    return timedelta(hours=n)


class TestDependencyOrdering(unittest.TestCase):
    def test_dependency_chain_respects_order_and_rollback(self):
        tasks = [
            Task("db-upgrade", "db", hours(2), rollback=hours(1)),
            Task("app-migrate", "app", hours(1), depends_on=("db-upgrade",)),
        ]
        plan = Planner(tasks, start=T0).plan()
        self.assertTrue(plan.ok, plan.blocked)
        db = plan.window_for("db-upgrade")
        app = plan.window_for("app-migrate")
        self.assertEqual(db.start, T0)
        self.assertEqual(db.work_end, T0 + hours(2))
        self.assertEqual(db.end, T0 + hours(3))  # 含回滚预算
        # 依赖未完成（含回滚缓冲）不能开始
        self.assertEqual(app.start, db.end)

    def test_unknown_dependency_blocks(self):
        plan = Planner(
            [Task("a", "svc", hours(1), depends_on=("ghost",))], start=T0
        ).plan()
        self.assertIn("不存在的任务", plan.reason_for("a"))


class TestCrossDayWindow(unittest.TestCase):
    def test_window_crosses_midnight(self):
        # 每天 08:00-20:00 业务黑窗，10 小时任务只能排在夜间并跨日
        blackout = Blackout(
            start=datetime(2026, 1, 1, 8, 0),
            end=datetime(2026, 1, 1, 20, 0),
            daily=True,
        )
        task = Task("big-migration", "db", hours(10))
        plan = Planner([task], blackouts=[blackout], start=T0).plan()
        self.assertTrue(plan.ok, plan.blocked)
        w = plan.window_for("big-migration")
        self.assertEqual(w.start, T0 + hours(20))       # 当天 20:00
        self.assertEqual(w.end, T0 + hours(30))         # 次日 06:00
        self.assertNotEqual(w.start.date(), w.end.date())

    def test_overnight_blackout_wraps_midnight(self):
        # 跨午夜黑窗 22:00 - 次日 06:00
        blackout = Blackout(
            start=datetime(2026, 1, 1, 22, 0),
            end=datetime(2026, 1, 1, 2, 0),
            daily=True,
        )
        task = Task("nightly", "db", hours(2))
        plan = Planner(
            [task], blackouts=[blackout], start=T0 + hours(21)
        ).plan()
        self.assertTrue(plan.ok, plan.blocked)
        w = plan.window_for("nightly")
        self.assertEqual(w.start, T0 + hours(26))  # 次日 02:00 黑窗结束


class TestDependencyCycle(unittest.TestCase):
    def test_cycle_blocks_members_and_downstream(self):
        tasks = [
            Task("a", "s1", hours(1), depends_on=("b",)),
            Task("b", "s2", hours(1), depends_on=("a",)),
            Task("c", "s3", hours(1), depends_on=("a",)),
            Task("d", "s4", hours(1)),
        ]
        plan = Planner(tasks, start=T0).plan()
        self.assertFalse(plan.ok)
        self.assertIn("依赖环", plan.reason_for("a"))
        self.assertIn("依赖环", plan.reason_for("b"))
        # 下游任务因环上的依赖被阻塞
        self.assertIn("被阻塞", plan.reason_for("c"))
        # 无环任务不受影响
        self.assertIsNotNone(plan.window_for("d"))


class TestBlackoutChanges(unittest.TestCase):
    def _task(self):
        return Task("compress", "db", hours(2), deadline=T0 + hours(12))

    def test_added_blackout_pushes_window(self):
        before = Planner([self._task()], start=T0).plan()
        self.assertEqual(before.window_for("compress").start, T0)

        blackout = Blackout(start=T0, end=T0 + hours(4))
        after = Planner([self._task()], blackouts=[blackout], start=T0).plan()
        self.assertEqual(after.window_for("compress").start, T0 + hours(4))

    def test_blackout_covering_all_time_before_deadline_blocks(self):
        blackout = Blackout(start=T0, end=T0 + hours(12))
        plan = Planner([self._task()], blackouts=[blackout], start=T0).plan()
        self.assertFalse(plan.ok)
        self.assertIn("黑窗", plan.reason_for("compress"))


class TestCapacity(unittest.TestCase):
    def test_insufficient_replica_capacity_blocks(self):
        services = {"db": ServiceCapacity(total=3, min_available=2)}  # 可用 1
        task = Task("heavy", "db", hours(1), replicas=2)
        plan = Planner([task], services=services, start=T0).plan()
        self.assertFalse(plan.ok)
        self.assertIn("容量不足", plan.reason_for("heavy"))

    def test_parallel_tasks_share_group_capacity(self):
        # 两个服务属于同一容量组，可用上限 1，必须串行
        services = {
            "svc-a": ServiceCapacity(total=2, min_available=1, group="db-cluster"),
            "svc-b": ServiceCapacity(total=2, min_available=1, group="db-cluster"),
        }
        tasks = [
            Task("t1", "svc-a", hours(2)),
            Task("t2", "svc-b", hours(2)),
        ]
        plan = Planner(tasks, services=services, start=T0).plan()
        self.assertTrue(plan.ok, plan.blocked)
        w1, w2 = plan.window_for("t1"), plan.window_for("t2")
        self.assertTrue(w1.end <= w2.start or w2.end <= w1.start)

    def test_capacity_freed_after_rollback_window(self):
        services = {"db": ServiceCapacity(total=2, min_available=1)}  # 可用 1
        tasks = [
            Task("first", "db", hours(1), rollback=hours(1)),
            Task("second", "db", hours(1)),
        ]
        plan = Planner(tasks, services=services, start=T0).plan()
        first = plan.window_for("first")
        second = plan.window_for("second")
        # 回滚预算期间容量仍被占用
        self.assertEqual(second.start, first.end)

    def test_capacity_contention_until_deadline_blocks(self):
        services = {"db": ServiceCapacity(total=2, min_available=1)}  # 可用 1
        tasks = [
            Task("long", "db", hours(6), priority=5),
            Task("tight", "db", hours(2), deadline=T0 + hours(3)),
        ]
        plan = Planner(tasks, services=services, start=T0).plan()
        self.assertFalse(plan.ok)
        self.assertIn("容量不足", plan.reason_for("tight"))


class TestPreemption(unittest.TestCase):
    def test_high_priority_jumps_queue(self):
        services = {"db": ServiceCapacity(total=2, min_available=1)}  # 可用 1
        tasks = [
            Task("low", "db", hours(2), priority=0),
            Task("high", "db", hours(2), priority=10),
        ]
        plan = Planner(tasks, services=services, start=T0).plan()
        self.assertTrue(plan.ok, plan.blocked)
        high = plan.window_for("high")
        low = plan.window_for("low")
        self.assertEqual(high.start, T0)          # 插队先执行
        self.assertEqual(low.start, high.end)     # 低优先级让位

    def test_urgent_task_beats_earlier_submitted_when_ready_together(self):
        services = {"db": ServiceCapacity(total=2, min_available=1)}  # 可用 1
        tasks = [
            Task("blocker", "db", hours(1)),
            Task("queued-low", "db", hours(1), depends_on=("blocker",), priority=0),
            Task("urgent-fix", "db", hours(1), depends_on=("blocker",), priority=99),
        ]
        plan = Planner(tasks, services=services, start=T0).plan()
        urgent = plan.window_for("urgent-fix")
        queued = plan.window_for("queued-low")
        self.assertEqual(urgent.start, T0 + hours(1))
        self.assertEqual(queued.start, T0 + hours(2))


class TestDeadline(unittest.TestCase):
    def test_deadline_includes_rollback_budget(self):
        task = Task(
            "risky", "db", hours(2), rollback=hours(2), deadline=T0 + hours(3)
        )
        plan = Planner([task], start=T0).plan()
        self.assertFalse(plan.ok)
        self.assertIsNotNone(plan.reason_for("risky"))

    def test_output_contains_start_end_for_every_task(self):
        tasks = [
            Task("ok", "db", hours(1)),
            Task("blocked", "db", hours(1), depends_on=("missing",)),
        ]
        plan = Planner(tasks, start=T0).plan()
        w = plan.window_for("ok")
        self.assertEqual((w.start, w.end), (T0, T0 + hours(1)))
        self.assertIsNotNone(plan.reason_for("blocked"))


if __name__ == "__main__":
    unittest.main()
