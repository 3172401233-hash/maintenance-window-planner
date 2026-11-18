"""维护窗口规划器。

在依赖关系、业务黑窗、副本容量与回滚预算约束下，
为维护任务寻找可行窗口与执行顺序（事件驱动的贪心调度）。

时间粒度：所有时间点对齐到 GRANULARITY（默认 5 分钟）边界，
任务时长与回滚预算必须是粒度的整数倍。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

GRANULARITY = timedelta(minutes=5)
DEFAULT_HORIZON = timedelta(days=7)
_FAR_FUTURE = datetime.max.replace(microsecond=0)


@dataclass(frozen=True)
class Task:
    """一个维护任务。

    - duration: 执行时长
    - depends_on: 前置任务 id，前置任务未完成的计划不能开始
    - deadline: 最晚完成时间（含回滚预算），None 表示只受规划视界限制
    - replicas: 执行期间占用的服务副本数
    - priority: 插队优先级，数值越大越优先抢占容量
    - rollback: 回滚预算，任务结束后仍占用容量的缓冲时间
    """

    task_id: str
    service: str
    duration: timedelta
    depends_on: tuple[str, ...] = ()
    deadline: datetime | None = None
    replicas: int = 1
    priority: int = 0
    rollback: timedelta = timedelta(0)

    @property
    def occupancy(self) -> timedelta:
        """占用容量的总时长（执行 + 回滚预算）。"""
        return self.duration + self.rollback


@dataclass(frozen=True)
class Blackout:
    """业务黑窗。daily=True 时按每天的起止时刻重复（支持跨午夜）。"""

    start: datetime
    end: datetime
    daily: bool = False

    def intervals(self, lo: datetime, hi: datetime):
        """生成与 [lo, hi) 相交的黑窗区间。"""
        if not self.daily:
            if self.end > lo and self.start < hi:
                yield (max(self.start, lo), min(self.end, hi))
            return
        start_t = self.start.time()
        end_t = self.end.time()
        day = lo.date() - timedelta(days=1)
        last = hi.date() + timedelta(days=1)
        while day <= last:
            s = datetime.combine(day, start_t)
            e = datetime.combine(day, end_t)
            if e <= s:  # 跨午夜黑窗，如 22:00 - 次日 02:00
                e += timedelta(days=1)
            if e > lo and s < hi:
                yield (s, e)
            day += timedelta(days=1)


@dataclass(frozen=True)
class ServiceCapacity:
    """服务副本容量。usable = total - min_available 为可并行压上的上限。

    group 相同的多个服务共享同一份容量（如同一数据库的多个依赖服务）。
    """

    total: int
    min_available: int = 0
    group: str | None = None

    @property
    def usable(self) -> int:
        return self.total - self.min_available


@dataclass
class ScheduledWindow:
    task_id: str
    service: str
    start: datetime
    work_end: datetime  # 执行结束时刻
    end: datetime       # 含回滚预算的容量释放时刻


@dataclass
class BlockedTask:
    task_id: str
    reason: str


@dataclass
class Plan:
    windows: list[ScheduledWindow] = field(default_factory=list)
    blocked: list[BlockedTask] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.blocked

    def window_for(self, task_id: str) -> ScheduledWindow | None:
        for w in self.windows:
            if w.task_id == task_id:
                return w
        return None

    def reason_for(self, task_id: str) -> str | None:
        for b in self.blocked:
            if b.task_id == task_id:
                return b.reason
        return None


@dataclass
class _Reservation:
    start: datetime
    end: datetime
    replicas: int
    task_id: str


def _align_up(t: datetime) -> datetime:
    """向上对齐到 GRANULARITY 边界（按绝对时间）。"""
    epoch = datetime(1970, 1, 1)
    delta = t - epoch
    step = GRANULARITY.total_seconds()
    rem = delta.total_seconds() % step
    if rem:
        delta += timedelta(seconds=step - rem)
    return epoch + delta


class Planner:
    def __init__(
        self,
        tasks: list[Task],
        blackouts: list[Blackout] | None = None,
        services: dict[str, ServiceCapacity] | None = None,
        start: datetime | None = None,
        horizon: timedelta = DEFAULT_HORIZON,
    ):
        self.tasks = {t.task_id: t for t in tasks}
        self.blackouts = list(blackouts or [])
        self.services = dict(services or {})
        self.start = _align_up(start or datetime.now())
        self.horizon_end = self.start + horizon
        self._reservations: dict[str, list[_Reservation]] = {}
        self._validate()

    # ---------- 输入校验 ----------

    def _validate(self) -> None:
        step = GRANULARITY.total_seconds()
        for t in self.tasks.values():
            if t.duration <= timedelta(0):
                raise ValueError(f"任务 {t.task_id} 的时长必须为正")
            for name, td in (("时长", t.duration), ("回滚预算", t.rollback)):
                if td.total_seconds() % step:
                    raise ValueError(
                        f"任务 {t.task_id} 的{name}必须是 {GRANULARITY} 的整数倍"
                    )
            if t.replicas <= 0:
                raise ValueError(f"任务 {t.task_id} 的副本数必须为正")

    # ---------- 约束查询 ----------

    def _group_of(self, service: str) -> str | None:
        cap = self.services.get(service)
        if cap is None:
            return None
        return cap.group or service

    def _usable_of(self, group: str | None) -> int | None:
        if group is None:
            return None
        for svc, cap in self.services.items():
            if (cap.group or svc) == group:
                return cap.usable
        return None

    def _blackout_hit(self, start: datetime, end: datetime):
        for b in self.blackouts:
            for s, e in b.intervals(start, end):
                if s < end and e > start:
                    return (s, e)
        return None

    def _capacity_conflict_end(
        self, group: str | None, start: datetime, end: datetime, need: int
    ) -> datetime | None:
        """[start, end) 内若容量超限，返回最早解除冲突的时刻，否则 None。"""
        usable = self._usable_of(group)
        if usable is None:
            return None
        if need > usable:
            return self.horizon_end  # 永远放不下
        res = self._reservations.get(group, [])
        points = {start, end}
        for r in res:
            if start < r.start < end:
                points.add(r.start)
            if start < r.end < end:
                points.add(r.end)
        ordered = sorted(points)
        for i in range(len(ordered) - 1):
            seg_s, seg_e = ordered[i], ordered[i + 1]
            if seg_s >= seg_e:
                continue
            used = sum(r.replicas for r in res if r.start < seg_e and r.end > seg_s)
            if used + need > usable:
                ends = [r.end for r in res if r.start < seg_e and r.end > seg_s]
                return min(ends)
        return None

    def _earliest_slot(
        self, task: Task, not_before: datetime, ignore_capacity: bool = False
    ) -> datetime | None:
        """在 [not_before, deadline] 内寻找满足黑窗与容量约束的最早起点。"""
        need = task.occupancy
        limit = min(task.deadline or _FAR_FUTURE, self.horizon_end)
        group = self._group_of(task.service)
        start = _align_up(not_before)
        while start + need <= limit:
            end = start + need
            hit = self._blackout_hit(start, end)
            if hit:
                start = _align_up(hit[1])
                continue
            if not ignore_capacity:
                conflict = self._capacity_conflict_end(group, start, end, task.replicas)
                if conflict is not None:
                    start = _align_up(conflict)
                    continue
            return start
        return None

    # ---------- 依赖分析 ----------

    def _find_cycle_members(self, candidates: set[str]) -> set[str]:
        """在候选集合中找出处于依赖环上的任务。"""
        members = set()
        for tid in candidates:
            seen, stack = set(), [tid]
            while stack:
                cur = stack.pop()
                for dep in self.tasks[cur].depends_on:
                    if dep == tid:
                        members.add(tid)
                    elif dep in candidates and dep not in seen:
                        seen.add(dep)
                        stack.append(dep)
        return members

    # ---------- 主调度 ----------

    def plan(self) -> Plan:
        result = Plan()
        blocked: dict[str, str] = {}

        pending = dict(self.tasks)
        # 未知依赖
        for t in list(pending.values()):
            unknown = [d for d in t.depends_on if d not in self.tasks]
            if unknown:
                blocked[t.task_id] = f"依赖了不存在的任务: {', '.join(unknown)}"
                del pending[t.task_id]
        # 依赖环
        cycle_members = self._find_cycle_members(set(pending))
        for tid in cycle_members:
            t = pending.pop(tid)
            cycle = sorted(cycle_members)
            blocked[tid] = f"检测到依赖环: {' -> '.join(cycle + [tid])}"

        done: dict[str, datetime] = {}  # task_id -> 容量释放时刻
        now = self.start
        while True:
            ready = [
                t
                for t in pending.values()
                if all(d in done and done[d] <= now for d in t.depends_on)
            ]
            if ready:
                # 插队：高优先级、紧截止的任务先拿容量
                ready.sort(
                    key=lambda t: (-t.priority, t.deadline or _FAR_FUTURE, t.task_id)
                )
                for t in ready:
                    slot = self._earliest_slot(t, now)
                    if slot is None:
                        blocked[t.task_id] = self._diagnose(t, now)
                    else:
                        end = slot + t.occupancy
                        group = self._group_of(t.service)
                        if group is not None:
                            self._reservations.setdefault(group, []).append(
                                _Reservation(slot, end, t.replicas, t.task_id)
                            )
                        done[t.task_id] = end
                        result.windows.append(
                            ScheduledWindow(
                                task_id=t.task_id,
                                service=t.service,
                                start=slot,
                                work_end=slot + t.duration,
                                end=end,
                            )
                        )
                    del pending[t.task_id]
            future = [e for e in done.values() if e > now]
            if not future:
                break
            now = min(future)

        # 剩余任务：因上游被阻塞而无法安排
        for t in pending.values():
            dep = next(
                (d for d in t.depends_on if d in blocked or d not in done), None
            )
            if dep in blocked:
                blocked[t.task_id] = f"依赖的任务 {dep} 被阻塞: {blocked[dep]}"
            else:
                blocked[t.task_id] = f"依赖的任务 {dep} 未能完成"

        result.windows.sort(key=lambda w: (w.start, w.task_id))
        result.blocked = [
            BlockedTask(task_id=tid, reason=reason)
            for tid, reason in sorted(blocked.items())
        ]
        return result

    def _diagnose(self, task: Task, now: datetime) -> str:
        """区分阻塞原因：黑窗/截止时间，还是容量不足。"""
        deadline = task.deadline or self.horizon_end
        group = self._group_of(task.service)
        usable = self._usable_of(group)
        if usable is not None and task.replicas > usable:
            return (
                f"容量不足: 资源组 {group} 需要 {task.replicas} 副本，"
                f"但可用上限仅 {usable}（总副本 - 最低可用副本）"
            )
        if self._earliest_slot(task, now, ignore_capacity=True) is None:
            return (
                f"黑窗冲突: 在最晚完成时间 {deadline:%Y-%m-%d %H:%M} 前"
                f"找不到连续 {task.occupancy} 的非黑窗时段"
            )
        return (
            f"容量不足: 资源组 {group} 在 {deadline:%Y-%m-%d %H:%M} 前"
            f"没有足够的空闲副本容纳 {task.replicas} 个副本"
        )
