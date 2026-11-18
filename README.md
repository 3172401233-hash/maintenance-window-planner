# maintenance-window-planner

Python 维护窗口规划库：在依赖关系、业务黑窗、副本容量和回滚预算约束下，
为维护任务寻找可行窗口与执行顺序，并输出每个任务的起止时间或阻塞原因。

## 用法

```python
from datetime import datetime, timedelta
from planner import Blackout, Planner, ServiceCapacity, Task

tasks = [
    Task("db-upgrade", "db", timedelta(hours=2), rollback=timedelta(hours=1)),
    Task("app-migrate", "app", timedelta(hours=1),
         depends_on=("db-upgrade",), priority=10),
]
blackouts = [Blackout(datetime(2026, 1, 1, 8), datetime(2026, 1, 1, 20), daily=True)]
services = {
    "db":  ServiceCapacity(total=4, min_available=2, group="db-cluster"),
    "app": ServiceCapacity(total=4, min_available=2, group="db-cluster"),
}
plan = Planner(tasks, blackouts=blackouts, services=services,
               start=datetime(2026, 11, 18)).plan()

for w in plan.windows:   # 每个已排任务的起止时间
    print(w.task_id, w.start, "->", w.end)
for b in plan.blocked:   # 每个被阻塞任务的原因
    print(b.task_id, b.reason)
```

## 时间粒度

- 所有时间点对齐到 `GRANULARITY`（默认 **5 分钟**）的绝对边界，规划起点向上取整对齐。
- 任务时长 `duration` 与回滚预算 `rollback` 必须是粒度的整数倍，否则 `Planner` 抛出 `ValueError`。
- 任务窗口为左闭右开区间 `[start, end)`；`end = start + duration + rollback`，
  其中 `work_end = start + duration` 是执行结束时刻，回滚预算期间容量仍被占用。
- 黑窗支持一次性区间，也支持 `daily=True` 的每日重复（起止时刻按当天解释，
  结束时刻不晚于起始时刻时视为跨午夜，如 22:00–次日 02:00），因此窗口可以跨日。
- `deadline` 为最晚完成时间，按含回滚预算的 `end` 判定；缺省只受规划视界
  （`horizon`，默认 7 天）限制。

## 并行约束

- **依赖**：`depends_on` 中的前置任务未完成（含回滚缓冲释放）前，后续任务不能开始；
  依赖环上的任务及其下游会被阻塞并给出原因。
- **容量**：每个服务声明 `total` 与 `min_available`，可并行压上的副本上限为
  `usable = total - min_available`。`group` 相同的多个服务共享同一组容量——
  并行执行不能同时压掉同一组容量，超限的任务会被推迟到容量释放之后。
- **回滚预算**：任务结束后 `rollback` 时长内仍占用副本，防止回滚期间容量被其他任务抢占。
- **插队**：同一时刻就绪的任务按 `(-priority, deadline, task_id)` 排序，
  高优先级任务先占用容量，低优先级任务顺延。

## 阻塞原因

`plan.blocked` 中每个任务附带中文原因，可能的类别：

- `依赖了不存在的任务` / `检测到依赖环` / `依赖的任务 X 被阻塞`
- `黑窗冲突`：最晚完成时间前没有连续可用的非黑窗时段
- `容量不足`：需求副本数超过可用上限，或截止前容量被其他任务占满

## 测试

```bash
python -m unittest
```

覆盖跨日窗口、依赖环、黑窗变化、容量不足与插队等场景。
