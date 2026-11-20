# 增量物化视图维护（支持迟到更新与删除）

按影响分区增量回补的物化视图引擎。底层事实是**只追加的事件流**，视图把事实按
`occurred_at` 的日期分区聚合；迟到更新、迟到删除、跨天移动只会把**受影响的分区**
标记为脏，维护任务只重算这些分区，绝不整表重算。

## 快速开始

```bash
python3 -m app.server 8765        # 启动，无需任何第三方依赖
# 浏览器打开 http://127.0.0.1:8765
```

运行测试：

```bash
python3 -m unittest discover -s tests -v      # 29 个用例
```

## 设计要点

### 1. 事实流与变更捕获

`fact_event` 是不可变的事件表，每次写入追加 `(key, day, category, op, value, occurred_at)`，
并带一个自增 `seq`。一个键的“当前值”= 该键 `seq <= watermark` 的最新事件，`op='delete'`
表示不存在。

写入事实时同步维护 `dirty_partition(view, day, fact_floor, fact_ceil, marked_at)`。
跨分区移动会同时标脏源分区和目标分区。**写入路径不做任何聚合**。

### 2. 分区级增量回补（不是全表重算）

`compute_partition(day, watermark)` 只触碰与该分区相关的键：

```sql
touched = 在该分区出现过事件的键
latest  = 这些键在全流中 seq <= watermark 的最新事件
结果    = latest 中 op='upsert' 且仍属于该分区的键，按 category 聚合
```

关键点：判断“键是否还在这个分区”用的是**全局最新事件**，而不是分区内最新事件。
这样跨天移动与删除才不会在旧分区残留数据。

`verify` 会重算每个已提交分区，并同时比对（a）校验和与事实流是否一致、
（b）分区元数据与聚合行是否一致，用于发现漂移或丢行。

### 3. 版本与水位

- `maintenance_job.watermark`：任务创建时对 `MAX(fact_seq)` 取快照。
  任务只能看到 `seq <= watermark` 的事实。
- 回补期间新写入的事实 `seq > watermark`，**不会**进入本次结果，而是留在
  `dirty_partition` 里等下一轮。
- `view_meta.version`：只有整个任务的**所有分区一次性切换**成功后才 +1。
  失败的、中断的任务绝不推进版本。

### 4. 原子切换与查询一致性

回补先把所有分区算进 `view_partition_staging` / `mv_partition_row_staging`，
然后在**单个 SQLite 事务**里：写入新版本分区行 + 更新 `view_meta.version`
+ 清理脏标记 + 记录切换结果。

`mv_partition_row` 以 `(name, day, version, category)` 为主键，**按天保留最近两代**。
查询默认读当前已提交版本，也可以 `version=` 固定某一代：

```bash
curl 'localhost:8765/api/view?version=1'      # 回补期间读完整旧版本
```

因为切换是原子的，慢查询读到的要么是完整旧版本、要么是完整新版本，
不会出现一半新一半旧的撕裂快照。`tests/test_engine.py::test_queries_never_see_mixed_versions`
用一个并发读者线程验证了这一点。

### 5. 失败重试

- 分区级重试：`_compute_with_retry` 对单个分区最多尝试 `max_attempts` 次；
  进度逐次写入 `maintenance_job.progress`，可在页面看到 `failed → computed`。
- 任务级重试：`retry_job` 生成新任务（`attempt+1`，`retry_of` 指向原任务），
  **不继承**故障注入计划，运维重试不会必然重蹈覆辙。
- 重试次数上限耗尽后拒绝重试。
- `partition_failure` 记录每个分区的连续失败次数，供后续隔离使用。

### 6. 并发锁

`maintenance_lock` 是单行锁，带 `token` 与 `expires_at`：

- `acquire_lock` 在写事务里检查；过期锁视为崩溃遗留，可被抢占。
- 运行中每个分区后刷新锁 TTL。
- 已持有锁时再执行任务会抛 `LockBusy`（HTTP 409）。
- 重复触发默认去重：已有 `queued/running` 任务时返回它而不是新建；
  但如果新请求带明确的崩溃/失败注入参数，且旧任务尚未开始，则把旧任务置为
  `superseded`，以免测试参数被静默吞掉（`running` 中的任务永不被取代）。

### 7. 中断与恢复

`crash_at=N` 模拟进程在算完 N 个分区后猝死：任务置为 `interrupted`，锁保留但很快过期，
**已算好的分区暂存保留**。

`resume_job` 用原来的 watermark、版本区间和分区集合新建任务，并把旧任务的暂存
复制到新任务下，然后正常跑完。结果里 `reused` 列出复用的分区，页面事件流里能看到
`partition_reused`。因为复用与新算的分区在同一事务里切换，最终仍是一次原子提交。

## 页面

- 顶部指标：视图版本、物化水位、待回补分区数、维护锁状态。
- 「事实写入」：生成示例数据，或一键制造迟到更新 / 迟到删除 / 跨天移动 / 新增分区。
- 「维护任务」：触发、执行、模拟中断、恢复、释放锁；可设最大尝试、每分区延迟、
  崩溃点、失败注入。
- 「查询结果」：当前或固定版本的分区表、合计、一致性校验。
- 「待回补分区」「分区代际」：脏分区与各分区代际/水位/校验和。
- 「任务历史 / 任务事件」：版本区间、水位、尝试次数、耗时、切换结果与逐条事件。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 指标、待回补分区、代际、最近任务、锁 |
| GET | `/api/view?version=&day=` | 一致性快照查询，可按版本固定 |
| GET | `/api/verify` | 重算并校验所有已提交分区 |
| GET | `/api/jobs` / `/api/job?id=` | 任务列表 / 任务详情与事件 |
| POST | `/api/facts` | 写入事实（upsert / delete） |
| POST | `/api/trigger` | 触发增量回补（支持 `partitions`、`fail_plan`、`crash_at`、`slow_ms`） |
| POST | `/api/run` / `/api/run-job` | 执行队首 / 指定任务 |
| POST | `/api/retry` / `/api/resume` | 任务重试 / 中断恢复 |
| POST | `/api/release-lock` | 释放维护锁 |
| POST | `/api/seed` / `/api/chaos` / `/api/reset` | 演示数据与场景 |

## 已验证场景（tests/）

- 迟到更新 / 迟到删除 / 跨天移动只回补受影响分区
- 新于水位的写入不泄漏进当前版本，留待下一轮
- 旧版本在回补后仍可完整查询
- 并发读不会看到混合版本
- 重复触发去重、运行中任务不被取代、锁争用返回 409、过期锁可抢占
- 分区级瞬时失败在任务内重试成功；持续失败不推进版本且保留脏标记
- 崩溃后旧版本不变、暂存保留；恢复任务复用暂存并原子切换
- `verify` 能发现校验和篡改与分区聚合行缺失
