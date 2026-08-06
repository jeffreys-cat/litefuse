# Doris Telemetry 读取与删除说明

本文描述当前 Doris telemetry 的运行模型。适用于 trace、observation/span 及其聚合指标；不涵盖仍使用共享表的 `scores`、`dataset_run_items_rmt` 和 blob log。

## 结论

- 每个项目只读写自己的分表，不再读取或回退到共享 `events_full`、`traces_scalar`。
- `events_full_<projectId>` 同时保存 trace root 和 observation/span；没有物理 `observations` 表。
- 自动数据保留由 Doris 的 dynamic partition TTL 完成，分区时间列是 `start_time`，不是 `created_at`。
- 用户主动删除 trace 时，系统仍会立即执行 Doris `DELETE`；项目删除时直接 `DROP TABLE`。TTL 不替代这两类主动清理。

## 1. 表与数据模型

每个项目 `p` 有以下 Doris 对象：

| 逻辑数据       | 物理对象                | 用途                                                                    |
| -------------- | ----------------------- | ----------------------------------------------------------------------- |
| 全量 span 事件 | `events_full_<p>`       | 根 span（trace）和非根 span（observation）均在此表；用 `is_root` 区分。 |
| Trace 标量镜像 | `traces_scalar_<p>`     | Trace 列表、书签、公开状态、标签等需要快速访问的标量字段。              |
| Trace 指标聚合 | `trace_metrics_agg_<p>` | `events_full_<p>` 的同步物化视图，用于 trace 聚合指标。                 |

`trace_metrics_agg_<p>` 是 `events_full_<p>` 的同步 MV，不是独立写入目标。删除或 drop base table 后，MV 随之维护或移除。

项目 ID 会在生成物理表名之前校验，只允许受控逻辑表加合法项目 ID 生成标识符；业务调用方不能自行拼接表名。

## 2. 读取逻辑

### 单项目读取

所有单项目 telemetry 查询通过 `tableFor(projectId, logicalTable)` 路由：

```text
events_full   -> events_full_<projectId>
traces_scalar -> traces_scalar_<projectId>
```

路由不再取决于 split cache 是否已经标记为 LIVE，也不会回退到共享表。cache 只表示分表是否已经完成 provisioning，可用于控制 ingestion lane 是否允许出库。

### 跨项目读取

跨项目统计必须显式传入 `projectIds`，然后对每个项目生成一个分表查询目标：

```text
[p1, p2, p3] -> [events_full_p1, events_full_p2, events_full_p3]
```

执行器默认最多并发 8 个 Doris 查询，并在应用侧合并结果。不会再查询共享 telemetry 表，也不会自动扫描所有项目表。需要“找到第一个匹配项”的场景会在发现匹配后停止继续分派新查询；已经发出的查询允许完成。

### 分表尚未创建或暂时缺失

读取 `events_full_<p>`、`traces_scalar_<p>` 或 `trace_metrics_agg_<p>` 遇到 Doris 的缺表错误时，读接口返回空数组、空结果或 `0`，并记录 `langfuse.doris.split_table.read_missing` 指标和结构化日志。

这是有意的降级：旧共享 telemetry 历史不会因此重新可见，也不会让页面/API 因某个旧项目未 provision 而失败。缺表应由 provisioning/retry 修复，而不是通过 shared fallback 掩盖。

## 3. 写入与就绪关系

OTel 文件始终先进入项目专属 lane：`lane-<projectId>`。没有控制记录的项目会先被 designate 为 pending，并触发异步 provisioning。

在以下对象就绪前，grouper 不会 cut pending lane：

1. `events_full_<p>` 已存在；
2. `traces_scalar_<p>` 已存在；
3. `trace_metrics_agg_<p>` 的构建已完成。

就绪后 group job 将数据 load 到该项目的两个 base table。若写入时发现分表缺失，活跃项目会触发 reprovision 后重试；已删除项目进入 dead-letter/ledger。无论 `LITEFUSE_OTEL_GROUPING_ENABLED` 的值为何，telemetry 都不会写回 shared shard 或共享 telemetry 表。

## 4. 删除逻辑

### 4.1 主动删除单个或多个 trace

trace delete worker 对同一批 `traceIds` 并行执行以下清理：

1. 从 `events_full_<p>` 按 `project_id + trace_id` 删除；该操作同时删除 root trace 和所有 `is_root = 0` observation/span，**只执行一次**。
2. 从 `traces_scalar_<p>` 按 `project_id + id` 删除，避免 trace 列表镜像残留。
3. 从共享 `scores` 删除关联分数。
4. 如启用 blob-storage file log，删除对应 ingestion 文件引用及对象；同时清理与 trace/observation 关联的孤儿 media。

不会查询或删除名为 `observations` 的 Doris 表；该表已经不存在。`trace_metrics_agg_<p>` 由同步 MV 跟随 `events_full_<p>` 的删除自动维护，无需额外 DELETE。

删除请求可重试：对已不存在的数据再次 DELETE 没有业务副作用。Doris mutation 的实际完成时间仍取决于 Doris 的异步执行状态，因此“命令已提交”不等于查询立即看不到所有旧行。

### 4.2 待删除队列补偿

正常 trace 删除由队列处理。若队列积压，`BatchTraceDeletionCleaner` 会从 PostgreSQL `pending_deletions` 选出积压最多的项目，按批调用同一套 trace 删除逻辑，并在成功后把 pending 记录标为已删除。它不是 TTL 清理器。

### 4.3 项目删除

项目删除队列和批量项目清理器共用同一个幂等流程：

1. 删除项目 split 控制记录，并刷新本进程 split cache；
2. 执行 `DROP TABLE IF EXISTS events_full_<p>`；关联 MV 随 base table 删除；
3. 执行 `DROP TABLE IF EXISTS traces_scalar_<p>`；
4. 清理共享 `scores`、`dataset_run_items_rmt`，以及项目级 blob/S3、PostgreSQL 数据。

该流程不会清理共享 `events_full` 或 `traces_scalar`：它们不是当前业务读取目标，历史 shared telemetry 也不在本次模型中迁移或回读。

## 5. 自动保留（TTL）

`events_full_<p>` 与 `traces_scalar_<p>` 都使用：

```sql
PARTITION BY RANGE(`start_time`)
```

并启用 Doris `dynamic_partition`。有效保留天数会写为：

```text
dynamic_partition.start = -<retentionDays>
dynamic_partition.time_unit = DAY
dynamic_partition.time_zone = Etc/UTC
```

因此，分区何时过期只由 `start_time` 所在日期决定：

- 修改 `created_at` 不会让行更早进入 TTL 删除范围；`created_at` 可以用于审计/聚合，但不是 telemetry TTL 分区键。
- 修改或重新配置项目 retention 后，provisioning 会对两个 base table 执行 `ALTER TABLE ... SET dynamic_partition.start`；已有表不会仅靠 `CREATE TABLE IF NOT EXISTS` 自动更新 TTL。
- dynamic partition 清理由 Doris FE 的 scheduler 在下一次检查时执行，不是应用每小时逐行发 `DELETE`，也不保证在到期瞬间同步删除。
- `retentionDays = null` 表示近似无 TTL（当前实现设为 10 年阈值），不是立即删除。
- `trace_metrics_agg_<p>` 随 `events_full_<p>` 的分区/MV 维护，不单独运行行级 retention cleaner。

应用侧 `BatchDataRetentionCleaner` 不再对 telemetry 表执行行级 DELETE；它当前只对仍共享的 `scores` 按 `timestamp` 进行保留清理。

## 6. 排障与上线核验

当用户反馈“改了 `created_at`，为什么没有自动删除”时，应优先检查 `start_time` 及表的 dynamic partition 属性：

```sql
SHOW CREATE TABLE `events_full_<projectId>`;
SHOW CREATE TABLE `traces_scalar_<projectId>`;
SHOW PARTITIONS FROM `events_full_<projectId>`;
SHOW PARTITIONS FROM `traces_scalar_<projectId>`;
```

重点确认：

1. 两张 base table 是否存在；
2. 分区键是否为 `start_time`；
3. `dynamic_partition.enable` 是否为 `true`；
4. `dynamic_partition.start` 是否等于期望的负保留天数；
5. Doris FE 的 dynamic partition scheduler 是否正常运行；
6. 需要删除的数据的 `start_time` 是否确实落在过期分区内。

如果读取为空，同时日志出现 `Doris split-table read target missing`，说明是项目分表缺失，而不是 shared 数据被过滤。应检查 provisioning 队列、控制表状态和建表/MV 状态，并重试 provisioning。

## 7. 非目标与兼容性边界

- 不迁移、也不读取共享 `events_full` / `traces_scalar` 中的历史 telemetry。
- 不在本次逻辑中 DROP 这两张共享物理表；其物理回收应由单独的 DBA/ops 计划执行。
- `scores`、dataset run items、blob log 仍为共享表，遵循各自的读取和 retention 规则。
- 保留在 repository 中的旧行级过期辅助函数不构成当前 telemetry 自动保留路径；当前自动 telemetry retention 的唯一来源是分表 dynamic partition TTL。

## 代码入口

- 路由与项目 lane：`packages/shared/src/server/doris/tableRouting.ts`
- 跨项目分表 fan-out：`packages/shared/src/server/doris/crossProjectTableRouting.ts`
- 缺表读取降级：`packages/shared/src/server/repositories/doris.ts`
- 分表 DDL、TTL 与 MV：`packages/shared/src/server/doris/splitTableTemplates.ts`、`packages/shared/src/server/doris/provisionSplitTables.ts`
- trace telemetry 删除：`packages/shared/src/server/repositories/traces.ts`、`worker/src/features/traces/processDorisTraceDelete.ts`
- 项目级 Doris 清理：`worker/src/features/doris-project-cleanup/index.ts`
- shared `scores` retention：`worker/src/features/batch-data-retention-cleaner/index.ts`
