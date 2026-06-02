# Master 分支 events_full 迁移方案

> **目标**：master 上把 trace/observation 落到 `events_full` 单表；写入只接受 OTel ingestion（Python v4 / JS v5）；保留 MinIO、Redis、BullMQ。
>
> **原则**：
> - **SQL 复用**：能直接 copy lightweight 的 SQL（schema / view DDL / 读 CTE / 聚合表达式）就直接 copy。TS 代码按 master 现有风格写或修。
> - **代码保留**：legacy 路径上的函数、辅助代码、API endpoint 文件全部保留；写入路径靠**入口闸门**让 legacy 处理函数自然变成 dead code path。
> - **开发模式**：feature 分支开发 + 1-2 周验证 → 一次性合 master。无运行时 flag 灰度。

---

## 0. master 现状（关键事实）

**master 是 langfuse-main v4 transition 中期 snapshot**——TS 基础设施全部就位但 stub 状态，物理表未建。

| 设施 | 状态 |
|---|---|
| `EventRecordInsertType` schema (`definitions.ts:654-772`) | ✅ 已存在；带 V3 transition 残留字段（`metadata` Map / `metadata_hashes` / `metadata_long_values` / `metadata_raw_values`） |
| `IngestionService.createEventRecord` (`IngestionService.ts:277`) | ✅ 完整实现，构造 `EventRecordInsertType` |
| `IngestionService.writeEventRecord` (`IngestionService.ts:455-461`) | ⚠️ **stub no-op**："Events table is not supported in Doris, skipping" |
| `worker/src/queues/eventPropagationQueue.ts` | ✅ BullMQ Processor 已存在 |
| `worker/src/features/eventPropagation/handleEventPropagationJob.ts` | ⚠️ 893 行，目标是 `observations_batch_staging → events` 中转（**两个表都没建**，且这套设计 langfuse-main 已抛弃）|
| `worker/src/features/eventPropagation/handleExperimentBackfill.ts` | ✅ 已存在；SQL 已是 Doris 形态，读 `dataset_run_items_rmt` + `traces`，写 `events`（不存在的表）via `writeEventRecord` stub |
| `worker/src/queues/otelIngestionQueue.ts:441-479` | ✅ 已经在调 `createEventRecord` + `writeEventRecord`（被 stub 吞掉），由 `LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE` env flag 控制 |
| `worker/src/features/batch-data-retention-cleaner` / `batch-project-cleaner` | ✅ 表列表引用 `events_full`, `events_core`, `events`（后两个都不会建） |
| 物理表 `events` | ❌ 不存在（master 跟着的是 langfuse-main 中间阶段 events 表名，后已 rename 为 events_full） |
| 物理表 `observations_batch_staging` | ❌ 不存在（langfuse-main 已废弃此中转表）|
| 物理表 `events_core` | ❌ 不存在（lightweight 也不引入，用 view 替代） |
| 物理表 `events_full` | ✅ 本次 PR 已加 |
| Read 路径 (`repositories/observations.ts` / `events.ts` 等) | 仍 `FROM observation_source o` / `FROM traces` |

### master 跟着的中间阶段 vs langfuse-main 最新（lightweight）

| 项 | master 半成品形态 | langfuse-main 最新 / lightweight |
|---|---|---|
| 主写入表 | `events` | `events_full` |
| 中转表 | `observations_batch_staging` | 无（直接写 events_full） |
| 元数据字段名 | `metadata_raw_values` | `metadata_values` |
| 元数据 Map 字段 | 有 `metadata` Map | 已删（只剩并行数组）|
| 元数据残留字段 | `metadata_hashes` / `metadata_long_values` | 已删 |
| prompt_version 类型 | string | int |

本次 PR **跳过 master 当前中间阶段，直接对齐 lightweight + langfuse-main 当前形态**。

---

## 1. 决策（已锁定）

| ID | 主题 | 结论 |
|---|---|---|
| **A** | observation 行 denormalize trace 字段 | **Denormalize**。每条 OTel span 一行，trace-level 字段（trace_name / user_id / session_id / tags / release / bookmarked / public / environment）inline 到每行；不写合成 `t-<trace_id>` 行。与 langfuse-main 对齐。 |
| **B** | Stream Load 头与写入语义 | 完整行写入 + Doris UNIQUE KEY MoW（load-order 决定胜负）。**不用** `group_commit`（DorisWriter 已攒批）/ `partial_columns`（主写路径） / `sequence_col`。 |
| **C** | SDK 版本闸门 | 硬拒。trace/observation 入口检查 Python ≥ 4.0.0 / JS ≥ 5.0.0 / `x-langfuse-ingestion-version=4`，不满足 → 400。 |
| **D** | 写入路径 OTel-only | trace/observation 只能从 `/api/public/otel/v1/traces` 进。其他 endpoint 的 trace/observation 事件入口 400。worker 端 `processTraceEventList` / `processObservationEventList` 因入口闸门**永不被触发**，自然成 dead code path（保留不删）。 |
| **E** | legacy 表 / legacy 函数 | 代码全部保留。`traces` / `observation_source` 物理表保留（不 DROP）；legacy 处理函数保留为 dead code path。 |
| **F** | legacy public API endpoints 收口 | 见下表。 |
| **G** | `EventRecordInsertType` schema 字段对齐 | 删 V3 残留字段（`metadata` Map / `metadata_hashes` / `metadata_long_values`），rename `metadata_raw_values` → `metadata_values`，`prompt_version` 类型 string → int。这不是删除代码，是修正字段名以匹配事实上的 events_full 表 schema。 |
| **H** | trace 读侧聚合策略 | 抄 lightweight `traces.ts:buildTraceAggregationQuery` 两 CTE 模式（`trace_scalars` 用 `MAX_BY(IF(cond, val, NULL), event_ts)` 等价上游 `argMaxIf`；`trace_root` 用 `ROW_NUMBER() OVER (...) WHERE rn=1 AND parent_span_id=''` 取 Array / Variant 字段）。 |
| **I** | experiment_* 列处理 | **路径 1（ingestion-time inline）**：`createEventRecord` 把 SDK 上传的 12 个 experiment 字段 inline 到 events_full 行。本次 PR 激活 writeEventRecord 后真正落库——`experiment.run()` 主动跑实验场景**功能完整**。**路径 2（UI 创建 dataset_run → 异步 backfill）**：master 上 `handleExperimentBackfill.ts` + `EventPropagationQueue` 一直存在但因 writeEventRecord 是 stub 而从未跑通。本次 PR **激活 + 修 SQL**——getRelevantTraces/Observations 读 events_full（根/非根 span），prefiltered_events CTE 也指向 events_full（按 `experiment_id != ''` 反向筛已 enrich 的 trace）。后端到端可用。 |
| **J** | `handleEventPropagationJob.ts` | 整段 retire。它的目标设计（`observations_batch_staging → events` 中转）已被 langfuse-main 抛弃；改成 early return + `logger.info("deprecated, see events_full direct write")`。代码保留。 |

### 决策 F：endpoint 收口表

| Endpoint | trace/observation 事件 | score 事件 | dataset_run_item 事件 |
|---|---|---|---|
| `/api/public/otel/v1/traces` | ✅ 唯一入口（SDK 闸门生效） | — | — |
| `/api/public/ingestion` | ❌ 400 | ✅ 白名单允许 | ✅ 白名单允许 |
| `/api/public/spans` / `generations` / `events` | ❌ 400（文件 / handler 保留） | — | — |
| `/api/public/scores` / `/api/public/dataset-run-items` | — | ✅ 保留 | ✅ 保留 |

---

## 2. 实施清单

### 2.1 ✅ Schema migration（已 commit）

- ✅ `0037_create_events_full.up/down.sql` （从 lightweight 直接 copy）
- ✅ `0039_create_events_full_view.up/down.sql`
- ✅ `0040_create_events_full_trace_view.up/down.sql`

### 2.2 ✅ Utils（已 commit）

- ✅ `packages/shared/src/server/utils/dorisArrays.ts` 补全 `parseDorisStringArrayKeepEmpty` / `zipDorisMetadataArrays`（从 lightweight 直接 copy）
- ✅ 测试同步

### 2.3 修 `EventRecordInsertType` schema（决策 G）

- [ ] `packages/shared/src/server/repositories/definitions.ts`：
  - `eventRecordBaseSchema`：删 `metadata: z.record(...)` 行；`metadata_values: z.array(z.string().nullish()).default([])` 加入 base；`prompt_version: z.number().int().nullish()`
  - `eventRecordReadSchema`：删 `metadata_values` / `metadata_hashes` / `metadata_long_values` 三行（前者移到 base，后两个 V3 残留删）
  - `eventRecordInsertSchema`：rename / 删 `metadata_raw_values` 行（已在 base 里），加 `total_cost: z.number().nullish()`
- [ ] 验证全仓没有其他文件再引用这些 V3 残留字段名（grep `metadata_raw_values` / `metadata_hashes` / `metadata_long_values`）；如果有，按相同思路 rename

### 2.4 DorisWriter 加 `EventsFull`

- [ ] `worker/src/services/DorisWriter/index.ts`：
  - `TableName` 枚举加 `EventsFull = "events_full"`
  - `RecordInsertType<T>` 条件类型加 `T extends TableName.EventsFull ? EventRecordInsertType : ...`
  - `flushAll` 加 `this.flush(TableName.EventsFull, fullQueue)` 分支（共 7 张表）
  - 其他 flush 分支（Traces / Observations）保留——dead 但代码保留

### 2.5 激活 `writeEventRecord`

- [ ] `worker/src/services/IngestionService/index.ts:455-461`：
  - 把 stub 改为真写：`this.dorisWriter?.addToQueue(TableName.EventsFull, eventRecord)`
  - 删 "skipping" 日志

### 2.6 修 `createEventRecord` 内部字段名

- [ ] `worker/src/services/IngestionService/index.ts:277-450`：
  - 删 `metadata: convertRecordValuesToString(eventData.metadata ?? {})` 那行（V3 Map 字段已不在 schema）
  - rename `metadata_raw_values: metadataValues` → `metadata_values: metadataValues`
  - 其他字段不动

### 2.7 `handleEventPropagationJob.ts` retire（决策 J）

- [ ] `worker/src/features/eventPropagation/handleEventPropagationJob.ts`：
  - `handleEventPropagationJob` 函数体改为 `logger.info("[deprecated] eventPropagationJob is now a no-op; events_full is written directly via OTel ingestion"); return;`
  - 其余 893 行**整体保留为参考**（注释包裹也行，或者纯保留函数体在 if (false) 内）
  - `eventPropagationQueueProcessor` 仍然调它（无害）

### 2.8 ~~修 `handleExperimentBackfill.ts` SQL~~（不做）

master 上这套 backfill 设施（队列 + 893 行 handler + Doris SQL）齐全但**功能从未跑通**——writeEventRecord 是 stub no-op。本次 PR 激活 writeEventRecord 后理论上 write 端通了，但 read 端 SQL 仍指向 `traces` 表（已经无新数据），整体仍然不工作。

**与 master 现状一致：不激活，留独立 PR 处理**。

代码状态：`handleExperimentBackfill.ts` 文件保留不删；`eventPropagationProcessor` 仍调用它（runExperimentBackfill），但因数据源 traces 表空（OTel-only 之后不再写）跑出空结果。无害。

如果未来要激活路径 2，独立 PR 范围：
- SQL 改读 events_full（root span 行）替代 traces JOIN
- 字段名对齐（`metadata_raw_values` → `metadata_values` — 实际上 schema 已经修了，需要更新 backfill 内部对应字段）
- 端到端测试 UI 创建 dataset run 后 events_full 实际接收 enrichment

### 2.9 Read 路径切 events_full

**SQL 字符串直接抄 lightweight**，TS 控制流按 master 现有形态。

| 类别 | 文件 |
|---|---|
| 核心读 | `repositories/traces.ts` (`getTraceById` + `buildTraceAggregationQuery`)、`observations.ts` (`getObservationsForTrace` / `getObservationByIdInternal`) |
| 列表 / 计数 / 聚合 | `traces.ts` (`getTracesTable` + count + metrics)、`observations.ts` (list / count / grouped reads) |
| sessions | `services/sessions-ui-table-service.ts` / `sessions-ui-table-events-service.ts` |
| 过滤 / dropdown | trace filter dropdowns + `getTracesByIds` |
| 其余 queries | `events.ts`、剩余 `traces.ts` / `observations.ts` |
| query builder | `dataModelDoris` |
| dashboards | `dashboards.ts` |
| dataset run items | `dataset-run-items.ts` |
| public API | `web/src/pages/api/public/` 下涉及 trace/observation 的所有文件 |
| logging page | UI 端默认指向 events_full |

实施方式：每个文件 `diff <(git show litefuse-lightweight:<path>) <(git show master:<path>)`，把 lightweight 的 SQL 块整段贴到 master，TS 控制流保留 master 现有形态。列别名约定：`trace_id AS id`、`start_time AS timestamp`、`span_id AS id`、`parent_span_id AS parent_observation_id`、`model_id AS internal_model_id`。

### 2.10 入口收口（决策 C + F）

- [ ] `web/src/pages/api/public/otel/v1/traces/index.ts`：加 SDK 版本检查（端口 lightweight `directWriteHelpers.ts:checkHeaderBasedDirectWrite`），不满足 → 400
- [ ] `web/src/pages/api/public/ingestion.ts`：加 `ALLOWED_EVENT_TYPES = {SCORE_CREATE, SDK_LOG}` 白名单（参考 lightweight `ingestion.ts:36-66`），整批含非白名单事件 → 400
- [ ] `web/src/pages/api/public/spans.ts` / `generations.ts` / `events.ts`：入口直接 400 + 升级提示；文件 / handler 保留不删
- [ ] `/api/public/scores` / `/api/public/dataset-run-items` 不动

### 2.11 batch cleaner 表列表对齐

- [ ] `worker/src/features/batch-data-retention-cleaner/index.ts` 和 `batch-project-cleaner/index.ts`：
  - `BATCH_DATA_RETENTION_TABLES` 列表里删 `events`、`events_core`（这两个表不会建），保留 `events_full`
  - `traces` / `observations` 保留（legacy 表保留）

### 2.12 Feature 分支验证

- [ ] dev 环境 v4 Python SDK + v5 JS SDK 上行 → events_full 有数据，UI 所有页面正常
- [ ] v3 SDK trace/observation 上传 → 400，score / dataset_run_item 不受影响
- [ ] e2e 测试通过
- [ ] burn-in（时长 + 指标见 §5 TBD）
- [ ] 合 master 前 `git fetch && git ls-tree origin/master -- 'packages/shared/doris/migrations/'` 重新确认 0037/0039/0040 编号未被抢占

---

## 3. 代码搬运清单

### 3.1 SQL 直接抄 lightweight

| lightweight 来源 | master 目标 | 状态 |
|---|---|---|
| `packages/shared/doris/migrations/0037_create_events_full.up/down.sql` | 同位置 | ✅ |
| `packages/shared/doris/migrations/0039_create_events_full_view.up/down.sql` | 同位置 | ✅ |
| `packages/shared/doris/migrations/0040_create_events_full_trace_view.up/down.sql` | 同位置 | ✅ |
| `traces.ts:buildTraceAggregationQuery` 函数体里的 CTE SQL 字符串 | 同位置 | 待做 |
| `traces.ts` / `observations.ts` / `dashboards.ts` / `sessions-*.ts` / `events.ts` / `dataModelDoris` 中的 events_full SQL 块 | 对应位置 | 待做 |
| `utils/dorisArrays.ts` (`zipDorisMetadataArrays`, `parseDorisStringArrayKeepEmpty`) | 同位置 | ✅ |

### 3.2 TS 代码不搬运、修 master 已有

| master 已有 | 改动方式 |
|---|---|
| `IngestionService.createEventRecord` | 修字段名（`metadata_raw_values` → `metadata_values`、删 V3 Map） |
| `IngestionService.writeEventRecord` | stub → 真写 `addToQueue(TableName.EventsFull, ...)` |
| `definitions.ts:eventRecord*Schema` | 删 V3 残留字段、字段名对齐、字段类型对齐 |
| `worker/src/services/DorisWriter/index.ts` | 加 EventsFull 枚举、type map、flush 分支 |
| `handleExperimentBackfill.ts` SQL | 从 traces → events_full、字段名对齐 |
| `handleEventPropagationJob.ts` | 整段 deprecated early return，函数体保留 |
| `batch-data-retention-cleaner` / `batch-project-cleaner` 表列表 | 删 events / events_core |

### 3.3 明确不要搬 / 不要建

- ❌ `events` 物理表（master 中间阶段方向，langfuse-main 已废）
- ❌ `observations_batch_staging` 物理表（同上）
- ❌ `events_core` 物理表（lightweight + master 都不用，view 替代）
- ❌ lightweight 的 `buildEventFullFromTrace` / `buildEventFullFromObservation` / `buildSyntheticTraceSpanForOrphan` 这套 mergeAndWrite 路径函数（master 已有 `createEventRecord` 等价）
- ❌ lightweight 的 OTel 直写路径（`processSpansSync` / `RequestWriteBuffer`）—— master 保留 BullMQ 异步
- ❌ lightweight 的 pg-boss / app_cache / 去 Redis / 去 BullMQ 改造

---

## 4. 不做的事

- ❌ OTel 直写 Doris（不引入 `RequestWriteBuffer` 到 web）
- ❌ 剥离 MinIO / Redis / BullMQ
- ❌ 把 IngestionService 搬到 shared
- ❌ 引入 `group_commit` / `partial_columns`（主写路径）/ `sequence_col`
- ❌ 引入新的跨表 pre-read 函数
- ❌ 物理 DROP legacy `traces` / `observation_source` 表
- ❌ 物理 DROP master 已存在的 `events` / `observations_batch_staging` 等 placeholder migration（这些表本来就没建）
- ❌ 删除 `processTraceEventList` / `processObservationEventList` 函数（保留作为 dead code path，入口闸门保证不会被触发）

---

## 5. 待定

- **Burn-in 时长 + "稳定"判定指标**：实施前再定。

---

## 6. Out-of-scope（独立 PR）

### 6.1 历史数据迁移（traces / observation_source → events_full）

本次 PR 不处理。合 PR 后 events_full 只有合并之后的新数据，PR 合并前的历史 trace / observation 在新 read path 下不可见。

参考：`~/work/langfuse-main/worker/src/backgroundMigrations/backfillEventsHistoric.ts` + `backfillEventsHistoricFromParts.ts`。

**合 master 前必须先完成本条**，否则 hard regression。

### 6.2 ~~experiment_* 异步 backfill 功能~~（**已实现**，留作引用）

> 状态更新：本次 PR 已激活该路径。保留小节作为决策历史。

`handleExperimentBackfill.ts` 现在端到端可用：
- `getRelevantTraces`：读 events_full 根 span（`parent_span_id = ''`），trace-level 字段直接从 denormalized 列拿
- `getRelevantObservations`：读 events_full 非根 span（`parent_span_id != ''`），列名对齐（id → span_id、internal_model_id → model_id、metadata Map → metadata_names/metadata_values 在 TS 侧 zip）
- `prefiltered_events` CTE：从 events_full 找已被 enrich 过的 trace（`experiment_id != ''`），LEFT ANTI JOIN 排除——避免重复回填
- 写入路径：`IngestionService.writeEventRecord` 已激活（§2.5），enriched span 直接落 events_full；Doris UNIQUE KEY MoW 保证 replay idempotent

路径 1（SDK `experiment.run()`）和路径 2（UI 创建 dataset run 后异步回填）都端到端工作。

---

## 7. 已排查

### Migration 编号占用

master 当前最大编号 `0036_create_observations_view`。0037 / 0039 / 0040 在 master 当前空闲（0038 跳号，lightweight 历史 squash）。占用这些编号的分支都是 lightweight 体系下游 PR，不会向 master 合。

### DRI 写入链路完整

`/api/public/dataset-run-items.ts:125` → `processEventBatch` → BullMQ → worker `IngestionService.ts:559` → `addToQueue(TableName.DatasetRunItems, record)` → `dataset_run_items_rmt`。**`handleExperimentBackfill` 的数据源可用**。

### master 现有 events_full 基础设施

master 已经从 langfuse-main 拉过来一套 v4 transition 中期 snapshot 的代码：`createEventRecord` 完整、`writeEventRecord` 是 stub、`EventPropagationQueue` + `handleExperimentBackfill` 已存在但 SQL 指向不存在的表（`events` / `observations_batch_staging`）。本次 PR 是**激活 + 修正**这套基础设施到 langfuse-main 当前的 events_full 形态，**不是全新建造**。
