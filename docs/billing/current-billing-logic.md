# Litefuse Billing 当前计量计费逻辑

> 本文基于 2026-07-31 的当前实现，说明 Billing 的业务规则、数据流和已知边界，不展开具体代码结构。

## 1. 核心结论

Litefuse 保留两个定时 Job，但它们承担不同职责：

- Metering Job 负责把 Pro 用量上报给 Stripe。
- Threshold Job 负责 Developer 免费额度、预警和 ingestion 阻断。

这两个 Job 不会再共同覆盖同一个用量数字。

Developer 和 Pro 也不再共用同一种用量存储方式：

- Developer 使用定期计算的免费额度快照。
- Pro 使用“已成功提交 Stripe 的用量”加“尚未上报的实时用量”。

因此，Pro 页面主用量始终满足：

**当前用量 = 已上报用量 + 待上报用量。**

## 2. Units 如何计算

Organization 下所有未删除 Project 的用量会合并计算。

计量口径为：

- 每个根事件贡献 1 个 trace unit。
- 每个事件贡献 1 个 observation unit，根事件也包含在内。
- 每个 score 贡献 1 个 score unit。

例如，一个根事件、一个子事件和一个 score 会产生：

- 1 个 trace unit；
- 2 个 observation units；
- 1 个 score unit；
- 合计 4 units。

所有计量都使用服务端创建时间，不使用客户端提供的业务时间。这样可以避免通过回填历史时间把新写入的数据计入旧账期。

## 3. 套餐和额度

| 套餐       |         固定月费 |    每月包含量 | 超出后处理                      |
| ---------- | ---------------: | ------------: | ------------------------------- |
| Developer  |             免费 | 100,000 units | 达到上限后可触发 ingestion 阻断 |
| Pro        |             $199 | 200,000 units | 每个额外 unit 收取 $0.00004     |
| Teams      | Pro 加历史附加项 |      继承 Pro | 仅兼容已有订阅，不开放自助购买  |
| Enterprise |         合同约定 |      合同约定 | 不走自助 Checkout               |

Pro 每额外 100,000 units 收取 $4。

应用始终向 Stripe 上报原始 units，不会先减去 200,000 免费额度。免费层和超额价格由 Stripe 的阶梯价格负责计算。

Billing 页面展示的超额金额只是税前、折扣前估算。最终税费、优惠、发票和应收金额以 Stripe 为准。

## 4. 账期如何计算

### Developer

Developer 默认以 Organization 创建时间作为月度账期锚点。

账期保留完整 UTC 时间，包括时、分、秒和毫秒，不会截断到当天零点。

### Pro

升级 Pro 后，以 Stripe 当前订阅账期开始时间作为月度账期锚点。

计费生命周期还会记录订阅真正开始和结束的精确时间，用来排除升级前和订阅结束后的数据。

例如在 10:37 升级：

- 10:37 之前的数据属于 Developer 阶段；
- 10:37 之后的数据才进入 Pro metering；
- 不会因为 Job 按整小时执行，就把 10:00 到 10:37 的数据计入 Pro。

### 月末处理

如果锚点位于 29、30 或 31 日，较短月份会使用该月最后一天，同时继续保留原始时分秒。

例如 1 月 31 日 10:37 的锚点，在闰年 2 月会落到 2 月 29 日 10:37。

### 账期变化时

账期锚点真正变化后，旧的 Developer 用量快照会被标记为失效。

系统不会写入一个看似刚计算完成的 0。这样可以避免升级后页面在 0 和旧数据之间交替显示。

同一账期的重复 webhook 不会触碰用量快照。

## 5. Developer 用量逻辑

Developer 用量快照只由 Threshold Job 更新。

Threshold Job 每小时第 35 分钟运行，计算 Organization 当前账期内所有有效 Project 的总 units。

状态规则为：

|   当前账期用量 | 状态    |
| -------------: | ------- |
|       0–79,999 | 正常    |
|  80,000–99,999 | WARNING |
| 100,000 及以上 | BLOCKED |

### Shadow 模式

默认测试阶段建议关闭 enforcement。

关闭时：

- 仍然计算和保存用量；
- 不发送预警邮件；
- 不进入 WARNING 或 BLOCKED；
- 不阻断 ingestion。

### Enforcement 模式

开启后：

- 首次进入 WARNING 时通知 Organization OWNER 和 ADMIN。
- 首次进入 BLOCKED 时发送阻断通知。
- 进入或退出 BLOCKED 时刷新 API Key 权限缓存。
- 受控 ingestion 写入返回 403。
- 已有数据读取、Billing 页面、Stripe Portal 和升级入口仍然可用。

### 如何避免升级竞态

Threshold Job 开始计算时会记住 Organization 的版本和账期锚点。

写入结果时会再次确认这两个值没有变化。如果计算期间发生升级、订阅同步或账期切换，旧计算结果会被丢弃。

因此，Developer 的旧用量不会在升级后重新覆盖 Pro 状态，也不会把已经升级的 Organization 再次变为 BLOCKED。

付费 Organization 不会被 Threshold Job 写入用量快照。Job 只会按需清除遗留的免费层阻断状态。

## 6. Pro 用量逻辑

Pro 页面把用量分成两部分。

### Reported to Stripe

这是当前精确账期内已经成功提交给 Stripe API 的用量。

它来自本地提交账本，只统计提交成功且已经位于 metering checkpoint 之前的记录。

“Reported to Stripe”只表示 Stripe API 已经接受，不表示 Stripe Dashboard 已完成异步聚合。Dashboard 可能稍后才显示相同结果。

### Pending

这是 metering checkpoint 之后，到当前时刻之间的 Doris 用量。

如果刚升级且 checkpoint 早于升级时间，Pending 会从升级时间开始计算，不会包含升级前数据。

### Current

页面主数字是 Reported 和 Pending 之和。

预计超额也基于这个主数字计算，因此页面既包含已经上报的用量，也包含尚未等到下一个小时 Job 的新用量。

Developer 页面不显示 Reported 和 Pending，只显示免费层当前累计。

## 7. Metering Job 如何工作

Metering Job 每小时第 5 分钟触发，Worker 启动时也会执行一次追赶检查。

它一次认领一个已经结束的完整小时，并预留 5 分钟等待数据落入 Doris。

### 防止两个 Worker 重复处理

每个小时只有一个 Worker 可以成功认领。

认领使用数据库状态和租约保护。第二个 Worker 如果发现该小时已经被处理，会直接退出，不会重复提交 Stripe。

### 与订阅生命周期求交

Job 会把目标小时与每个 Organization 的订阅有效时间求交。

可能得到以下结果：

- 整个小时都有效，按完整小时统计；
- 只在小时中途开始，排除开始前数据；
- 只在小时中途结束，排除结束后数据；
- 与订阅生命周期完全不相交，不做任何上报。

### 跨月账期拆分

如果月度账期边界落在小时中间，该小时会被拆成两个独立 segment。

每个 segment 都有自己的开始时间、结束时间、本地提交记录和 Stripe 幂等标识，确保前后两部分进入正确账期。

### 零用量

零用量 segment 不创建 Stripe meter event，但不会阻止小时 checkpoint 继续推进。

### 失败和重试

每次 Stripe API 调用会自动重试。整个 Job 也有 BullMQ 重试。

只有该小时所有 Organization、所有 segment 都成功后，全局 checkpoint 才会推进。

如果某些 segment 已成功，而后续 segment 失败，下一次重试会跳过已提交部分，不会重复计费。

如果 Worker 落后多个小时，会按小时依次追赶，而不是把多个小时合成一个大区间。

## 8. Webhook 和订阅同步

Billing 会处理 Checkout、订阅创建、订阅更新、订阅删除和发票状态事件。

### Event 幂等

每个 Stripe event 都会记录处理状态：

- 已处理事件再次到达时直接视为重复事件。
- 正在处理的事件有短期租约，防止并发重复执行。
- 处理失败或租约超时后可以重新认领。

### 使用 Stripe 当前状态

收到 webhook 后，系统优先重新读取 Stripe 当前订阅，而不是完全相信事件中可能已经过期的订阅快照。

Checkout、subscription 和 invoice 等不同事件即使乱序到达，最终也会尽量同步 Stripe 的最新状态。

### Organization 行锁

订阅同步会锁定目标 Organization，并在锁内重新读取当前状态后再更新。

因此多个不同 event ID 同时到达时，不会都基于同一份旧数据互相覆盖。

### 旧删除事件保护

删除事件只会清理与当前 Organization 订阅 ID 相同的订阅。

如果 Organization 已经切换到新订阅，旧订阅延迟到达的删除事件会被忽略，不会清掉新订阅。

### 区域隔离

Stripe Customer、Checkout 和 Subscription 都携带 Organization 和 Cloud Region 信息。

如果订阅所属 Region 与当前部署不一致，Webhook 不会更新本地 Organization，避免跨区域串单。

## 9. 订阅生命周期

### Developer 升级 Pro

升级会创建包含 Pro 固定月费和 Usage Price 的 Stripe Checkout。

付款完成后，由 webhook 确认最新订阅状态并把 Organization 切换为 Pro。

升级发生的精确时间同时成为 Pro metering 的起点。

### Pro 再次选择 Pro

不会创建新的订阅，也不会创建重复的套餐变更。

### 历史 Teams 切换 Pro

当前账期继续保留 Teams 权益，在下个账期开始时移除 Teams 附加项。

### 账期末取消

取消不会立即降级。当前账期内继续保留 Pro 权益，到期后切回 Developer。

### 恢复订阅

在取消真正生效前可以恢复订阅，清除账期末取消标记。

### 保持当前套餐

如果存在待生效套餐变更，可以取消该计划并继续当前套餐。

### 付款异常

Active、Trialing 和 Past Due 状态仍保留付费权益。

Unpaid、Canceled、Incomplete Expired 等终止状态会清除付费套餐并回到 Developer。

Past Due 期间页面会提醒更新付款方式，但不会立即失去 Pro 权益。

## 10. Billing 页面展示

Billing 页面每 60 秒自动刷新。窗口重新获得焦点、标签页重新可见或从 Stripe Portal 返回时也会刷新。

### Developer 页面

页面优先使用 5 分钟内的免费额度快照。

快照过期后，页面可以直接查询 Doris 得到较新的估算值，但页面查询不会把结果写回 Organization。

因此页面查看本身不会与 Threshold Job 争抢快照写入权。

### Pro 页面

页面每次根据当前账期的已提交账本和 Doris Pending 计算：

- 当前总用量；
- Reported to Stripe；
- Pending；
- 已包含额度；
- 超额 units；
- 预计超额金额；
- 重置日期。

如果 Stripe Dashboard 尚未完成异步聚合，页面 Reported 可能暂时比 Dashboard 先更新。

## 11. 删除数据后的影响

删除对 Developer、Pro Pending 和 Pro Reported 的影响不同。

| 删除发生的位置            | Developer          | Pro Pending        | Pro Reported / Stripe                            |
| ------------------------- | ------------------ | ------------------ | ------------------------------------------------ |
| 上报前删除 trace 或 score | 下次计算后下降     | 下降               | 最终不会上报被删除部分                           |
| 上报后删除 trace 或 score | 下次计算后下降     | 不涉及已提交区间   | 已提交用量不自动冲销                             |
| 删除 Project              | 立即从后续计算排除 | 不再包含该 Project | 历史已提交用量保持不变                           |
| Retention 清理            | 下次计算后下降     | 只影响尚未提交部分 | 历史已提交用量保持不变                           |
| 删除 Organization         | 页面不再可用       | 页面不再可用       | 历史 Customer、meter event 和 invoice 不自动删除 |

### 删除发生在上报前

Metering 和 Pending 都查询 Doris 当前数据。

如果数据已经删除，或者 Project 已被软删除，该部分不会进入最终 Stripe 上报。

### 删除发生在上报后

已经成功提交的用量不会自动产生负数调整、退款或 credit note。

正常情况下，checkpoint 只向前推进，因此系统也不会重新计算已经提交的 segment。

### Developer 解除 BLOCKED

删除数据后，Developer 页面可能先显示较低用量，但 BLOCKED 状态要等下一次 Threshold Job 才会更新。

因此可能短暂出现页面低于 100,000 units，但 ingestion 仍返回 403 的情况。

## 12. Project Transfer 的影响

Project Transfer 没有专门的账务拆分。

- Developer 会在下次计算时，按 Project 当前所属 Organization 和新 Organization 的账期重新归属历史数据。
- Pro 尚未上报的 Pending 会按 Project 当前所属 Organization 计算。
- 已经提交 Stripe 的用量仍保留在原 Organization 对应的 Stripe Customer，不会随 Project 迁移。
- 转移操作不会立即刷新来源和目标 Organization 的 Developer 快照。

## 13. Data Retention 与数据访问

Pro 的 3 年数据访问窗口和 Project Data Retention 是两件不同的事。

- 数据访问窗口决定用户最多可以查询多早的数据。
- Data Retention 决定系统是否主动物理删除超过 Project 保留期限的数据。

每个 Project 可以设置独立的保留天数。到期后，Worker 会异步清理事件、score、媒体和启用 blob log 时的 ingestion 文件。

Retention 删除发生在 Stripe 上报前时，会减少最终计费；发生在上报后时，不会自动冲销已提交用量。

## 14. 当前边界和运维注意事项

### 删除不冲销 Stripe

已成功提交的 meter event 不会因为后续删除数据而回退。

### 不要人工回退已提交 checkpoint

正常流程只向前推进 metering checkpoint。

如果人工把 checkpoint 回退到已经提交的区间，Worker 可能按当前 Doris 数据更新本地提交记录，但因为该记录已标记成功，不会重新发送 Stripe。

如果区间数据已经删除，本地 Reported 和 Stripe 已接受值可能因此不一致。

### 全局 checkpoint

一个 Organization 的 Stripe 提交失败，会阻止整个小时的 checkpoint 推进。其他 Organization 已成功的 segment 不会重复计费，但仍要等待失败部分恢复。

### Stripe Dashboard 有聚合延迟

页面 Reported 表示 Stripe API 已接受，不保证 Stripe Dashboard 已经展示相同 summary。

### Project Transfer 不迁移历史账单

已上报 Stripe 的用量不会随 Project 转移到新 Organization。

### Developer 阻断不是全系统只读锁

当前主要 ingestion、OTel traces、v1 score 和 media 写入已接入阻断。

部分 v2 score 和 MCP 写操作尚未统一接入，应继续作为 QA 覆盖缺口。

### Stripe 配置依赖外部正确性

应用可以确认 Price ID 格式，但不会替代 QA 检查 Stripe 中的固定月费、阶梯价格、免费额度和 meter 配置。

### 正式历史数据需要兼容切换

当前精确时间和 segment 口径默认用于 billing/shadow 测试阶段。

如果已经存在正式客户历史 meter events，部署前需要按 Organization 和账期设计切换方案，不能直接改变已提交历史区间的计量口径。

## 15. 最小验证场景

### 升级

1. 创建新的 Developer Organization。
2. 在升级前写入一些数据并确认 Developer 页面有用量。
3. 在非整点时间升级 Pro。
4. 确认 Pro 新账期不包含升级前数据。
5. 确认页面不在 0 和旧数据之间交替。

### Pending

升级后写入一个根事件、一个子事件和一个 score。

页面应显示 4 个 Pending units，Reported 为 0，主用量为 4。

### Reported

等待目标小时结束并完成 Metering Job。

页面应变为 Reported 4、Pending 0，主用量仍为 4。

Stripe meter summary 最终也应增加 4 units。

### 幂等重放

同一个已处理区间正常重试时，不应创建重复 Stripe meter event，Stripe summary 仍为 4。

### 升级竞态

让 Threshold Job 在 Developer 状态下开始计算，然后在写入前完成升级。

旧 Developer 结果应被丢弃，Organization 不得重新进入 BLOCKED。

### 旧删除事件

先切换到新订阅，再投递旧订阅删除事件。

Organization 应继续保留新订阅和 Pro 权益。
