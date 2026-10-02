# 夜间即时零售冷链履约系统

在基础经营模块（主体、站点、操作者、幂等回执、SQLite 事务、哈希审计链）之上，提供网络热梗带动夜间订单高峰时的即时零售履约能力：

- **带有效期的可解释承诺**：逐门店按销售区域 → 品牌渠道规则 → 冷链状态 → 保质窗口（临期阈值）→ 批次渠道标签 → 现货余量 → 配送运力的顺序试算；报价带 `expires_at`，每个被排除批次返回机器可读理由码（`cold_chain_broken`、`near_expiry`、`channel_not_allowed`、`brand_channel_blocked` 等），缺货时只推荐规则允许的替代规格。
- **确认时原子占用**：单条 `BEGIN IMMEDIATE` 事务内以条件 UPDATE 占用批次（`held ≤ on_hand - picked - sold`）并按配送员实时容量拆单装箱，库存或运力任一不足整单回滚，高峰并发绝不超卖。
- **取货保护与超时释放**：`held → picked → partial/delivered/cancelled` 状态机；取货把占位转为取货量，周期回收只释放仍为 `held` 的到期单及批次失配单（门店更新为失温/停用），已取货订单不受影响。
- **数量与金额一致**：每单恒满足 `承诺 = 已送达 + 已退款 + 未决`，金额按批次单价同步核算；取消整单解除、部分送达按未送达件数退款、拆单逐腿结清。
- **幂等与冲突**：所有写接口要求 `request_id`；平台重试回放首次结果（迟到的送达回执也安全），同一 `request_id` 不同载荷返回 409。回执与业务写入在同一事务落库。
- **重启恢复**：全部状态在 SQLite（WAL）中；进程启动先执行一次回收，待履约订单原样恢复。

## 目录

- `src/beverage_ops_foundation/fulfillment.py`：履约领域服务（报价引擎、占用、状态机、结算、回收）；
- `src/beverage_ops_foundation/maintenance.py`：启动恢复与周期超时回收守护线程；
- `src/beverage_ops_foundation/storage.py`：SQLite schema（批次、规则、运力、报价、订单、配送腿）；
- `src/beverage_ops_foundation/api.py`：HTTP/JSON 边界；
- `src/beverage_ops_foundation/acceptance.py`：离线端到端验收；
- `tests/`：基础规则、履约规则、HTTP 路由、高峰并发与重启恢复测试。

## 环境

- Linux / Python 3.11+，仅使用标准库与 SQLite。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m beverage_ops_foundation.acceptance
```

覆盖：冷链/临期批次的解释排除、规则内替代规格、原子占用与骑手拆单、请求号冲突、超时释放与已取货保护、部分送达金额精确、迟到回执幂等回放、进程重启后待履约恢复与审计链连续。成功时输出 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m beverage_ops_foundation.api --database beverage.sqlite3 --host 127.0.0.1 --port 8080
```

写接口通过 `X-Actor-Id` 标识操作者并要求 `request_id`；重试返回 200，首次创建返回 201，载荷冲突返回 409。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/channels` `/products` `/stores` `/couriers` | 渠道、商品（含冷链标记）、门店（含销售区域）、运力（含容量） |
| POST | `/batches` | 门店现货批次 upsert（冷链、到期日、临期阈值、渠道标签、乐观版本） |
| POST | `/brand-channel-rules` `/brand-region-rules` | 品牌 × 渠道 / 品牌 × 区域准入 |
| POST | `/substitution-rules` | 缺货替代规格（按渠道配置、带优先级） |
| POST | `/quotes` | 可解释报价（不占库，带 TTL） |
| POST | `/orders/confirm` `/orders/cancel` | 原子占库占运力 / 取货前取消 |
| POST | `/deliveries/pick` `/deliveries/complete` | 取货 / 送达回执（支持部分送达） |
| POST | `/reconcile` | 手动触发超时与失配回收 |
| GET | `/orders/{id}` | 订单全貌（行、配送腿、数量/金额一致性字段） |
| GET | `/batches?store_id=` | 批次占用快照（on_hand/held/picked/sold/available） |
