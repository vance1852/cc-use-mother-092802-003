# 编排夜间即时零售的冷链履约基础服务

本项目提供酒类生产、品牌和渠道团队共享的后台基础能力，负责经营主体、生产经营站点、操作者与结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务和哈希串联审计。领域项目可以在这些稳定边界上增加独立的业务状态、规则与接口。

## 目录

- `src/beverage_ops_foundation/`：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
- `tests/`：基础规则、事务边界、接口路由和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m beverage_ops_foundation.acceptance
```

验收命令会在临时 SQLite 数据库中登记经营主体、操作者、站点和参考资料，核对幂等回执与审计链，成功时输出一行 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m beverage_ops_foundation.api --database beverage_ops.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查使用 `GET /health`。写入接口通过 `X-Actor-Id` 标识操作者，服务重启后 SQLite 中的业务状态和审计链继续保留。

## 夜间即时零售履约

在基础模块之上实现了面向高端啤酒夜间高峰的即时零售履约引擎
（`beverage_ops_foundation.fulfillment`），仅依赖标准库与 SQLite：

- **可解释承诺（quote）**：综合门店现货批次、冷藏状态、保质窗口、配送员班次容量、
  销售区域和品牌渠道规则；按到期日最近优先（FEFO）选择批次，排除未冷藏批次与
  临期批次。报价只给出带有效期（默认 120 秒）和送达窗口的承诺，**不占用任何资源**。
  缺货时只在 `allowed_substitutes` 中推荐品牌渠道规则允许的替代规格；默认不擅自改配，
  调用方显式 `allow_substitute=true` 才接受替代并在每行返回改配原因与批次明细。
- **原子确认（confirm）**：在单个 `BEGIN IMMEDIATE` 事务中二次校验并原子占用库存批次
  与配送容量；报价后库存/价格/运力发生变化会拒绝确认，绝不超卖。
- **取货（pick）**：门店拣货、配送员取货后，库存冻结转为物理出库、运力转为已使用，
  此后**超时释放不再影响该订单**。
- **配送回执（delivery-receipt）**：支持迟到回执；短少数量自动转为部分送达，
  短少库存退回门店并按行退款，保证 `已付金额 = 已交付金额 + 退款金额`。
- **取消（cancel）/ 拆单（split）**：未取货可取消并全额退款释放；拆单保持数量与
  金额守恒，子单共享同一运力占用。
- **幂等与冲突**：报价、确认、取货、回执、取消、拆单均以请求号幂等；平台重试返回
  首次结果（`replayed=true`），同一请求号携带不同动作或不同内容一律 `409`。
- **超时扫描与恢复**：`expire_stale_promises` 释放过期且未取货的占用，绝不触碰已取货
  订单；`pending_promises` 用于进程重启后恢复待履约订单；`reconcile` 校验占用、
  数量、金额与拆单守恒。

金额使用整数毫（`*_milli`，1 元 = 1000 毫）避免浮点误差；所有订单状态迁移写入
`order_events`，关键履约动作同时追加到哈希审计链。

### 履约 HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/fulfillment/quotes` | 报价（不占用），返回有效期与批次级解释 |
| POST | `/fulfillment/confirms` | 原子占用库存与运力，生成订单 |
| POST | `/fulfillment/picks` | 门店拣货/配送员取货 |
| POST | `/fulfillment/delivery-receipts` | 送达回执，支持短少部分送达与退款 |
| POST | `/fulfillment/cancellations` | 未取货取消并全额退款 |
| POST | `/fulfillment/splits` | 按行号拆单，数量金额守恒 |
| POST | `/fulfillment/sweep-expirations` | 主动扫描并释放超时未取货占用 |
| GET | `/fulfillment/pending` | 待履约订单（重启恢复） |
| GET | `/fulfillment/reconcile` | 占用/数量/金额对账 |
| GET | `/fulfillment/promises/{id}` | 订单快照（含批次占用明细） |
| POST | `/fulfillment/admin/{skus,channel-rules,substitutions,stores,batches,couriers,shifts}` | 主数据与门店库存更新 |

门店批次更新带 `data_version`；当更新后现货量小于该批次在途冻结量时拒绝覆盖，
防止把已承诺库存改掉。HTTP 服务内置后台超时扫描线程（`--sweep-interval`）。

### 履约离线验收

```bash
PYTHONPATH=src python3 -m beverage_ops_foundation.acceptance_fulfillment
```

覆盖高峰并发不超卖、迟到配送回执与部分送达退款、门店数据更新约束，以及进程重启后的
待履约恢复和跨重启幂等重放，全部检查通过时输出 `"status": "ok"`。
