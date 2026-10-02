"""夜间即时零售履约引擎。

在基础服务之上综合门店现货批次、冷藏状态、保质窗口、配送员容量、
销售区域与品牌渠道规则，为订单生成带有效期的可解释承诺，并在确认时
原子占用库存与运力。所有写入均走请求号幂等，所有状态迁移均记录
order_events，便于重启恢复与对账。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import (
    ConflictError,
    NotFoundError,
    UnfulfillableError,
    ValidationError,
)

# 承诺有效期（秒）
PROMISE_TTL_SECONDS = 120
# 承诺送达窗口（分钟）
DELIVERY_WINDOW_MINUTES = 45
# 单笔订单占用的配送容量槽位
ORDER_SLOTS = 1
# 允许承诺的最短剩余保质期天数
MIN_SHELF_DAYS = 1

# 订单状态机
STATUS_CREATED = "created"          # 已生成承诺，等待确认
STATUS_CONFIRMED = "confirmed"      # 已原子占用库存与运力
STATUS_PICKED = "picked"            # 门店已拣货，配送员已取货
STATUS_DELIVERED = "delivered"      # 已全部送达
STATUS_PARTIAL = "partially_delivered"
STATUS_CANCELLED = "cancelled"
STATUS_EXPIRED = "expired"          # 未确认超时，占用已释放
STATUS_SPLIT = "split"              # 拆单原单
# 不再参与履约的终态
TERMINAL_STATUSES = (STATUS_DELIVERED, STATUS_PARTIAL, STATUS_CANCELLED, STATUS_EXPIRED)
HOLD_HELD = "held"
HOLD_CONSUMED = "consumed"
HOLD_RELEASED = "released"
RES_HELD = "held"
RES_USED = "used"
RES_RELEASED = "released"


def _iso(value: datetime) -> str:
    return value.astimezone().isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    """解析服务生成的 ISO 时间字符串。"""

    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class FulfillmentService:
    """实现承诺、占用、状态迁移与恢复。"""

    def __init__(self, database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def _now(self) -> datetime:
        return self.clock.now().astimezone()

    def _stamp(self) -> str:
        return _iso(self._now())

    @staticmethod
    def _int(value: Any, field: str, minimum: int = 1) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValidationError(f"{field} 必须是整数")
        if value < minimum:
            raise ValidationError(f"{field} 不能小于 {minimum}")
        return value

    @staticmethod
    def _text(value: Any, field: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValidationError(f"{field} 不能为空")
        return value

    def _event(self, connection, *, promise_id: str, request_id: str | None,
               kind: str, detail: dict[str, Any], when: str) -> None:
        connection.execute(
            "INSERT INTO order_events(event_id,promise_id,request_id,kind,detail_json,occurred_at) "
            "VALUES(?,?,?,?,?,?)",
            (uuid.uuid4().hex, promise_id, request_id, kind, canonical_json(detail), when),
        )

    def _save_receipt(self, connection, *, request_id: str, action: str,
                      payload: dict[str, Any], resource_type: str, resource_id: str,
                      response: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
            "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, digest(payload), resource_type, resource_id,
             canonical_json(response), self._stamp()),
        )

    # ------------------------------------------------------------------
    # 主数据维护（供门店数据更新使用，均带版本号）
    # ------------------------------------------------------------------
    def upsert_sku(self, *, sku_id: str, brand_id: str, product_line: str, spec: str,
                   display_name: str, unit_price_milli: int, requires_cold: bool,
                   shelf_life_days: int, active: bool = True) -> None:
        sku_id = self._text(sku_id, "sku_id")
        brand_id = self._text(brand_id, "brand_id")
        spec = self._text(spec, "spec")
        price = self._int(unit_price_milli, "unit_price_milli", 0)
        shelf = self._int(shelf_life_days, "shelf_life_days", 1)
        with self.database.transaction(immediate=True) as conn:
            row = conn.execute("SELECT version FROM skus WHERE sku_id=?", (sku_id,)).fetchone()
            version = (row["version"] + 1) if row else 1
            conn.execute(
                "INSERT INTO skus(sku_id,brand_id,product_line,spec,display_name,unit_price_milli,"
                "requires_cold,shelf_life_days,active,version,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(sku_id) DO UPDATE SET brand_id=excluded.brand_id,"
                "product_line=excluded.product_line,spec=excluded.spec,"
                "display_name=excluded.display_name,unit_price_milli=excluded.unit_price_milli,"
                "requires_cold=excluded.requires_cold,shelf_life_days=excluded.shelf_life_days,"
                "active=excluded.active,version=excluded.version,updated_at=excluded.updated_at",
                (sku_id, brand_id, self._text(product_line, "product_line"), spec,
                 self._text(display_name, "display_name"), price, int(requires_cold), shelf,
                 int(active), version, self._stamp()),
            )

    def upsert_channel_rule(self, *, brand_id: str, channel: str, allowed_specs: list[str]) -> None:
        brand_id = self._text(brand_id, "brand_id")
        channel = self._text(channel, "channel")
        if not allowed_specs or not all(isinstance(s, str) and s.strip() for s in allowed_specs):
            raise ValidationError("allowed_specs 必须是非空字符串列表")
        with self.database.transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT version FROM channel_rules WHERE brand_id=? AND channel=?",
                (brand_id, channel),
            ).fetchone()
            version = (row["version"] + 1) if row else 1
            conn.execute(
                "INSERT INTO channel_rules(brand_id,channel,allowed_specs_json,active,version,updated_at) "
                "VALUES(?,?,?,1,?,?) ON CONFLICT(brand_id,channel) DO UPDATE SET "
                "allowed_specs_json=excluded.allowed_specs_json,version=excluded.version,"
                "updated_at=excluded.updated_at",
                (brand_id, channel, canonical_json(sorted(allowed_specs)), version, self._stamp()),
            )

    def upsert_substitution(self, *, brand_id: str, channel: str, from_sku: str,
                            rank: int, to_sku: str) -> None:
        with self.database.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO substitution_rules(brand_id,channel,from_sku,rank,to_sku) "
                "VALUES(?,?,?,?,?) ON CONFLICT(brand_id,channel,from_sku,rank) DO UPDATE SET "
                "to_sku=excluded.to_sku",
                (self._text(brand_id, "brand_id"), self._text(channel, "channel"),
                 self._text(from_sku, "from_sku"), self._int(rank, "rank", 1),
                 self._text(to_sku, "to_sku")),
            )

    def upsert_store(self, *, store_id: str, name: str, region_id: str,
                     sales_regions: list[str]) -> None:
        store_id = self._text(store_id, "store_id")
        if not sales_regions or region_id not in sales_regions:
            raise ValidationError("门店所在区域必须包含在销售区域内")
        with self.database.transaction(immediate=True) as conn:
            row = conn.execute("SELECT version FROM stores WHERE store_id=?", (store_id,)).fetchone()
            version = (row["version"] + 1) if row else 1
            conn.execute(
                "INSERT INTO stores(store_id,name,region_id,sales_regions_json,active,version,updated_at) "
                "VALUES(?,?,?,?,1,?,?) ON CONFLICT(store_id) DO UPDATE SET name=excluded.name,"
                "region_id=excluded.region_id,sales_regions_json=excluded.sales_regions_json,"
                "version=excluded.version,updated_at=excluded.updated_at",
                (store_id, self._text(name, "name"), self._text(region_id, "region_id"),
                 canonical_json(sorted(sales_regions)), version, self._stamp()),
            )

    def upsert_batch(self, *, batch_id: str, store_id: str, sku_id: str,
                     quantity_on_hand: int, production_date: str, expiration_date: str,
                     chilled: bool) -> None:
        """登记或更新门店批次。数量为物理现货，占用量另行计算。"""

        batch_id = self._text(batch_id, "batch_id")
        qty = self._int(quantity_on_hand, "quantity_on_hand", 0)
        prod = self._text(production_date, "production_date")
        exp = self._text(expiration_date, "expiration_date")
        if parse_iso(f"{exp}T00:00:00+00:00") <= parse_iso(f"{prod}T00:00:00+00:00"):
            raise ValidationError("到期日必须晚于生产日期")
        with self.database.transaction(immediate=True) as conn:
            if conn.execute("SELECT 1 FROM stores WHERE store_id=?", (store_id,)).fetchone() is None:
                raise NotFoundError("门店不存在")
            if conn.execute("SELECT 1 FROM skus WHERE sku_id=?", (sku_id,)).fetchone() is None:
                raise NotFoundError("SKU 不存在")
            held = conn.execute(
                "SELECT COALESCE(SUM(quantity),0) AS held FROM inventory_holds "
                "WHERE batch_id=? AND state=?", (batch_id, HOLD_HELD),
            ).fetchone()["held"]
            if qty < held:
                raise ConflictError("更新后现货量小于当前占用量，不能覆盖在途承诺")
            row = conn.execute("SELECT data_version FROM inventory_batches WHERE batch_id=?",
                               (batch_id,)).fetchone()
            version = (row["data_version"] + 1) if row else 1
            conn.execute(
                "INSERT INTO inventory_batches(batch_id,store_id,sku_id,quantity_on_hand,"
                "production_date,expiration_date,chilled,data_version,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(batch_id) DO UPDATE SET "
                "quantity_on_hand=excluded.quantity_on_hand,production_date=excluded.production_date,"
                "expiration_date=excluded.expiration_date,chilled=excluded.chilled,"
                "data_version=excluded.data_version,updated_at=excluded.updated_at",
                (batch_id, store_id, sku_id, qty, prod, exp, int(chilled), version, self._stamp()),
            )

    def upsert_courier(self, *, courier_id: str, store_id: str, region_id: str,
                       active: bool = True) -> None:
        with self.database.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO couriers(courier_id,store_id,region_id,active) VALUES(?,?,?,?) "
                "ON CONFLICT(courier_id) DO UPDATE SET store_id=excluded.store_id,"
                "region_id=excluded.region_id,active=excluded.active",
                (self._text(courier_id, "courier_id"), self._text(store_id, "store_id"),
                 self._text(region_id, "region_id"), int(active)),
            )

    def upsert_shift(self, *, shift_id: str, courier_id: str, starts_at: str,
                     ends_at: str, capacity: int) -> None:
        start = parse_iso(self._text(starts_at, "starts_at"))
        end = parse_iso(self._text(ends_at, "ends_at"))
        if end <= start:
            raise ValidationError("班次结束时间必须晚于开始时间")
        with self.database.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO courier_shifts(shift_id,courier_id,starts_at,ends_at,capacity,active) "
                "VALUES(?,?,?,?,?,1) ON CONFLICT(shift_id) DO UPDATE SET courier_id=excluded.courier_id,"
                "starts_at=excluded.starts_at,ends_at=excluded.ends_at,capacity=excluded.capacity",
                (self._text(shift_id, "shift_id"), self._text(courier_id, "courier_id"),
                 _iso(start), _iso(end),
                 self._int(capacity, "capacity", 1)),
            )

    # ------------------------------------------------------------------
    # 可售性与选择
    # ------------------------------------------------------------------
    def _sku(self, conn, sku_id: str):
        row = conn.execute("SELECT * FROM skus WHERE sku_id=? AND active=1", (sku_id,)).fetchone()
        if row is None:
            raise UnfulfillableError(f"SKU {sku_id} 不可用", {"sku": sku_id, "reason": "sku_inactive"})
        return row

    def _assert_channel_allows(self, conn, brand_id: str, channel: str, spec: str) -> None:
        row = conn.execute(
            "SELECT allowed_specs_json FROM channel_rules WHERE brand_id=? AND channel=? AND active=1",
            (brand_id, channel),
        ).fetchone()
        if row is None:
            raise UnfulfillableError("品牌未开通当前渠道", {"reason": "brand_channel_closed"})
        if spec not in json.loads(row["allowed_specs_json"]):
            raise UnfulfillableError("当前规格不允许在该渠道销售",
                                     {"reason": "spec_blocked", "spec": spec})

    def _available_batches(self, conn, *, store_id: str, sku_id: str,
                           requires_cold: bool, moment: datetime):
        """返回按（到期日升序、批次号）排序的可售批次与剩余量。"""

        rows = conn.execute(
            "SELECT b.*, COALESCE(SUM(h.quantity),0) AS held FROM inventory_batches b "
            "LEFT JOIN inventory_holds h ON h.batch_id=b.batch_id AND h.state=? "
            "WHERE b.store_id=? AND b.sku_id=? GROUP BY b.batch_id "
            "ORDER BY b.expiration_date, b.batch_id",
            (HOLD_HELD, store_id, sku_id),
        ).fetchall()
        result = []
        for row in rows:
            remaining = row["quantity_on_hand"] - row["held"]
            if remaining <= 0:
                continue
            if requires_cold and not row["chilled"]:
                continue
            expiry = parse_iso(f"{row['expiration_date']}T23:59:59+00:00")
            if expiry < moment + timedelta(days=MIN_SHELF_DAYS):
                continue
            result.append((row, remaining))
        return result

    def _choose_store(self, conn, *, region_id: str) -> str:
        rows = conn.execute("SELECT * FROM stores WHERE active=1").fetchall()
        for row in rows:
            regions = json.loads(row["sales_regions_json"])
            if region_id in regions:
                return row["store_id"]
        raise UnfulfillableError("当前销售区域没有可服务门店",
                                 {"reason": "no_store_in_region", "region": region_id})

    def _substitutes(self, conn, *, brand_id: str, channel: str, sku_id: str):
        rows = conn.execute(
            "SELECT to_sku FROM substitution_rules WHERE brand_id=? AND channel=? AND from_sku=? "
            "ORDER BY rank", (brand_id, channel, sku_id),
        ).fetchall()
        return [r["to_sku"] for r in rows]

    def _allocate_line(self, conn, *, store_id: str, requested_sku: str, quantity: int,
                       channel: str, moment: datetime, allow_substitute: bool):
        """为一个订单行分配 SKU 与批次。

        成功返回 (sku_row, 批次明细, 说明)；失败返回 (None, 缺货说明列表, None)。
        """

        primary = self._sku(conn, requested_sku)
        self._assert_channel_allows(conn, primary["brand_id"], channel, primary["spec"])
        candidates = [(primary, False)]
        if allow_substitute:
            for sub_id in self._substitutes(conn, brand_id=primary["brand_id"],
                                            channel=channel, sku_id=requested_sku):
                sub = conn.execute("SELECT * FROM skus WHERE sku_id=? AND active=1",
                                   (sub_id,)).fetchone()
                if sub is None:
                    continue
                try:
                    self._assert_channel_allows(conn, sub["brand_id"], channel, sub["spec"])
                except UnfulfillableError:
                    continue
                candidates.append((sub, True))

        blockers: list[dict[str, Any]] = []
        for sku, is_sub in candidates:
            batches = self._available_batches(
                conn, store_id=store_id, sku_id=sku["sku_id"],
                requires_cold=bool(sku["requires_cold"]), moment=moment,
            )
            total = sum(remaining for _, remaining in batches)
            if total < quantity:
                blockers.append({"sku": sku["sku_id"], "available": total,
                                 "substituted": is_sub})
                continue
            picks = []
            need = quantity
            for batch, remaining in batches:
                take = min(need, remaining)
                expiry = parse_iso(f"{batch['expiration_date']}T23:59:59+00:00")
                picks.append({
                    "batch_id": batch["batch_id"],
                    "sku_id": sku["sku_id"],
                    "quantity": take,
                    "chilled": bool(batch["chilled"]),
                    "expiration_date": batch["expiration_date"],
                    "remaining_shelf_days": (expiry.date() - moment.date()).days,
                    "data_version": batch["data_version"],
                })
                need -= take
                if need == 0:
                    break
            explain = {
                "requested_sku": requested_sku,
                "allocated_sku": sku["sku_id"],
                "substituted": is_sub,
                "reason": ("指定 SKU 现货不足，按品牌渠道替代规则改配" if is_sub
                           else "现货批次满足冷藏与保质窗口要求"),
                "allocation": "按到期日最近优先（FEFO）",
                "batches": picks,
            }
            return sku, picks, explain
        return None, blockers, None

    def _choose_shift(self, conn, *, store_id: str, region_id: str, moment: datetime,
                      slots: int = ORDER_SLOTS):
        rows = conn.execute(
            "SELECT s.*, c.region_id AS courier_region, c.store_id AS courier_store, c.active AS ca "
            "FROM courier_shifts s JOIN couriers c ON c.courier_id=s.courier_id "
            "WHERE s.active=1 AND c.active=1 AND c.store_id=? AND c.region_id=? "
            "AND s.starts_at<=? AND s.ends_at>?",
            (store_id, region_id, _iso(moment), _iso(moment)),
        ).fetchall()
        for shift in rows:
            used = conn.execute(
                "SELECT COALESCE(SUM(slots),0) AS used FROM capacity_reservations "
                "WHERE shift_id=? AND state=?", (shift["shift_id"], RES_HELD),
            ).fetchone()["used"]
            if shift["capacity"] - used >= slots:
                return shift
        return None

    # ------------------------------------------------------------------
    # 承诺（quote，不占用任何资源）
    # ------------------------------------------------------------------
    def quote(self, *, request_id: str, channel: str, region_id: str,
              customer_location: str, items: list[dict[str, Any]],
              allow_substitute: bool = False) -> dict[str, Any]:
        """生成带有效期的可解释承诺；不写入任何占用。

        默认不擅自改配规格：缺货时仅在 allowed_substitutes 中推荐规则允许的
        替代规格；调用方显式传入 allow_substitute=True 才接受自动改配。
        """

        request_id = self._text(request_id, "request_id")
        channel = self._text(channel, "channel")
        region_id = self._text(region_id, "region_id")
        location = self._text(customer_location, "customer_location")
        if not items:
            raise ValidationError("items 不能为空")
        normalized = []
        for index, item in enumerate(items, start=1):
            normalized.append({
                "line_no": index,
                "sku_id": self._text(item.get("sku_id"), f"items[{index}].sku_id"),
                "quantity": self._int(item.get("quantity"), f"items[{index}].quantity"),
            })
        payload = {"channel": channel, "region_id": region_id, "location": location,
                   "items": normalized, "allow_substitute": allow_substitute}

        moment = self._now()
        with self.database.transaction(immediate=True) as conn:
            receipt_row = conn.execute(
                "SELECT action,payload_hash,response_json FROM request_receipts WHERE request_id=?",
                (request_id,)).fetchone()
            if receipt_row is not None:
                if receipt_row["action"] != "quote" or receipt_row["payload_hash"] != digest(payload):
                    raise ConflictError("request_id 已被不同内容使用")
                cached = json.loads(receipt_row["response_json"])
                cached["replayed"] = True
                return cached
            store_id = self._choose_store(conn, region_id=region_id)
            lines: list[dict[str, Any]] = []
            total_milli = 0
            all_blockers: list[dict[str, Any]] = []
            for wanted in normalized:
                sku, blockers, explain = self._allocate_line(
                    conn, store_id=store_id, requested_sku=wanted["sku_id"],
                    quantity=wanted["quantity"], channel=channel, moment=moment,
                    allow_substitute=allow_substitute,
                )
                if sku is None:
                    all_blockers.extend(blockers or [])
                    continue
                line_total = sku["unit_price_milli"] * wanted["quantity"]
                total_milli += line_total
                lines.append({
                    "line_no": wanted["line_no"],
                    "requested_sku": wanted["sku_id"],
                    "sku_id": sku["sku_id"],
                    "substituted": explain["substituted"],
                    "quantity": wanted["quantity"],
                    "unit_price_milli": sku["unit_price_milli"],
                    "line_total_milli": line_total,
                    "explain": explain,
                })
            shift = self._choose_shift(conn, store_id=store_id, region_id=region_id, moment=moment)
            if shift is None:
                all_blockers.append({"reason": "no_courier_capacity"})
            if lines and not all_blockers:
                expires = moment + timedelta(seconds=PROMISE_TTL_SECONDS)
                response = {
                    "request_kind": "quote",
                    "request_id": request_id,
                    "promise_id": None,
                    "replayed": False,
                    "available": True,
                    "channel": channel,
                    "region_id": region_id,
                    "customer_location": location,
                    "store_id": store_id,
                    "shift_id": shift["shift_id"],
                    "courier_id": shift["courier_id"],
                    "items": lines,
                    "total_milli": total_milli,
                    "currency": "CNY",
                    "promised_from": _iso(moment + timedelta(minutes=15)),
                    "promised_to": _iso(moment + timedelta(minutes=15 + DELIVERY_WINDOW_MINUTES)),
                    "expires_at": _iso(expires),
                    "quoted_at": _iso(moment),
                    "ttl_seconds": PROMISE_TTL_SECONDS,
                    "substitutions_offered": [l["sku_id"] for l in lines if l["substituted"]],
                }
            else:
                offered: list[dict[str, Any]] = []
                # 缺货时始终推荐规则允许的替代规格；allow_substitute 只决定是否自动改配
                for wanted in normalized:
                    primary = conn.execute("SELECT * FROM skus WHERE sku_id=?",
                                           (wanted["sku_id"],)).fetchone()
                    if primary is None:
                        continue
                    for sub_id in self._substitutes(
                            conn, brand_id=primary["brand_id"], channel=channel,
                            sku_id=wanted["sku_id"]):
                        sub = conn.execute("SELECT * FROM skus WHERE sku_id=? AND active=1",
                                           (sub_id,)).fetchone()
                        if sub is None:
                            continue
                        try:
                            self._assert_channel_allows(conn, sub["brand_id"], channel, sub["spec"])
                        except UnfulfillableError:
                            continue
                        avail = sum(r for _, r in self._available_batches(
                            conn, store_id=store_id, sku_id=sub_id,
                            requires_cold=bool(sub["requires_cold"]), moment=moment))
                        offered.append({"requested_sku": wanted["sku_id"], "substitute_sku": sub_id,
                                        "available": avail, "unit_price_milli": sub["unit_price_milli"]})
                response = {
                    "request_kind": "quote",
                    "request_id": request_id,
                    "promise_id": None,
                    "replayed": False,
                    "available": False,
                    "channel": channel,
                    "region_id": region_id,
                    "customer_location": location,
                    "store_id": store_id,
                    "items": [],
                    "total_milli": 0,
                    "currency": "CNY",
                    "blockers": all_blockers,
                    "allowed_substitutes": offered,
                    "quoted_at": _iso(moment),
                }
            # quote 结果也按 request_id 去重，但不登记为资源
            self._save_receipt(conn, request_id=request_id, action="quote",
                               payload=payload,
                               resource_type="quote", resource_id=request_id, response=response)
            return response

    # ------------------------------------------------------------------
    # 确认（原子占用库存与运力）
    # ------------------------------------------------------------------
    def confirm(self, *, request_id: str, quote_request_id: str,
                customer_location: str | None = None) -> dict[str, Any]:
        """基于已报承诺原子占用库存批次与配送容量，生成订单。"""

        request_id = self._text(request_id, "request_id")
        quote_request_id = self._text(quote_request_id, "quote_request_id")
        override_location = (self._text(customer_location, "customer_location")
                             if customer_location is not None else None)
        payload = {"quote_request_id": quote_request_id,
                   "customer_location": override_location}
        with self.database.transaction(immediate=True) as conn:
            receipt_row = conn.execute(
                "SELECT action,payload_hash,response_json FROM request_receipts WHERE request_id=?",
                (request_id,)).fetchone()
            if receipt_row is not None:
                if receipt_row["action"] != "confirm" or receipt_row["payload_hash"] != digest(payload):
                    raise ConflictError("request_id 已被不同内容使用")
                cached = json.loads(receipt_row["response_json"])
                cached["replayed"] = True
                return cached
            quote_row = conn.execute(
                "SELECT * FROM request_receipts WHERE request_id=? AND action='quote'",
                (quote_request_id,),
            ).fetchone()
            if quote_row is None:
                raise NotFoundError("承诺不存在，请先报价")
            quote = json.loads(quote_row["response_json"])
            if not quote.get("available"):
                raise UnfulfillableError("该承诺本来就不可履约", {"reason": "quote_unavailable"})
            if parse_iso(quote["expires_at"]) <= self._now():
                raise UnfulfillableError("承诺已超过有效期，请重新报价",
                                         {"reason": "promise_expired",
                                          "expires_at": quote["expires_at"]})

            moment = self._now()
            promise_id = uuid.uuid4().hex
            store_id = quote["store_id"]
            # 重新校验并占用：二次检查库存与运力，保证报价后数据变化不会超卖
            holds: list[dict[str, Any]] = []
            item_records: list[dict[str, Any]] = []
            total_milli = 0
            for line in quote["items"]:
                sku = self._sku(conn, line["sku_id"])
                self._assert_channel_allows(conn, sku["brand_id"], quote["channel"], sku["spec"])
                need = line["quantity"]
                batches = self._available_batches(
                    conn, store_id=store_id, sku_id=sku["sku_id"],
                    requires_cold=bool(sku["requires_cold"]), moment=moment)
                available = sum(r for _, r in batches)
                if available < need:
                    raise UnfulfillableError(
                        "确认时现货已变化，无法满足承诺",
                        {"reason": "stock_changed", "sku": sku["sku_id"],
                         "needed": need, "available": available})
                line_total = sku["unit_price_milli"] * need
                total_milli += line_total
                picks = []
                for batch, remaining in batches:
                    take = min(need, remaining)
                    hold_id = uuid.uuid4().hex
                    holds.append({"hold_id": hold_id, "batch_id": batch["batch_id"],
                                  "sku_id": sku["sku_id"], "quantity": take})
                    picks.append(take)
                    need -= take
                    if need == 0:
                        break
                item_records.append({**line, "line_total_milli": line_total,
                                     "unit_price_milli": sku["unit_price_milli"]})
            if total_milli != quote["total_milli"]:
                raise UnfulfillableError("确认金额与承诺不一致",
                                         {"reason": "price_changed",
                                          "quoted": quote["total_milli"], "now": total_milli})

            shift = self._choose_shift(conn, store_id=store_id, region_id=quote["region_id"],
                                       moment=moment)
            if shift is None or shift["shift_id"] != quote["shift_id"]:
                raise UnfulfillableError("确认时配送运力已变化",
                                         {"reason": "capacity_changed"})

            location = override_location or quote.get("customer_location") or quote["region_id"]
            now_stamp = _iso(moment)
            # 报价 TTL 仅约束“确认”动作；确认后占用保留至承诺送达窗口上限，
            # 届时仍未取货才允许被超时释放，已取货订单不受影响。
            hold_expires_at = quote["promised_to"]
            conn.execute(
                "INSERT INTO promises(promise_id,request_id,channel,region_id,store_id,"
                "customer_location,status,shift_id,courier_id,expires_at,promised_from,"
                "promised_to,total_milli,response_json,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (promise_id, request_id, quote["channel"], quote["region_id"], store_id,
                 location, STATUS_CONFIRMED, shift["shift_id"], shift["courier_id"],
                 hold_expires_at, quote["promised_from"], quote["promised_to"],
                 total_milli, "{}", now_stamp, now_stamp),
            )
            for hold in holds:
                conn.execute(
                    "INSERT INTO inventory_holds(hold_id,promise_id,batch_id,sku_id,quantity,"
                    "state,created_at) VALUES(?,?,?,?,?,?,?)",
                    (hold["hold_id"], promise_id, hold["batch_id"], hold["sku_id"],
                     hold["quantity"], HOLD_HELD, now_stamp),
                )
            conn.execute(
                "INSERT INTO capacity_reservations(reservation_id,promise_id,shift_id,courier_id,"
                "slots,state,created_at) VALUES(?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, promise_id, shift["shift_id"], shift["courier_id"],
                 ORDER_SLOTS, RES_HELD, now_stamp),
            )
            for line in item_records:
                conn.execute(
                    "INSERT INTO promise_items(promise_id,line_no,sku_id,requested_sku,"
                    "substituted,quantity,unit_price_milli,line_total_milli,explain_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (promise_id, line["line_no"], line["sku_id"], line["requested_sku"],
                     int(line["substituted"]), line["quantity"], line["unit_price_milli"],
                     line["line_total_milli"], canonical_json(line["explain"])),
                )
            self._event(conn, promise_id=promise_id, request_id=request_id, kind="confirmed",
                        detail={"store_id": store_id, "shift_id": shift["shift_id"],
                                "total_milli": total_milli, "holds": len(holds)}, when=now_stamp)
            response = self._order_snapshot(conn, promise_id, request_kind="confirm",
                                            replayed=False)
            conn.execute("UPDATE promises SET response_json=? WHERE promise_id=?",
                         (canonical_json(response), promise_id))
            self._save_receipt(conn, request_id=request_id, action="confirm",
                               payload=payload,
                               resource_type="promise", resource_id=promise_id, response=response)
            append_event(conn, actor_id="system", action="fulfillment.promised",
                         resource_type="promise", resource_id=promise_id,
                         detail={"request_id": request_id, "total_milli": total_milli,
                                 "store_id": store_id}, occurred_at=now_stamp)
            return response

    # ------------------------------------------------------------------
    # 状态迁移：取货 / 送达
    # ------------------------------------------------------------------
    def _load_promise(self, conn, promise_id: str):
        row = conn.execute("SELECT * FROM promises WHERE promise_id=?", (promise_id,)).fetchone()
        if row is None:
            raise NotFoundError("订单不存在")
        return row

    def _family_root(self, promise) -> str:
        """拆单家族共享同一运力占用，返回其根（原单）ID。"""

        return promise["parent_id"] or promise["promise_id"]

    def _family_ids(self, conn, promise) -> list[str]:
        root = self._family_root(promise)
        rows = conn.execute(
            "SELECT promise_id FROM promises WHERE promise_id=? OR parent_id=?",
            (root, root),
        ).fetchall()
        return [r["promise_id"] for r in rows]

    def _family_has_open(self, conn, promise) -> bool:
        """家族中是否还有已确认未取货的订单。"""

        ids = self._family_ids(conn, promise)
        placeholders = ",".join("?" for _ in ids)
        row = conn.execute(
            f"SELECT COUNT(*) AS count FROM promises WHERE promise_id IN ({placeholders}) "
            f"AND status IN (?,?)",
            (*ids, STATUS_CREATED, STATUS_CONFIRMED),
        ).fetchone()
        return row["count"] > 0

    def _release_family_capacity_if_idle(self, conn, promise, when: str) -> None:
        """家族无待取货订单时释放仍保留的运力占用。"""

        if not self._family_has_open(conn, promise):
            root = self._family_root(promise)
            conn.execute(
                "UPDATE capacity_reservations SET state=?,released_at=? "
                "WHERE promise_id=? AND state=?",
                (RES_RELEASED, when, root, RES_HELD),
            )

    def _consume_family_capacity(self, conn, promise) -> None:
        """任一家族成员取货，共享运力即转为已使用，不再受超时释放影响。"""

        root = self._family_root(promise)
        conn.execute(
            "UPDATE capacity_reservations SET state=? WHERE promise_id=? AND state=?",
            (RES_USED, root, RES_HELD),
        )

    def _consume_holds(self, conn, promise_id: str) -> None:
        """把订单持有中的库存转为已消耗，并物理扣减批次现货量。"""

        rows = conn.execute(
            "SELECT batch_id, COALESCE(SUM(quantity),0) AS qty FROM inventory_holds "
            "WHERE promise_id=? AND state=? GROUP BY batch_id",
            (promise_id, HOLD_HELD),
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE inventory_batches SET quantity_on_hand = quantity_on_hand - ? "
                "WHERE batch_id=?",
                (row["qty"], row["batch_id"]),
            )
        conn.execute(
            "UPDATE inventory_holds SET state=? WHERE promise_id=? AND state=?",
            (HOLD_CONSUMED, promise_id, HOLD_HELD),
        )

    def _return_stock(self, conn, batch_id: str, quantity: int) -> None:
        """短少/退回的库存恢复为门店现货。"""

        conn.execute(
            "UPDATE inventory_batches SET quantity_on_hand = quantity_on_hand + ? WHERE batch_id=?",
            (quantity, batch_id),
        )

    def _guard_request(self, conn, request_id: str, action: str,
                       payload: dict[str, Any], promise_id: str) -> dict[str, Any] | None:
        """幂等检查；返回已存响应或 None。不同动作/载荷复用请求号报冲突。"""

        row = conn.execute("SELECT * FROM request_receipts WHERE request_id=?",
                           (request_id,)).fetchone()
        if row is not None:
            if row["action"] != action or row["payload_hash"] != digest(payload) \
                    or row["resource_id"] != promise_id:
                raise ConflictError("request_id 已被不同内容使用")
            cached = json.loads(row["response_json"])
            cached["replayed"] = True
            return cached
        return None

    def mark_picked(self, *, request_id: str, promise_id: str) -> dict[str, Any]:
        """门店拣货完成、配送员取货。取货后占用不再受超时释放影响。"""

        with self.database.transaction(immediate=True) as conn:
            promise = self._load_promise(conn, promise_id)
            payload = {"promise_id": promise_id}
            cached = self._guard_request(conn, request_id, "pick", payload, promise_id)
            if cached:
                return cached
            if promise["status"] != STATUS_CONFIRMED:
                raise ConflictError(f"订单当前状态 {promise['status']} 不能取货")
            now_stamp = self._stamp()
            self._consume_holds(conn, promise_id)
            self._consume_family_capacity(conn, promise)
            conn.execute("UPDATE promises SET status=?,updated_at=? WHERE promise_id=?",
                         (STATUS_PICKED, now_stamp, promise_id))
            self._event(conn, promise_id=promise_id, request_id=request_id, kind="picked",
                        detail={}, when=now_stamp)
            response = self._order_snapshot(conn, promise_id, request_kind="pick", replayed=False)
            self._save_receipt(conn, request_id=request_id, action="pick", payload=payload,
                               resource_type="promise", resource_id=promise_id, response=response)
            return response

    def delivery_receipt(self, *, request_id: str, promise_id: str,
                         delivered: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """登记配送回执。迟到回执仍可登记；数量短少转为部分送达并退款。

        delivered 为 None 表示全部送达；否则给出每行实际送达数量。
        """

        with self.database.transaction(immediate=True) as conn:
            promise = self._load_promise(conn, promise_id)
            payload = {"promise_id": promise_id, "delivered": delivered}
            cached = self._guard_request(conn, request_id, "delivery_receipt", payload, promise_id)
            if cached:
                return cached
            if promise["status"] not in (STATUS_CONFIRMED, STATUS_PICKED):
                raise ConflictError(f"订单当前状态 {promise['status']} 不能登记送达")
            lines = conn.execute(
                "SELECT * FROM promise_items WHERE promise_id=? ORDER BY line_no", (promise_id,)
            ).fetchall()
            actual: dict[int, int] = {}
            for line in lines:
                actual[line["line_no"]] = line["quantity"]
            short: list[dict[str, Any]] = []
            if delivered is not None:
                given = {int(d["line_no"]): self._int(d.get("quantity"), "delivered.quantity", 0)
                         for d in delivered}
                if set(given) - set(actual):
                    raise ValidationError("回执包含不存在的行号")
                for line_no, qty in given.items():
                    ordered = actual[line_no]
                    if qty > ordered:
                        raise ValidationError(f"行 {line_no} 送达数量超过订单数量")
                    actual[line_no] = qty
            refund_milli = 0
            delivered_milli = 0
            delivered_qty = 0
            for line in lines:
                got = actual[line["line_no"]]
                delivered_qty += got
                delivered_milli += got * line["unit_price_milli"]
                missing = line["quantity"] - got
                if missing:
                    refund_milli += missing * line["unit_price_milli"]
                    short.append({"line_no": line["line_no"], "sku_id": line["sku_id"],
                                  "ordered": line["quantity"], "delivered": got,
                                  "missing": missing,
                                  "refund_milli": missing * line["unit_price_milli"]})
            # 数量与金额一致性：已付 = 已交付金额 + 退款金额
            if delivered_milli + refund_milli != promise["total_milli"]:
                raise ConflictError("金额对账不平衡，拒绝登记回执")
            fully = delivered_qty == sum(l["quantity"] for l in lines) and not short
            new_status = STATUS_DELIVERED if fully else STATUS_PARTIAL
            now_stamp = self._stamp()
            if promise["status"] == STATUS_CONFIRMED:
                # 未显式取货直接回执：视为取货后送达
                self._consume_holds(conn, promise_id)
                self._consume_family_capacity(conn, promise)
            # 短少数量对应库存不再消耗：把这部分释放并恢复为门店现货
            if short:
                # 同一 SKU 可能跨多行，先汇总短少量再按当前已消耗记录退回
                missing_by_sku: dict[str, int] = {}
                for item in short:
                    missing_by_sku[item["sku_id"]] = missing_by_sku.get(item["sku_id"], 0) + item["missing"]
                for sku_id, missing_qty in missing_by_sku.items():
                    remaining_release = missing_qty
                    held_rows = conn.execute(
                        "SELECT * FROM inventory_holds WHERE promise_id=? AND sku_id=? AND state=? "
                        "ORDER BY batch_id",
                        (promise_id, sku_id, HOLD_CONSUMED),
                    ).fetchall()
                    # FEFO 取货意味着后取的批次先退回
                    for row in reversed(held_rows):
                        if remaining_release <= 0:
                            break
                        cut = min(row["quantity"], remaining_release)
                        self._return_stock(conn, row["batch_id"], cut)
                        if cut == row["quantity"]:
                            conn.execute(
                                "UPDATE inventory_holds SET state=?,released_at=? WHERE hold_id=?",
                                (HOLD_RELEASED, now_stamp, row["hold_id"]),
                            )
                        else:
                            # 拆分占用：原记录减量，新增一条退回释放记录
                            conn.execute(
                                "UPDATE inventory_holds SET quantity=? WHERE hold_id=?",
                                (row["quantity"] - cut, row["hold_id"]),
                            )
                            conn.execute(
                                "INSERT INTO inventory_holds(hold_id,promise_id,batch_id,sku_id,"
                                "quantity,state,created_at,released_at) VALUES(?,?,?,?,?,?,?,?)",
                                (uuid.uuid4().hex, promise_id, row["batch_id"], row["sku_id"],
                                 cut, HOLD_RELEASED, now_stamp, now_stamp),
                            )
                        remaining_release -= cut
                    if remaining_release > 0:
                        raise ConflictError("短少退回数量超过已占用量，数据不一致")
            conn.execute("UPDATE promises SET status=?,updated_at=? WHERE promise_id=?",
                         (new_status, now_stamp, promise_id))
            self._event(conn, promise_id=promise_id, request_id=request_id,
                        kind="delivered" if fully else "partially_delivered",
                        detail={"delivered_milli": delivered_milli,
                                "refund_milli": refund_milli, "short": short}, when=now_stamp)
            response = self._order_snapshot(
                conn, promise_id, request_kind="delivery_receipt", replayed=False,
                extra={"delivered_milli": delivered_milli, "refund_milli": refund_milli,
                       "short_lines": short})
            self._save_receipt(conn, request_id=request_id, action="delivery_receipt",
                               payload=payload, resource_type="promise", resource_id=promise_id,
                               response=response)
            append_event(conn, actor_id="system",
                         action="fulfillment.delivered" if fully else "fulfillment.partial",
                         resource_type="promise", resource_id=promise_id,
                         detail={"refund_milli": refund_milli}, occurred_at=now_stamp)
            return response

    # ------------------------------------------------------------------
    # 取消（未取货）/ 拆单
    # ------------------------------------------------------------------
    def cancel(self, *, request_id: str, promise_id: str, reason: str = "customer_cancelled") -> dict[str, Any]:
        """取消未取货订单，全额退款并释放库存与运力。"""

        with self.database.transaction(immediate=True) as conn:
            promise = self._load_promise(conn, promise_id)
            payload = {"promise_id": promise_id, "reason": reason}
            cached = self._guard_request(conn, request_id, "cancel", payload, promise_id)
            if cached:
                return cached
            if promise["status"] not in (STATUS_CREATED, STATUS_CONFIRMED):
                raise ConflictError(f"订单当前状态 {promise['status']} 不能取消（已取货请走售后）")
            now_stamp = self._stamp()
            conn.execute(
                "UPDATE inventory_holds SET state=?,released_at=? WHERE promise_id=? AND state=?",
                (HOLD_RELEASED, now_stamp, promise_id, HOLD_HELD),
            )
            conn.execute("UPDATE promises SET status=?,updated_at=? WHERE promise_id=?",
                         (STATUS_CANCELLED, now_stamp, promise_id))
            # 拆单家族共享运力：仅当所有成员都不再待取货时才释放
            self._release_family_capacity_if_idle(conn, promise, now_stamp)
            self._event(conn, promise_id=promise_id, request_id=request_id, kind="cancelled",
                        detail={"reason": reason, "refund_milli": promise["total_milli"]},
                        when=now_stamp)
            response = self._order_snapshot(
                conn, promise_id, request_kind="cancel", replayed=False,
                extra={"refund_milli": promise["total_milli"], "cancel_reason": reason})
            self._save_receipt(conn, request_id=request_id, action="cancel", payload=payload,
                               resource_type="promise", resource_id=promise_id, response=response)
            return response

    def split(self, *, request_id: str, promise_id: str,
              groups: list[list[int]]) -> dict[str, Any]:
        """把已确认未取货订单按行号拆成多单，金额与数量保持不变。"""

        with self.database.transaction(immediate=True) as conn:
            promise = self._load_promise(conn, promise_id)
            payload = {"promise_id": promise_id, "groups": groups}
            cached = self._guard_request(conn, request_id, "split", payload, promise_id)
            if cached:
                return cached
            if promise["status"] != STATUS_CONFIRMED:
                raise ConflictError("只有已确认未取货订单可以拆单")
            if len(groups) < 2:
                raise ValidationError("拆单至少需要两个分组")
            lines = conn.execute(
                "SELECT * FROM promise_items WHERE promise_id=? ORDER BY line_no", (promise_id,)
            ).fetchall()
            by_no = {line["line_no"]: line for line in lines}
            seen: set[int] = set()
            for group in groups:
                if not group:
                    raise ValidationError("拆单分组不能为空")
                for no in group:
                    if no not in by_no:
                        raise ValidationError(f"行号 {no} 不属于该订单")
                    if no in seen:
                        raise ValidationError(f"行号 {no} 在拆单中重复")
                    seen.add(no)
            if seen != set(by_no):
                raise ValidationError("拆单必须覆盖订单全部行且不重复")
            moment = self._now()
            children: list[str] = []
            child_total = 0
            child_qty = 0
            now_stamp = self._stamp()
            for group in groups:
                child_id = uuid.uuid4().hex
                total = sum(by_no[no]["line_total_milli"] for no in group)
                qty = sum(by_no[no]["quantity"] for no in group)
                child_total += total
                child_qty += qty
                conn.execute(
                    "INSERT INTO promises(promise_id,request_id,parent_id,channel,region_id,store_id,"
                    "customer_location,status,shift_id,courier_id,expires_at,promised_from,"
                    "promised_to,total_milli,response_json,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (child_id, None, promise_id, promise["channel"], promise["region_id"],
                     promise["store_id"],
                     promise["customer_location"], STATUS_CONFIRMED, promise["shift_id"],
                     promise["courier_id"], promise["expires_at"], promise["promised_from"],
                     promise["promised_to"], total, "{}", now_stamp, now_stamp),
                )
                for new_no, old_no in enumerate(group, start=1):
                    old = by_no[old_no]
                    conn.execute(
                        "INSERT INTO promise_items(promise_id,line_no,sku_id,requested_sku,"
                        "substituted,quantity,unit_price_milli,line_total_milli,explain_json) "
                        "VALUES(?,?,?,?,?,?,?,?,?)",
                        (child_id, new_no, old["sku_id"], old["requested_sku"],
                         old["substituted"], old["quantity"], old["unit_price_milli"],
                         old["line_total_milli"], old["explain_json"]),
                    )
                # 把对应 SKU 的持有量按数量转移到子单
                for no in group:
                    old = by_no[no]
                    need = old["quantity"]
                    rows = conn.execute(
                        "SELECT * FROM inventory_holds WHERE promise_id=? AND sku_id=? AND state=? "
                        "ORDER BY batch_id", (promise_id, old["sku_id"], HOLD_HELD),
                    ).fetchall()
                    for row in rows:
                        if need <= 0:
                            break
                        cut = min(row["quantity"], need)
                        if cut == row["quantity"]:
                            conn.execute(
                                "UPDATE inventory_holds SET promise_id=? WHERE hold_id=?",
                                (child_id, row["hold_id"]),
                            )
                        else:
                            conn.execute(
                                "UPDATE inventory_holds SET quantity=? WHERE hold_id=?",
                                (row["quantity"] - cut, row["hold_id"]),
                            )
                            conn.execute(
                                "INSERT INTO inventory_holds(hold_id,promise_id,batch_id,sku_id,"
                                "quantity,state,created_at) VALUES(?,?,?,?,?,?,?)",
                                (uuid.uuid4().hex, child_id, row["batch_id"], row["sku_id"],
                                 cut, HOLD_HELD, now_stamp),
                            )
                        need -= cut
                children.append(child_id)
            if child_total != promise["total_milli"] or child_qty != sum(l["quantity"] for l in lines):
                raise ConflictError("拆单数量或金额不平衡")
            # 运力占用保留在原单并随子单共享：原单转为 split，子单合计占用不增加
            conn.execute("DELETE FROM promise_items WHERE promise_id=?", (promise_id,))
            conn.execute("UPDATE promises SET status=?,updated_at=? WHERE promise_id=?",
                         (STATUS_SPLIT, now_stamp, promise_id))
            self._event(conn, promise_id=promise_id, request_id=request_id, kind="split",
                        detail={"children": children, "total_milli": child_total}, when=now_stamp)
            for child_id in children:
                self._event(conn, promise_id=child_id, request_id=None, kind="created_from_split",
                            detail={"parent": promise_id}, when=now_stamp)
            response = {"request_kind": "split", "request_id": request_id,
                        "promise_id": promise_id, "status": STATUS_SPLIT,
                        "children": [self._order_snapshot(conn, c, request_kind="child",
                                                         replayed=False) for c in children],
                        "total_milli": promise["total_milli"],
                        "quantity_conserved": child_qty == sum(l["quantity"] for l in lines),
                        "amount_conserved": child_total == promise["total_milli"]}
            self._save_receipt(conn, request_id=request_id, action="split", payload=payload,
                               resource_type="promise", resource_id=promise_id, response=response)
            return response

    # ------------------------------------------------------------------
    # 超时释放与恢复
    # ------------------------------------------------------------------
    def expire_stale_promises(self) -> list[str]:
        """释放已过期但仍处于 created/confirmed 且未取货的承诺。

        已取货（picked 及之后）的订单绝不释放。
        """

        moment = self._now()
        expired: list[str] = []
        with self.database.transaction(immediate=True) as conn:
            rows = conn.execute(
                "SELECT * FROM promises WHERE status IN (?,?) AND expires_at<=?",
                (STATUS_CREATED, STATUS_CONFIRMED, _iso(moment)),
            ).fetchall()
            now_stamp = _iso(moment)
            for row in rows:
                conn.execute(
                    "UPDATE inventory_holds SET state=?,released_at=? WHERE promise_id=? AND state=?",
                    (HOLD_RELEASED, now_stamp, row["promise_id"], HOLD_HELD),
                )
                conn.execute("UPDATE promises SET status=?,updated_at=? WHERE promise_id=?",
                             (STATUS_EXPIRED, now_stamp, row["promise_id"]))
                # 家族共享运力：仅当全部成员都不再待取货时释放；已取货成员会使计数保持非零
                self._release_family_capacity_if_idle(conn, row, now_stamp)
                self._event(conn, promise_id=row["promise_id"], request_id=row["request_id"],
                            kind="expired", detail={"sweep": True}, when=now_stamp)
                expired.append(row["promise_id"])
        return expired

    def pending_promises(self) -> list[dict[str, Any]]:
        """重启恢复：列出仍待履约（已确认/已取货/部分送达）的订单。"""

        rows = self.database.connection.execute(
            "SELECT * FROM promises WHERE status IN (?,?,?) ORDER BY created_at",
            (STATUS_CONFIRMED, STATUS_PICKED, STATUS_PARTIAL),
        ).fetchall()
        return [self._order_snapshot(self.database.connection, r["promise_id"],
                                     request_kind="pending", replayed=False) for r in rows]

    def reconcile(self) -> dict[str, Any]:
        """对账：校验库存占用、运力占用与订单数量金额的一致性。"""

        conn = self.database.connection
        problems: list[str] = []
        for batch in conn.execute("SELECT * FROM inventory_batches").fetchall():
            held = conn.execute(
                "SELECT COALESCE(SUM(quantity),0) AS held FROM inventory_holds "
                "WHERE batch_id=? AND state=?", (batch["batch_id"], HOLD_HELD),
            ).fetchone()["held"]
            # 持有量是从物理现货中冻结但尚未出店的部分，仍计入 quantity_on_hand
            if held > batch["quantity_on_hand"]:
                problems.append(f"批次 {batch['batch_id']} 冻结量超过现货")
        for promise in conn.execute("SELECT * FROM promises").fetchall():
            lines_total = conn.execute(
                "SELECT COALESCE(SUM(line_total_milli),0) AS total, "
                "COALESCE(SUM(quantity),0) AS qty FROM promise_items WHERE promise_id=?",
                (promise["promise_id"],),
            ).fetchone()
            if promise["status"] != STATUS_SPLIT and lines_total["total"] != promise["total_milli"]:
                problems.append(f"订单 {promise['promise_id']} 行金额合计与总额不符")
            if promise["status"] in (STATUS_CONFIRMED, STATUS_PICKED):
                held_qty = conn.execute(
                    "SELECT COALESCE(SUM(quantity),0) AS q FROM inventory_holds "
                    "WHERE promise_id=? AND state IN (?,?)",
                    (promise["promise_id"], HOLD_HELD, HOLD_CONSUMED),
                ).fetchone()["q"]
                if held_qty != lines_total["qty"]:
                    problems.append(
                        f"订单 {promise['promise_id']} 占用数量 {held_qty} 与订单数量 "
                        f"{lines_total['qty']} 不一致")
        # 拆单守恒：子单合计金额与数量等于父单
        for parent in conn.execute("SELECT * FROM promises WHERE status=?",
                                   (STATUS_SPLIT,)).fetchall():
            agg = conn.execute(
                "SELECT COALESCE(SUM(p.total_milli),0) AS total FROM promises p "
                "WHERE p.parent_id=?", (parent["promise_id"],),
            ).fetchone()
            if agg["total"] != parent["total_milli"]:
                problems.append(f"拆单 {parent['promise_id']} 子单金额合计与父单不符")
        return {"balanced": not problems, "problems": problems}

    # ------------------------------------------------------------------
    # 快照
    # ------------------------------------------------------------------
    def _order_snapshot(self, conn, promise_id: str, *, request_kind: str,
                        replayed: bool, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        promise = self._load_promise(conn, promise_id)
        items = []
        for row in conn.execute(
                "SELECT * FROM promise_items WHERE promise_id=? ORDER BY line_no",
                (promise_id,)).fetchall():
            items.append({"line_no": row["line_no"], "requested_sku": row["requested_sku"],
                          "sku_id": row["sku_id"], "substituted": bool(row["substituted"]),
                          "quantity": row["quantity"],
                          "unit_price_milli": row["unit_price_milli"],
                          "line_total_milli": row["line_total_milli"],
                          "explain": json.loads(row["explain_json"])})
        holds = conn.execute(
            "SELECT batch_id,sku_id,quantity,state FROM inventory_holds WHERE promise_id=?",
            (promise_id,)).fetchall()
        snapshot = {
            "request_kind": request_kind,
            "request_id": promise["request_id"],
            "promise_id": promise_id,
            "status": promise["status"],
            "channel": promise["channel"],
            "region_id": promise["region_id"],
            "store_id": promise["store_id"],
            "customer_location": promise["customer_location"],
            "shift_id": promise["shift_id"],
            "courier_id": promise["courier_id"],
            "items": items,
            "total_milli": promise["total_milli"],
            "currency": "CNY",
            "promised_from": promise["promised_from"],
            "promised_to": promise["promised_to"],
            "expires_at": promise["expires_at"],
            "replayed": replayed,
            "holds": [dict(h) for h in holds],
        }
        if extra:
            snapshot.update(extra)
        return snapshot

    def get_promise(self, promise_id: str) -> dict[str, Any]:
        return self._order_snapshot(self.database.connection, promise_id,
                                    request_kind="get", replayed=False)

    def history(self, promise_id: str) -> list[dict[str, Any]]:
        rows = self.database.connection.execute(
            "SELECT sequence,kind,detail_json,occurred_at,request_id FROM order_events "
            "WHERE promise_id=? ORDER BY sequence", (promise_id,)).fetchall()
        return [{"sequence": r["sequence"], "kind": r["kind"], "request_id": r["request_id"],
                 "occurred_at": r["occurred_at"], "detail": json.loads(r["detail_json"])}
                for r in rows]
