"""夜间即时零售履约领域：可解释报价、原子占用与一致结算。"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import Any, Callable

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import (
    ConflictError,
    InsufficientCapacityError,
    NotFoundError,
    OrderStateError,
    PermissionDenied,
    QuoteExpiredError,
    ValidationError,
)
from .storage import Database

# 报价默认有效期（秒）。夜间高峰的承诺窗口刻意保持较短。
DEFAULT_QUOTE_TTL_SECONDS = 120

# 批次被排除的理由码，同时作为对消费者可解释承诺的依据。
REASON_BATCH_INACTIVE = "batch_inactive"
REASON_STORE_INACTIVE = "store_inactive"
REASON_COLD_CHAIN_BROKEN = "cold_chain_broken"
REASON_NEAR_EXPIRY = "near_expiry"
REASON_CHANNEL_NOT_ALLOWED = "channel_not_allowed"
REASON_PRODUCT_INACTIVE = "product_inactive"
REASON_OUT_OF_STOCK = "out_of_stock"
REASON_BRAND_CHANNEL_BLOCKED = "brand_channel_blocked"
REASON_BRAND_REGION_BLOCKED = "brand_region_blocked"

WRITE_ROLES = ("admin", "operator")


class FulfillmentService:
    """编排夜间即时零售的询价、占库、派运力与回执处理。"""

    def __init__(self, database: Database, clock: Clock | None = None,
                 quote_ttl_seconds: int = DEFAULT_QUOTE_TTL_SECONDS) -> None:
        self.database = database
        self.clock = clock or SystemClock()
        self.quote_ttl_seconds = quote_ttl_seconds

    # ------------------------------------------------------------------ 基础

    def _ts(self) -> str:
        return self.clock.now().isoformat(timespec="microseconds").replace("+00:00", "Z")

    def _today(self) -> date:
        return self.clock.now().date()

    def _actor(self, connection, actor_id: str, roles: tuple[str, ...] | None = None):
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not bool(row["active"]):
            raise PermissionDenied("操作者已停用")
        if roles and row["role"] not in roles:
            raise PermissionDenied("当前角色不能执行该动作")
        return row

    @staticmethod
    def _require(condition: bool, message: str) -> None:
        if not condition:
            raise ValidationError(message)

    def _idempotent(self, *, request_id: str, action: str, payload: dict[str, Any],
                    create: Callable[[Any], dict[str, Any]]) -> dict[str, Any]:
        """在同一写事务内完成回执查重、业务写入与回执落库。

        - 相同 request_id + 相同载荷：回放首次结果（迟到回执/平台重试安全）；
        - 相同 request_id + 不同载荷：409 冲突；
        - 回执与业务写入同事务提交，崩溃恢复后不会重复创建。
        """

        if not isinstance(request_id, str) or not request_id.strip():
            raise ValidationError("request_id 不能为空")
        request_id = request_id.strip()
        payload_hash = digest(payload)
        with self.database.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is not None:
                if row["action"] != action or row["payload_hash"] != payload_hash:
                    raise ConflictError("request_id 已被不同内容使用")
                return {**json.loads(row["response_json"]), "replayed": True}
            result = create(connection)
            result.pop("replayed", None)
            connection.execute(
                "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,"
                "resource_id,response_json,created_at) VALUES(?,?,?,?,?,?,?)",
                (request_id, action, payload_hash, result.get("resource_type", ""),
                 result.get("resource_id", ""), canonical_json(result), self._ts()),
            )
            return {**result, "replayed": False}

    def _order_event(self, connection, *, order_id: str, event_type: str,
                     detail: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO order_events(event_id,order_id,event_type,occurred_at,detail_json) "
            "VALUES(?,?,?,?,?)",
            (uuid.uuid4().hex, order_id, event_type, self._ts(), canonical_json(detail)),
        )

    @staticmethod
    def _product(connection, product_id: str, cache: dict[str, Any] | None = None):
        if cache is not None and product_id in cache:
            return cache[product_id]
        row = connection.execute("SELECT * FROM products WHERE product_id=?",
                                 (product_id,)).fetchone()
        if cache is not None:
            cache[product_id] = row
        return row

    # ---------------------------------------------------------- 目录与规则

    def register_channel(self, *, request_id: str, actor_id: str, channel_id: str,
                         name: str) -> dict[str, Any]:
        payload = {"channel_id": channel_id, "name": name}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id, WRITE_ROLES)
            self._require(isinstance(channel_id, str) and channel_id.strip(),
                          "channel_id 不能为空")
            clean_name = (name or "").strip()
            self._require(bool(clean_name), "name 不能为空")
            if connection.execute("SELECT 1 FROM channels WHERE channel_id=?",
                                  (channel_id,)).fetchone():
                raise ConflictError("渠道已存在")
            connection.execute(
                "INSERT INTO channels(channel_id,name,active,created_at) VALUES(?,?,1,?)",
                (channel_id, clean_name, self._ts()),
            )
            append_event(connection, actor_id=actor_id, action="channel.registered",
                         resource_type="channel", resource_id=channel_id,
                         detail={"name": clean_name}, occurred_at=self._ts())
            return {"resource_type": "channel", "resource_id": channel_id,
                    "channel_id": channel_id}

        return self._idempotent(request_id=request_id, action="register_channel",
                                payload=payload, create=create)

    def register_product(self, *, request_id: str, actor_id: str, product_id: str,
                         brand_id: str, spec_name: str, tier: str, requires_cold: bool,
                         list_price_cents: int) -> dict[str, Any]:
        payload = {"product_id": product_id, "brand_id": brand_id, "spec_name": spec_name,
                   "tier": tier, "requires_cold": requires_cold,
                   "list_price_cents": list_price_cents}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id, WRITE_ROLES)
            self._require(isinstance(product_id, str) and product_id.strip(),
                          "product_id 不能为空")
            self._require(isinstance(brand_id, str) and brand_id.strip(), "brand_id 不能为空")
            clean_spec = (spec_name or "").strip()
            self._require(bool(clean_spec), "spec_name 不能为空")
            self._require(isinstance(list_price_cents, int) and list_price_cents >= 0,
                          "list_price_cents 必须是非负整数")
            if connection.execute("SELECT 1 FROM products WHERE product_id=?",
                                  (product_id,)).fetchone():
                raise ConflictError("商品已存在")
            connection.execute(
                "INSERT INTO products(product_id,brand_id,spec_name,tier,requires_cold,"
                "list_price_cents,active,created_at) VALUES(?,?,?,?,?,?,1,?)",
                (product_id, brand_id, clean_spec, tier, 1 if requires_cold else 0,
                 list_price_cents, self._ts()),
            )
            append_event(connection, actor_id=actor_id, action="product.registered",
                         resource_type="product", resource_id=product_id,
                         detail={"brand_id": brand_id, "spec_name": clean_spec,
                                 "tier": tier},
                         occurred_at=self._ts())
            return {"resource_type": "product", "resource_id": product_id,
                    "product_id": product_id}

        return self._idempotent(request_id=request_id, action="register_product",
                                payload=payload, create=create)

    def register_store(self, *, request_id: str, actor_id: str, store_id: str,
                       organization_id: str, name: str, region_id: str) -> dict[str, Any]:
        payload = {"store_id": store_id, "organization_id": organization_id,
                   "name": name, "region_id": region_id}

        def create(connection) -> dict[str, Any]:
            actor = self._actor(connection, actor_id, WRITE_ROLES)
            self._require(isinstance(store_id, str) and store_id.strip(), "store_id 不能为空")
            if connection.execute("SELECT 1 FROM stores WHERE store_id=?",
                                  (store_id,)).fetchone():
                raise ConflictError("门店已存在")
            if connection.execute(
                "SELECT 1 FROM organizations WHERE organization_id=?", (organization_id,)
            ).fetchone() is None:
                raise NotFoundError("经营主体不存在")
            if actor["organization_id"] != organization_id and actor["role"] != "admin":
                raise PermissionDenied("不能为其他组织登记门店")
            connection.execute(
                "INSERT INTO stores(store_id,organization_id,name,region_id,active,version,"
                "created_at,updated_at) VALUES(?,?,?,?,1,1,?,?)",
                (store_id, organization_id, name, region_id, self._ts(), self._ts()),
            )
            append_event(connection, actor_id=actor_id, action="store.registered",
                         resource_type="store", resource_id=store_id,
                         detail={"region_id": region_id, "name": name},
                         occurred_at=self._ts())
            return {"resource_type": "store", "resource_id": store_id,
                    "store_id": store_id, "version": 1}

        return self._idempotent(request_id=request_id, action="register_store",
                                payload=payload, create=create)

    def upsert_batch(self, *, request_id: str, actor_id: str, batch_id: str, store_id: str,
                     product_id: str, lot_no: str, qty_on_hand: int, cold_chain_ok: bool,
                     expiry_date: str, near_expiry_days: int, unit_price_cents: int,
                     channel_tags: list[str] | None = None, production_date: str | None = None,
                     active: bool = True) -> dict[str, Any]:
        """登记或更新门店现货批次；数量下调不得击穿已取货/已售义务。"""

        payload = {"batch_id": batch_id, "store_id": store_id, "product_id": product_id,
                   "lot_no": lot_no, "qty_on_hand": qty_on_hand, "cold_chain_ok": cold_chain_ok,
                   "expiry_date": expiry_date, "near_expiry_days": near_expiry_days,
                   "unit_price_cents": unit_price_cents,
                   "channel_tags": sorted(channel_tags or []),
                   "production_date": production_date, "active": active}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id, WRITE_ROLES)
            self._require(isinstance(qty_on_hand, int) and qty_on_hand >= 0,
                          "qty_on_hand 必须是非负整数")
            self._require(isinstance(near_expiry_days, int) and near_expiry_days >= 0,
                          "near_expiry_days 必须是非负整数")
            self._require(isinstance(unit_price_cents, int) and unit_price_cents >= 0,
                          "unit_price_cents 必须是非负整数")
            date.fromisoformat(expiry_date)
            if production_date:
                date.fromisoformat(production_date)
            if connection.execute("SELECT 1 FROM stores WHERE store_id=? AND active=1",
                                  (store_id,)).fetchone() is None:
                raise NotFoundError("门店不存在或已停用")
            if connection.execute("SELECT 1 FROM products WHERE product_id=?",
                                  (product_id,)).fetchone() is None:
                raise NotFoundError("商品不存在")
            existing = connection.execute(
                "SELECT * FROM store_batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            tags_json = canonical_json(sorted(channel_tags or []))
            if existing is not None:
                if existing["store_id"] != store_id or existing["product_id"] != product_id \
                        or existing["lot_no"] != lot_no:
                    raise ConflictError("批次主键与既有记录不一致")
                obligated = (existing["qty_held"] + existing["qty_picked"]
                             + existing["qty_sold"])
                if qty_on_hand < obligated:
                    raise InsufficientCapacityError(
                        "现货数量不能低于已占用、已取货与已售出数量之和")
                connection.execute(
                    "UPDATE store_batches SET qty_on_hand=?, cold_chain_ok=?, expiry_date=?, "
                    "production_date=?, near_expiry_days=?, unit_price_cents=?, "
                    "channel_tags_json=?, active=?, version=version+1, updated_at=? "
                    "WHERE batch_id=? AND version=?",
                    (qty_on_hand, 1 if cold_chain_ok else 0, expiry_date, production_date,
                     near_expiry_days, unit_price_cents, tags_json, 1 if active else 0,
                     self._ts(), batch_id, existing["version"]),
                )
                version = existing["version"] + 1
                action = "batch.updated"
            else:
                connection.execute(
                    "INSERT INTO store_batches(batch_id,store_id,product_id,lot_no,qty_on_hand,"
                    "qty_held,qty_picked,qty_sold,cold_chain_ok,production_date,expiry_date,"
                    "near_expiry_days,channel_tags_json,unit_price_cents,active,version,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,0,0,0,?,?,?,?,?,?,?,1,?,?)",
                    (batch_id, store_id, product_id, lot_no, qty_on_hand,
                     1 if cold_chain_ok else 0, production_date, expiry_date,
                     near_expiry_days, tags_json, unit_price_cents, 1 if active else 0,
                     self._ts(), self._ts()),
                )
                version = 1
                action = "batch.registered"
            append_event(connection, actor_id=actor_id, action=action,
                         resource_type="batch", resource_id=batch_id,
                         detail={"store_id": store_id, "product_id": product_id,
                                 "lot_no": lot_no, "qty_on_hand": qty_on_hand,
                                 "cold_chain_ok": cold_chain_ok, "active": active,
                                 "version": version}, occurred_at=self._ts())
            return {"resource_type": "batch", "resource_id": batch_id,
                    "batch_id": batch_id, "version": version}

        return self._idempotent(request_id=request_id, action="upsert_batch",
                                payload=payload, create=create)

    def set_brand_channel_rule(self, *, request_id: str, actor_id: str, brand_id: str,
                               channel_id: str, allowed: bool) -> dict[str, Any]:
        payload = {"brand_id": brand_id, "channel_id": channel_id, "allowed": allowed}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id, WRITE_ROLES)
            connection.execute(
                "INSERT INTO brand_channel_rules(brand_id,channel_id,allowed,updated_at) "
                "VALUES(?,?,?,?) ON CONFLICT(brand_id,channel_id) DO UPDATE SET "
                "allowed=excluded.allowed, updated_at=excluded.updated_at",
                (brand_id, channel_id, 1 if allowed else 0, self._ts()),
            )
            append_event(connection, actor_id=actor_id, action="brand_channel_rule.set",
                         resource_type="brand_channel_rule",
                         resource_id=f"{brand_id}:{channel_id}",
                         detail={"allowed": allowed}, occurred_at=self._ts())
            return {"resource_type": "brand_channel_rule",
                    "resource_id": f"{brand_id}:{channel_id}", "allowed": allowed}

        return self._idempotent(request_id=request_id, action="set_brand_channel_rule",
                                payload=payload, create=create)

    def set_brand_region_rule(self, *, request_id: str, actor_id: str, brand_id: str,
                              region_id: str, allowed: bool) -> dict[str, Any]:
        payload = {"brand_id": brand_id, "region_id": region_id, "allowed": allowed}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id, WRITE_ROLES)
            connection.execute(
                "INSERT INTO brand_region_rules(brand_id,region_id,allowed,updated_at) "
                "VALUES(?,?,?,?) ON CONFLICT(brand_id,region_id) DO UPDATE SET "
                "allowed=excluded.allowed, updated_at=excluded.updated_at",
                (brand_id, region_id, 1 if allowed else 0, self._ts()),
            )
            append_event(connection, actor_id=actor_id, action="brand_region_rule.set",
                         resource_type="brand_region_rule",
                         resource_id=f"{brand_id}:{region_id}",
                         detail={"allowed": allowed}, occurred_at=self._ts())
            return {"resource_type": "brand_region_rule",
                    "resource_id": f"{brand_id}:{region_id}", "allowed": allowed}

        return self._idempotent(request_id=request_id, action="set_brand_region_rule",
                                payload=payload, create=create)

    def set_substitution_rule(self, *, request_id: str, actor_id: str, product_id: str,
                              channel_id: str, substitute_product_id: str,
                              priority: int) -> dict[str, Any]:
        payload = {"product_id": product_id, "channel_id": channel_id,
                   "substitute_product_id": substitute_product_id, "priority": priority}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id, WRITE_ROLES)
            self._require(product_id != substitute_product_id,
                          "替代规格不能与原规格相同")
            self._require(isinstance(priority, int), "priority 必须是整数")
            connection.execute(
                "INSERT INTO substitution_rules(product_id,channel_id,substitute_product_id,"
                "priority,active,updated_at) VALUES(?,?,?,?,1,?) "
                "ON CONFLICT(product_id,channel_id,substitute_product_id) DO UPDATE SET "
                "priority=excluded.priority, active=1, updated_at=excluded.updated_at",
                (product_id, channel_id, substitute_product_id, priority, self._ts()),
            )
            append_event(connection, actor_id=actor_id, action="substitution_rule.set",
                         resource_type="substitution_rule",
                         resource_id=f"{product_id}:{channel_id}:{substitute_product_id}",
                         detail={"priority": priority}, occurred_at=self._ts())
            return {"resource_type": "substitution_rule",
                    "resource_id": f"{product_id}:{channel_id}:{substitute_product_id}",
                    "priority": priority}

        return self._idempotent(request_id=request_id, action="set_substitution_rule",
                                payload=payload, create=create)

    def register_courier(self, *, request_id: str, actor_id: str, courier_id: str, name: str,
                         region_id: str, capacity_units: int) -> dict[str, Any]:
        payload = {"courier_id": courier_id, "name": name, "region_id": region_id,
                   "capacity_units": capacity_units}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id, WRITE_ROLES)
            self._require(isinstance(capacity_units, int) and capacity_units > 0,
                          "capacity_units 必须是正整数")
            if connection.execute("SELECT 1 FROM couriers WHERE courier_id=?",
                                  (courier_id,)).fetchone():
                raise ConflictError("配送员已存在")
            connection.execute(
                "INSERT INTO couriers(courier_id,name,region_id,capacity_units,active,created_at) "
                "VALUES(?,?,?,?,1,?)",
                (courier_id, name, region_id, capacity_units, self._ts()),
            )
            append_event(connection, actor_id=actor_id, action="courier.registered",
                         resource_type="courier", resource_id=courier_id,
                         detail={"region_id": region_id,
                                 "capacity_units": capacity_units},
                         occurred_at=self._ts())
            return {"resource_type": "courier", "resource_id": courier_id,
                    "courier_id": courier_id}

        return self._idempotent(request_id=request_id, action="register_courier",
                                payload=payload, create=create)

    # ------------------------------------------------------------- 报价引擎

    def _brand_allowed(self, connection, brand_id: str, channel_id: str,
                       region_id: str) -> tuple[bool, str | None]:
        row = connection.execute(
            "SELECT allowed FROM brand_channel_rules WHERE brand_id=? AND channel_id=?",
            (brand_id, channel_id),
        ).fetchone()
        if row is None or not row["allowed"]:
            return False, REASON_BRAND_CHANNEL_BLOCKED
        row = connection.execute(
            "SELECT allowed FROM brand_region_rules WHERE brand_id=? AND region_id=?",
            (brand_id, region_id),
        ).fetchone()
        if row is None or not row["allowed"]:
            return False, REASON_BRAND_REGION_BLOCKED
        return True, None

    def _eligible_batches(self, connection, *, product_id: str, store_ids: list[str],
                          channel_id: str
                          ) -> tuple[list[tuple[Any, int]], list[dict[str, Any]]]:
        """返回（FEFO 排序的(批次行, 可用量)，每个被排除批次的理由说明）。"""

        excluded: list[dict[str, Any]] = []
        product = self._product(connection, product_id)
        if product is None:
            raise NotFoundError(f"商品不存在: {product_id}")
        today = self._today()
        candidates: list[tuple[Any, int, str]] = []
        if not store_ids:
            return [], excluded
        placeholders = ",".join("?" for _ in store_ids)
        rows = connection.execute(
            f"SELECT b.*, s.active AS store_active FROM store_batches b "
            f"JOIN stores s ON s.store_id=b.store_id "
            f"WHERE b.product_id=? AND b.store_id IN ({placeholders})",
            [product_id, *store_ids],
        ).fetchall()
        for row in rows:
            reasons: list[str] = []
            if not bool(product["active"]):
                reasons.append(REASON_PRODUCT_INACTIVE)
            if not bool(row["store_active"]):
                reasons.append(REASON_STORE_INACTIVE)
            if not bool(row["active"]):
                reasons.append(REASON_BATCH_INACTIVE)
            if bool(product["requires_cold"]) and not bool(row["cold_chain_ok"]):
                reasons.append(REASON_COLD_CHAIN_BROKEN)
            tags = json.loads(row["channel_tags_json"])
            if tags and channel_id not in tags:
                reasons.append(REASON_CHANNEL_NOT_ALLOWED)
            remaining_days = (date.fromisoformat(row["expiry_date"]) - today).days
            if remaining_days < int(row["near_expiry_days"]):
                reasons.append(REASON_NEAR_EXPIRY)
            available = (row["qty_on_hand"] - row["qty_held"]
                         - row["qty_picked"] - row["qty_sold"])
            if available <= 0:
                reasons.append(REASON_OUT_OF_STOCK)
            entry = {"store_id": row["store_id"], "batch_id": row["batch_id"],
                     "lot_no": row["lot_no"], "remaining_days": remaining_days,
                     "available_qty": max(available, 0)}
            if reasons:
                excluded.append({**entry, "reasons": reasons})
            else:
                candidates.append((row, available, remaining_days))
        candidates.sort(key=lambda item: (item[0]["expiry_date"], item[0]["lot_no"]))
        return [(row, available) for row, available, _ in candidates], excluded

    def _allocate(self, candidates: list[tuple[Any, int]], need: int
                  ) -> tuple[list[dict[str, Any]], int]:
        """按 FEFO 从候选批次分配，返回（分配明细，未满足数量）。"""

        allocations: list[dict[str, Any]] = []
        remaining = need
        for row, available in candidates:
            if remaining <= 0:
                break
            take = min(available, remaining)
            if take <= 0:
                continue
            allocations.append({
                "batch_id": row["batch_id"], "lot_no": row["lot_no"],
                "store_id": row["store_id"], "qty": take,
                "unit_price_cents": row["unit_price_cents"],
                "expiry_date": row["expiry_date"],
                "cold_chain_ok": bool(row["cold_chain_ok"]),
                "remaining_days": (date.fromisoformat(row["expiry_date"]) - self._today()).days,
            })
            remaining -= take
        return allocations, remaining

    def _courier_capacity(self, connection, region_id: str) -> list[dict[str, Any]]:
        # 已送达的任务释放运力；在途（planned/picked）仍占用容量。
        rows = connection.execute(
            "SELECT c.*, COALESCE((SELECT SUM(d.units) FROM deliveries d "
            "WHERE d.courier_id=c.courier_id AND d.status IN ('planned','picked')),0) "
            "AS used_units FROM couriers c WHERE c.region_id=? AND c.active=1",
            (region_id,),
        ).fetchall()
        result = []
        for row in rows:
            spare = row["capacity_units"] - row["used_units"]
            result.append({"courier_id": row["courier_id"], "name": row["name"],
                           "capacity_units": row["capacity_units"],
                           "used_units": row["used_units"], "spare_units": spare})
        result.sort(key=lambda item: (-item["spare_units"], item["courier_id"]))
        return result

    @staticmethod
    def _normalize_items(items: Any) -> list[dict[str, Any]]:
        if not isinstance(items, list) or not items:
            raise ValidationError("items 必须是非空数组")
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                raise ValidationError("items 元素必须是对象")
            product_id = item.get("product_id")
            qty = item.get("qty")
            if not isinstance(product_id, str) or not product_id.strip():
                raise ValidationError("product_id 不能为空")
            if not isinstance(qty, int) or qty <= 0:
                raise ValidationError("qty 必须是正整数")
            if product_id in seen:
                raise ValidationError(f"同一商品在询价单中只能出现一次: {product_id}")
            seen.add(product_id)
            normalized.append({"product_id": product_id, "qty": qty})
        return normalized

    def _plan_store(self, connection, *, store_ids: list[str], channel_id: str,
                    items: list[dict[str, Any]], product_cache: dict[str, Any]
                    ) -> dict[str, Any]:
        """基于门店当前数据试算单店履约计划与排除理由。"""

        lines: list[dict[str, Any]] = []
        excluded_map: dict[str, list[dict[str, Any]]] = {}
        substitute_map: dict[str, list[dict[str, Any]]] = {}
        region_id = connection.execute(
            "SELECT region_id FROM stores WHERE store_id=?", (store_ids[0],)
        ).fetchone()["region_id"]
        for item in items:
            product_id = item["product_id"]
            product = self._product(connection, product_id, product_cache)
            allowed, block_reason = self._brand_allowed(
                connection, product["brand_id"], channel_id, region_id)
            if not allowed:
                excluded_map[product_id] = [{"scope": "brand", "reasons": [block_reason]}]
                candidates: list[tuple[Any, int]] = []
            else:
                candidates, excluded = self._eligible_batches(
                    connection, product_id=product_id, store_ids=store_ids,
                    channel_id=channel_id)
                excluded_map[product_id] = excluded
            allocations, shortfall = self._allocate(candidates, item["qty"])
            lines.append({"product_id": product_id, "qty": item["qty"],
                          "qty_matched": item["qty"] - shortfall,
                          "shortfall": shortfall, "allocations": allocations})
            if shortfall:
                substitute_map[product_id] = self._recommend_substitutes(
                    connection, product_id=product_id, store_ids=store_ids,
                    channel_id=channel_id, product_cache=product_cache)
        return {"lines": lines, "excluded": excluded_map,
                "substitutes": substitute_map}

    def _recommend_substitutes(self, connection, *, product_id: str, store_ids: list[str],
                               channel_id: str, product_cache: dict[str, Any]
                               ) -> list[dict[str, Any]]:
        """缺货时只返回品牌/区域/冷链/渠道/保质规则全部允许且有现货的替代规格。"""

        region_id = connection.execute(
            "SELECT region_id FROM stores WHERE store_id=?", (store_ids[0],)
        ).fetchone()["region_id"]
        rows = connection.execute(
            "SELECT substitute_product_id, priority FROM substitution_rules "
            "WHERE product_id=? AND channel_id=? AND active=1 "
            "ORDER BY priority, substitute_product_id",
            (product_id, channel_id),
        ).fetchall()
        recommendations: list[dict[str, Any]] = []
        for row in rows:
            sub_id = row["substitute_product_id"]
            product = self._product(connection, sub_id, product_cache)
            if product is None or not bool(product["active"]):
                continue
            allowed, _ = self._brand_allowed(connection, product["brand_id"],
                                             channel_id, region_id)
            if not allowed:
                continue
            candidates, excluded = self._eligible_batches(
                connection, product_id=sub_id, store_ids=store_ids, channel_id=channel_id)
            allocations, _ = self._allocate(candidates, 10**9)
            available_qty = sum(a["qty"] for a in allocations)
            if available_qty <= 0:
                continue
            recommendations.append({
                "for_product_id": product_id, "product_id": sub_id,
                "spec_name": product["spec_name"], "tier": product["tier"],
                "priority": row["priority"], "available_qty": available_qty,
                "batches": allocations, "excluded_batches": excluded,
            })
        return recommendations

    def quote(self, *, request_id: str, actor_id: str, channel_id: str, region_id: str,
              customer_id: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        """生成带有效期的可解释承诺（不占用任何库存与运力）。"""

        normalized_items = self._normalize_items(items)
        payload = {"actor_id": actor_id, "channel_id": channel_id, "region_id": region_id,
                   "customer_id": customer_id, "items": normalized_items}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id)
            if connection.execute(
                "SELECT 1 FROM channels WHERE channel_id=? AND active=1", (channel_id,)
            ).fetchone() is None:
                raise NotFoundError("渠道不存在或已停用")
            store_ids = [row["store_id"] for row in connection.execute(
                "SELECT store_id FROM stores WHERE region_id=? AND active=1 ORDER BY store_id",
                (region_id,)).fetchall()]

            # 逐门店试算，选择可满足数量最多的门店（全订单只从一个门店履约）。
            best: dict[str, Any] | None = None
            product_cache: dict[str, Any] = {}
            for store_id in store_ids:
                plan = self._plan_store(connection, store_ids=[store_id],
                                        channel_id=channel_id, items=normalized_items,
                                        product_cache=product_cache)
                score = sum(line["qty_matched"] for line in plan["lines"])
                if best is None or score > best["score"]:
                    best = {"score": score, "store_id": store_id, "plan": plan}
            if best is None:
                best = {"score": 0, "store_id": None,
                        "plan": {"lines": [], "excluded": {}, "substitutes": {}}}
            plan = best["plan"]

            total_qty = sum(item["qty"] for item in normalized_items)
            matched_qty = sum(line["qty_matched"] for line in plan["lines"])
            feasible = matched_qty == total_qty

            couriers = self._courier_capacity(connection, region_id)
            total_spare = sum(c["spare_units"] for c in couriers if c["spare_units"] > 0)
            courier_view = {"feasible": total_spare >= total_qty,
                            "available_units": total_spare,
                            "options": [{"courier_id": c["courier_id"],
                                         "spare_units": c["spare_units"]}
                                        for c in couriers if c["spare_units"] > 0][:5]}
            feasible = feasible and courier_view["feasible"]

            total_cents = 0
            line_views: list[dict[str, Any]] = []
            for line in plan["lines"]:
                amount = sum(a["qty"] * a["unit_price_cents"] for a in line["allocations"])
                total_cents += amount
                product = self._product(connection, line["product_id"], product_cache)
                line_views.append({
                    "requested_product_id": line["product_id"],
                    "product_id": line["product_id"],
                    "spec_name": product["spec_name"], "tier": product["tier"],
                    "qty_requested": line["qty"], "qty_matched": line["qty_matched"],
                    "shortfall": line["shortfall"], "amount_cents": amount,
                    "batches": line["allocations"],
                    "excluded_batches": plan["excluded"].get(line["product_id"], []),
                    "substitutes": plan["substitutes"].get(line["product_id"], []),
                })

            now = self.clock.now()
            expires_at = datetime.fromtimestamp(
                now.timestamp() + self.quote_ttl_seconds, tz=now.tzinfo)
            expires_text = expires_at.isoformat().replace("+00:00", "Z")
            quote_id = uuid.uuid4().hex
            stored_payload = {"items": normalized_items, "lines": line_views}
            connection.execute(
                "INSERT INTO quotes(quote_id,request_id,region_id,channel_id,customer_id,"
                "store_id,feasible,status,total_qty,total_cents,expires_at,created_at,"
                "payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (quote_id, request_id, region_id, channel_id, customer_id,
                 best["store_id"], 1 if feasible else 0, "offered", total_qty, total_cents,
                 expires_text, self._ts(), canonical_json(stored_payload)),
            )
            append_event(connection, actor_id=actor_id, action="quote.created",
                         resource_type="quote", resource_id=quote_id,
                         detail={"channel_id": channel_id, "region_id": region_id,
                                 "feasible": feasible, "store_id": best["store_id"],
                                 "total_cents": total_cents}, occurred_at=self._ts())
            return {"resource_type": "quote", "resource_id": quote_id, "quote_id": quote_id,
                    "feasible": feasible, "status": "offered",
                    "store_id": best["store_id"], "expires_at": expires_text,
                    "ttl_seconds": self.quote_ttl_seconds, "total_qty": total_qty,
                    "total_cents": total_cents, "courier": courier_view,
                    "lines": line_views}

        return self._idempotent(request_id=request_id, action="quote",
                                payload=payload, create=create)

    # --------------------------------------------------------------- 确认

    @staticmethod
    def _validate_substitutions_input(substitutions: Any) -> None:
        if not isinstance(substitutions, list):
            raise ValidationError("substitutions 必须是数组")
        seen: set[tuple[str, str]] = set()
        for sub in substitutions:
            if not isinstance(sub, dict):
                raise ValidationError("substitutions 元素必须是对象")
            key = (sub.get("for_product_id", ""), sub.get("product_id", ""))
            if key in seen:
                raise ValidationError("同一替代规格不能重复提交")
            seen.add(key)

    def confirm_order(self, *, request_id: str, actor_id: str, quote_id: str,
                      substitutions: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """在单事务内原子占用库存批次与配送运力，失败整体回滚。"""

        substitutions = substitutions or []
        payload = {"actor_id": actor_id, "quote_id": quote_id,
                   "substitutions": substitutions}
        self._validate_substitutions_input(substitutions)

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id)
            quote = connection.execute("SELECT * FROM quotes WHERE quote_id=?",
                                       (quote_id,)).fetchone()
            if quote is None:
                raise NotFoundError("报价不存在")
            if quote["status"] == "converted":
                raise OrderStateError("报价已经转化为订单")
            if quote["status"] == "expired" or self._ts() >= quote["expires_at"]:
                connection.execute("UPDATE quotes SET status='expired' WHERE quote_id=?",
                                   (quote_id,))
                raise QuoteExpiredError("报价已超过有效期，请重新询价")

            stored = json.loads(quote["payload_json"])
            wanted: dict[str, int] = {item["product_id"]: item["qty"]
                                      for item in stored["items"]}
            # 报价时给出的替代规格构成确认时的唯一白名单。
            allowed_subs: dict[tuple[str, str], int] = {}
            for line in stored["lines"]:
                for sub in line["substitutes"]:
                    allowed_subs[(sub["for_product_id"], sub["product_id"])] = \
                        sub["available_qty"]

            sub_requests: list[dict[str, Any]] = []
            for sub in substitutions:
                key = (sub.get("for_product_id"), sub.get("product_id"))
                if key not in allowed_subs:
                    raise ValidationError("替代规格不在当前报价允许范围内")
                qty = sub.get("qty")
                if not isinstance(qty, int) or qty <= 0 or qty > allowed_subs[key]:
                    raise ValidationError("替代数量超出可推荐数量")
                sub_requests.append({"for_product_id": key[0],
                                     "product_id": key[1], "qty": qty})

            store_id = quote["store_id"]
            if not store_id:
                raise InsufficientCapacityError("当前区域没有可履约门店，请重新询价")

            # 以门店最新数据重算（报价后批次可能更新过）。
            cache: dict[str, Any] = {}
            plan = self._plan_store(
                connection, store_ids=[store_id], channel_id=quote["channel_id"],
                items=[{"product_id": pid, "qty": qty} for pid, qty in wanted.items()],
                product_cache=cache)
            shortfall = {line["product_id"]: line["shortfall"] for line in plan["lines"]}
            primary_alloc = {line["product_id"]: line["allocations"] for line in plan["lines"]}

            # 用替代规格逐个补齐缺口；替代品按最新批次数据独立分配。
            covered_by_sub: dict[str, int] = {}
            sub_allocs: list[dict[str, Any]] = []
            for sub in sub_requests:
                target = sub["for_product_id"]
                gap = shortfall.get(target, 0) - covered_by_sub.get(target, 0)
                if gap <= 0:
                    raise ValidationError(f"{target} 已由现货满足，无需替代规格")
                take = min(sub["qty"], gap)
                candidates, _ = self._eligible_batches(
                    connection, product_id=sub["product_id"], store_ids=[store_id],
                    channel_id=quote["channel_id"])
                allocations, still_short = self._allocate(candidates, take)
                if still_short:
                    raise InsufficientCapacityError("替代规格现货不足，请重新询价")
                sub_allocs.append({"for_product_id": target,
                                   "product_id": sub["product_id"],
                                   "allocations": allocations})
                covered_by_sub[target] = covered_by_sub.get(target, 0) + take

            for pid, gap in shortfall.items():
                if gap - covered_by_sub.get(pid, 0) > 0:
                    raise InsufficientCapacityError("现货或运力已变化，请重新询价")

            line_records: list[dict[str, Any]] = []
            total = {"cents": 0, "units": 0}
            order_id = uuid.uuid4().hex
            connection.execute(
                "INSERT INTO orders(order_id,quote_id,customer_id,region_id,channel_id,"
                "store_id,status,total_qty,total_cents,refunded_cents,expires_at,created_at,"
                "updated_at) VALUES(?,?,?,?,?,?,'held',0,0,0,?,?,?)",
                (order_id, quote_id, quote["customer_id"], quote["region_id"],
                 quote["channel_id"], store_id, quote["expires_at"], self._ts(), self._ts()),
            )

            def persist_lines(product_id: str, requested_product_id: str, kind: str,
                              allocations: list[dict[str, Any]]) -> None:
                for alloc in allocations:
                    line_id = uuid.uuid4().hex
                    amount = alloc["qty"] * alloc["unit_price_cents"]
                    total["cents"] += amount
                    total["units"] += alloc["qty"]
                    connection.execute(
                        "INSERT INTO order_lines(line_id,order_id,product_id,"
                        "requested_product_id,kind,batch_id,unit_price_cents,qty_committed,"
                        "qty_delivered,qty_refunded,refunded_cents) "
                        "VALUES(?,?,?,?,?,?,?,?,0,0,0)",
                        (line_id, order_id, product_id, requested_product_id, kind,
                         alloc["batch_id"], alloc["unit_price_cents"], alloc["qty"]),
                    )
                    line_records.append({"line_id": line_id, "product_id": product_id,
                                         "requested_product_id": requested_product_id,
                                         "kind": kind, "batch_id": alloc["batch_id"],
                                         "lot_no": alloc["lot_no"], "qty": alloc["qty"],
                                         "unit_price_cents": alloc["unit_price_cents"],
                                         "amount_cents": amount})

            for pid in wanted:
                persist_lines(pid, pid, "primary", primary_alloc.get(pid, []))
            for sub in sub_allocs:
                persist_lines(sub["product_id"], sub["for_product_id"], "substitute",
                              sub["allocations"])

            # 条件占用库存：任一批次可用量不足则整单回滚，绝不超卖。
            for line in line_records:
                cursor = connection.execute(
                    "UPDATE store_batches SET qty_held=qty_held+?, version=version+1, "
                    "updated_at=? WHERE batch_id=? AND active=1 AND ? <= "
                    "qty_on_hand-qty_held-qty_picked-qty_sold",
                    (line["qty"], self._ts(), line["batch_id"], line["qty"]),
                )
                if cursor.rowcount != 1:
                    raise InsufficientCapacityError("批次现货不足，占用失败")

            # 按配送员实时余量拆单装箱；检查与插入在同一写事务内。
            deliveries = self._pack_deliveries(connection, region_id=quote["region_id"],
                                               line_records=line_records,
                                               units_total=total["units"])

            connection.execute(
                "UPDATE orders SET total_qty=?, total_cents=?, updated_at=? WHERE order_id=?",
                (total["units"], total["cents"], self._ts(), order_id),
            )
            for delivery in deliveries:
                connection.execute(
                    "INSERT INTO deliveries(delivery_id,order_id,courier_id,units,status,"
                    "planned_at) VALUES(?,?,?,?,'planned',?)",
                    (delivery["delivery_id"], order_id, delivery["courier_id"],
                     delivery["units"], self._ts()),
                )
                for item in delivery["items"]:
                    connection.execute(
                        "INSERT INTO delivery_items(delivery_id,line_id,qty) VALUES(?,?,?)",
                        (delivery["delivery_id"], item["line_id"], item["qty"]),
                    )
            connection.execute("UPDATE quotes SET status='converted' WHERE quote_id=?",
                               (quote_id,))
            self._order_event(connection, order_id=order_id, event_type="order.held",
                              detail={"total_qty": total["units"],
                                      "total_cents": total["cents"],
                                      "deliveries": [d["delivery_id"] for d in deliveries]})
            append_event(connection, actor_id=actor_id, action="order.confirmed",
                         resource_type="order", resource_id=order_id,
                         detail={"quote_id": quote_id, "total_qty": total["units"],
                                 "total_cents": total["cents"],
                                 "delivery_count": len(deliveries)},
                         occurred_at=self._ts())
            return {"resource_type": "order", "resource_id": order_id, "order_id": order_id,
                    "status": "held", "store_id": store_id,
                    "expires_at": quote["expires_at"], "total_qty": total["units"],
                    "total_cents": total["cents"], "refunded_cents": 0,
                    "lines": line_records,
                    "deliveries": [{"delivery_id": d["delivery_id"],
                                    "courier_id": d["courier_id"], "units": d["units"]}
                                   for d in deliveries]}

        return self._idempotent(request_id=request_id, action="confirm_order",
                                payload=payload, create=create)

    def _pack_deliveries(self, connection, *, region_id: str,
                         line_records: list[dict[str, Any]], units_total: int
                         ) -> list[dict[str, Any]]:
        """把订单单位流按配送员实时余量贪心装箱，容量不足时整体失败。"""

        couriers = self._courier_capacity(connection, region_id)
        available = [{"courier_id": c["courier_id"], "remaining": c["spare_units"]}
                     for c in couriers if c["spare_units"] > 0]
        if not available or sum(c["remaining"] for c in available) < units_total:
            raise InsufficientCapacityError("区域配送运力不足")

        bins: list[dict[str, Any]] = []
        courier_index = 0
        current: dict[str, Any] | None = None
        for line in line_records:
            for _ in range(line["qty"]):
                if current is None or current["remaining"] <= 0:
                    while courier_index < len(available) \
                            and available[courier_index]["remaining"] <= 0:
                        courier_index += 1
                    if courier_index >= len(available):
                        raise InsufficientCapacityError("拆单时配送运力不足")
                    current = {"delivery_id": uuid.uuid4().hex,
                               "courier_id": available[courier_index]["courier_id"],
                               "units": 0, "remaining": available[courier_index]["remaining"],
                               "items": {}}
                    bins.append(current)
                current["items"][line["line_id"]] = \
                    current["items"].get(line["line_id"], 0) + 1
                current["units"] += 1
                current["remaining"] -= 1
                available[courier_index]["remaining"] -= 1
        return [{"delivery_id": b["delivery_id"], "courier_id": b["courier_id"],
                 "units": b["units"],
                 "items": [{"line_id": lid, "qty": qty}
                           for lid, qty in sorted(b["items"].items())]} for b in bins]

    # ----------------------------------------------------------- 履约回执

    def _delivery_guard(self, connection, delivery_id: str):
        delivery = connection.execute("SELECT * FROM deliveries WHERE delivery_id=?",
                                      (delivery_id,)).fetchone()
        if delivery is None:
            raise NotFoundError("配送任务不存在")
        order = connection.execute("SELECT * FROM orders WHERE order_id=?",
                                   (delivery["order_id"],)).fetchone()
        return delivery, order

    def mark_picked(self, *, request_id: str, actor_id: str, delivery_id: str) -> dict[str, Any]:
        """门店取货：占位转为取货，订单自此受超时回收保护。"""

        payload = {"actor_id": actor_id, "delivery_id": delivery_id}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id)
            delivery, order = self._delivery_guard(connection, delivery_id)
            if delivery["status"] == "picked":
                raise OrderStateError("配送任务已取货")
            if delivery["status"] != "planned":
                raise OrderStateError("配送任务当前状态不能取货")
            if order["status"] == "cancelled":
                raise OrderStateError("订单已取消，不能取货")
            items = connection.execute(
                "SELECT * FROM delivery_items WHERE delivery_id=?", (delivery_id,)
            ).fetchall()
            # 取货前再次核验批次仍合规（可能刚被门店更新为失温/停用）。
            for item in items:
                row = connection.execute(
                    "SELECT b.active AS batch_active, b.cold_chain_ok, p.requires_cold "
                    "FROM order_lines l JOIN store_batches b ON b.batch_id=l.batch_id "
                    "JOIN products p ON p.product_id=l.product_id WHERE l.line_id=?",
                    (item["line_id"],)).fetchone()
                if not bool(row["batch_active"]):
                    raise OrderStateError("批次已被门店停用，不能取货")
                if bool(row["requires_cold"]) and not bool(row["cold_chain_ok"]):
                    raise OrderStateError("批次冷链失效，不能取货")
            for item in items:
                cursor = connection.execute(
                    "UPDATE store_batches SET qty_held=qty_held-?, qty_picked=qty_picked+?, "
                    "version=version+1, updated_at=? WHERE batch_id="
                    "(SELECT batch_id FROM order_lines WHERE line_id=?) AND qty_held>=?",
                    (item["qty"], item["qty"], self._ts(), item["line_id"], item["qty"]),
                )
                if cursor.rowcount != 1:
                    raise InsufficientCapacityError("批次占用量不足，取货失败")
            connection.execute(
                "UPDATE deliveries SET status='picked', picked_at=? WHERE delivery_id=?",
                (self._ts(), delivery_id),
            )
            if order["status"] == "held":
                connection.execute(
                    "UPDATE orders SET status='picked', updated_at=? WHERE order_id=?",
                    (self._ts(), order["order_id"]),
                )
            self._order_event(connection, order_id=order["order_id"],
                              event_type="delivery.picked",
                              detail={"delivery_id": delivery_id,
                                      "courier_id": delivery["courier_id"]})
            append_event(connection, actor_id=actor_id, action="delivery.picked",
                         resource_type="delivery", resource_id=delivery_id,
                         detail={"order_id": order["order_id"]}, occurred_at=self._ts())
            return {"resource_type": "delivery", "resource_id": delivery_id,
                    "delivery_id": delivery_id, "order_id": order["order_id"],
                    "status": "picked"}

        return self._idempotent(request_id=request_id, action="mark_picked",
                                payload=payload, create=create)

    def complete_delivery(self, *, request_id: str, actor_id: str, delivery_id: str,
                          delivered_items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """登记送达结果，支持部分送达；未送达部分退款并释放对应库存。"""

        delivered_items = delivered_items or []
        payload = {"actor_id": actor_id, "delivery_id": delivery_id,
                   "delivered_items": delivered_items}
        if not isinstance(delivered_items, list):
            raise ValidationError("delivered_items 必须是数组")

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id)
            delivery, order = self._delivery_guard(connection, delivery_id)
            if delivery["status"] not in ("picked", "planned"):
                raise OrderStateError("配送任务已结束，不能重复登记送达")
            prior_status = delivery["status"]
            carried = {row["line_id"]: row["qty"] for row in connection.execute(
                "SELECT line_id,qty FROM delivery_items WHERE delivery_id=?",
                (delivery_id,))}
            delivered_map = {line_id: 0 for line_id in carried}
            for entry in delivered_items:
                line_id = entry.get("line_id")
                qty = entry.get("qty")
                if line_id not in delivered_map:
                    raise ValidationError("送达明细不属于该配送任务")
                if not isinstance(qty, int) or qty < 0:
                    raise ValidationError("送达数量必须是非负整数")
                delivered_map[line_id] += qty
            line_rows = {row["line_id"]: row for row in connection.execute(
                "SELECT * FROM order_lines WHERE order_id=?", (order["order_id"],))}
            for line_id, qty in delivered_map.items():
                if qty > carried[line_id]:
                    raise ValidationError("送达数量超过该配送任务携带数量")
                row = line_rows[line_id]
                if row["qty_delivered"] + qty > row["qty_committed"]:
                    raise ValidationError("累计送达数量超过订单承诺数量")

            # 库存方向取决于取货是否已登记：planned 直接送达则从 held 转出。
            held_col = "qty_held" if prior_status == "planned" else "qty_picked"
            delivered_total = 0
            refunded_total = 0
            refunded_qty_total = 0
            per_line: list[dict[str, Any]] = []
            for line_id, delivered_qty in delivered_map.items():
                row = line_rows[line_id]
                short_qty = carried[line_id] - delivered_qty
                if delivered_qty:
                    connection.execute(
                        f"UPDATE store_batches SET {held_col}={held_col}-?, "
                        "qty_sold=qty_sold+?, version=version+1, updated_at=? WHERE batch_id=?",
                        (delivered_qty, delivered_qty, self._ts(), row["batch_id"]),
                    )
                    connection.execute(
                        "UPDATE order_lines SET qty_delivered=qty_delivered+? WHERE line_id=?",
                        (delivered_qty, line_id),
                    )
                if short_qty:
                    refund_cents = short_qty * row["unit_price_cents"]
                    connection.execute(
                        f"UPDATE store_batches SET {held_col}={held_col}-?, "
                        "version=version+1, updated_at=? WHERE batch_id=? AND "
                        f"{held_col}>=?",
                        (short_qty, self._ts(), row["batch_id"], short_qty),
                    )
                    connection.execute(
                        "UPDATE order_lines SET qty_refunded=qty_refunded+?, "
                        "refunded_cents=refunded_cents+? WHERE line_id=?",
                        (short_qty, refund_cents, line_id),
                    )
                    connection.execute(
                        "UPDATE orders SET refunded_cents=refunded_cents+? WHERE order_id=?",
                        (refund_cents, order["order_id"]),
                    )
                    refunded_total += refund_cents
                    refunded_qty_total += short_qty
                delivered_total += delivered_qty
                per_line.append({"line_id": line_id, "delivered_qty": delivered_qty,
                                 "refunded_qty": short_qty,
                                 "refunded_cents": short_qty * row["unit_price_cents"]})

            connection.execute(
                "UPDATE deliveries SET status='delivered', delivered_at=? WHERE delivery_id=?",
                (self._ts(), delivery_id),
            )
            self._refresh_order_totals(connection, order["order_id"])
            self._order_event(connection, order_id=order["order_id"],
                              event_type="delivery.completed",
                              detail={"delivery_id": delivery_id,
                                      "delivered_qty": delivered_total,
                                      "refunded_qty": refunded_qty_total,
                                      "refunded_cents": refunded_total})
            append_event(connection, actor_id=actor_id, action="delivery.completed",
                         resource_type="delivery", resource_id=delivery_id,
                         detail={"order_id": order["order_id"],
                                 "delivered_qty": delivered_total,
                                 "refunded_cents": refunded_total},
                         occurred_at=self._ts())
            final_order = connection.execute("SELECT * FROM orders WHERE order_id=?",
                                             (order["order_id"],)).fetchone()
            return {"resource_type": "delivery", "resource_id": delivery_id,
                    "delivery_id": delivery_id, "order_id": order["order_id"],
                    "status": "delivered", "lines": per_line,
                    "order_status": final_order["status"],
                    "refunded_cents_total": final_order["refunded_cents"]}

        return self._idempotent(request_id=request_id, action="complete_delivery",
                                payload=payload, create=create)

    def _refresh_order_totals(self, connection, order_id: str) -> None:
        """所有配送腿结束后落终态：无退款 delivered，有退款 partial。"""

        open_deliveries = connection.execute(
            "SELECT COUNT(*) AS count FROM deliveries WHERE order_id=? "
            "AND status NOT IN ('cancelled','delivered')", (order_id,)).fetchone()["count"]
        if open_deliveries != 0:
            connection.execute("UPDATE orders SET status='picked', updated_at=? WHERE order_id=?",
                               (self._ts(), order_id))
            return
        refunded = connection.execute(
            "SELECT COALESCE(SUM(qty_refunded),0) AS refunded FROM order_lines WHERE order_id=?",
            (order_id,)).fetchone()["refunded"]
        status = "delivered" if refunded == 0 else "partial"
        connection.execute(
            "UPDATE orders SET status=?, updated_at=?, finalized_at=? WHERE order_id=?",
            (status, self._ts(), self._ts(), order_id),
        )

    def cancel_order(self, *, request_id: str, actor_id: str, order_id: str) -> dict[str, Any]:
        """取消尚未取货的订单，释放全部占位库存与运力。"""

        payload = {"actor_id": actor_id, "order_id": order_id}

        def create(connection) -> dict[str, Any]:
            self._actor(connection, actor_id)
            order = connection.execute("SELECT * FROM orders WHERE order_id=?",
                                       (order_id,)).fetchone()
            if order is None:
                raise NotFoundError("订单不存在")
            if order["status"] != "held":
                raise OrderStateError("只有尚未取货的订单可以取消")
            released = self._release_held_order(connection, order_id,
                                                reason="user_cancelled")
            append_event(connection, actor_id=actor_id, action="order.cancelled",
                         resource_type="order", resource_id=order_id,
                         detail={"released_qty": released}, occurred_at=self._ts())
            return {"resource_type": "order", "resource_id": order_id,
                    "order_id": order_id, "status": "cancelled",
                    "released_qty": released}

        return self._idempotent(request_id=request_id, action="cancel_order",
                                payload=payload, create=create)

    def _release_held_order(self, connection, order_id: str, *, reason: str) -> int:
        """释放 held 订单全部占位并取消未执行配送；仅可在订单未取货时调用。"""

        released = 0
        rows = connection.execute(
            "SELECT line_id,batch_id,qty_committed FROM order_lines WHERE order_id=?",
            (order_id,),
        ).fetchall()
        for row in rows:
            cursor = connection.execute(
                "UPDATE store_batches SET qty_held=qty_held-?, version=version+1, updated_at=? "
                "WHERE batch_id=? AND qty_held>=?",
                (row["qty_committed"], self._ts(), row["batch_id"], row["qty_committed"]),
            )
            if cursor.rowcount != 1:
                raise InsufficientCapacityError("释放占位时发现批次数据不一致")
            released += row["qty_committed"]
        connection.execute(
            "UPDATE deliveries SET status='cancelled', cancelled_at=? WHERE order_id=? "
            "AND status='planned'", (self._ts(), order_id),
        )
        connection.execute(
            "UPDATE orders SET status='cancelled', updated_at=?, finalized_at=? WHERE order_id=?",
            (self._ts(), self._ts(), order_id),
        )
        connection.execute(
            "UPDATE quotes SET status='expired' WHERE quote_id="
            "(SELECT quote_id FROM orders WHERE order_id=?)", (order_id,),
        )
        self._order_event(connection, order_id=order_id, event_type="order.released",
                          detail={"reason": reason, "released_qty": released})
        return released

    # ----------------------------------------------------------- 超时与恢复

    def reconcile(self) -> dict[str, Any]:
        """超时回收与失配回收。

        - 到期报价置 expired；
        - 到期且仍为 held 的订单整单释放；
        - 报价后门店把批次更新为停用/失温的 held 订单同样整单释放；
        - 已取货（picked 及之后）订单一律不动；
        - 全部状态在 SQLite 中，进程重启后调用本方法即可继续待履约流程。
        """

        with self.database.transaction(immediate=True) as connection:
            now_text = self._ts()
            expired_quotes = connection.execute(
                "UPDATE quotes SET status='expired' WHERE status='offered' AND expires_at<=?",
                (now_text,),
            ).rowcount
            order_ids = [row["order_id"] for row in connection.execute(
                "SELECT order_id FROM orders WHERE status='held' AND expires_at<=?",
                (now_text,))]
            invalidated = [row["order_id"] for row in connection.execute(
                "SELECT DISTINCT o.order_id FROM orders o "
                "JOIN order_lines l ON l.order_id=o.order_id "
                "JOIN store_batches b ON b.batch_id=l.batch_id "
                "JOIN products p ON p.product_id=l.product_id "
                "WHERE o.status='held' AND (b.active=0 OR "
                "(p.requires_cold=1 AND b.cold_chain_ok=0))")]
            targets: list[tuple[str, str]] = [("hold_expired", oid) for oid in order_ids]
            expired_set = set(order_ids)
            for oid in invalidated:
                if oid not in expired_set:
                    targets.append(("batch_ineligible", oid))
            released_qty = 0
            expired_released: list[str] = []
            invalidated_released: list[str] = []
            for reason, oid in targets:
                released_qty += self._release_held_order(connection, oid, reason=reason)
                (expired_released if reason == "hold_expired"
                 else invalidated_released).append(oid)
            picked_protected = connection.execute(
                "SELECT COUNT(*) AS count FROM orders "
                "WHERE status IN ('picked','partial','delivered')"
            ).fetchone()["count"]
            return {"now": now_text, "expired_quotes": expired_quotes,
                    "expired_orders": expired_released,
                    "invalidated_orders": invalidated_released,
                    "released_qty": released_qty,
                    "picked_orders_protected": picked_protected}

    # --------------------------------------------------------------- 查询

    def get_order(self, order_id: str) -> dict[str, Any]:
        with self.database.reading() as connection:
            order = connection.execute("SELECT * FROM orders WHERE order_id=?",
                                       (order_id,)).fetchone()
            if order is None:
                raise NotFoundError("订单不存在")
            lines = []
            for row in connection.execute("SELECT * FROM order_lines WHERE order_id=? ",
                                          (order_id,)):
                lines.append({
                    "line_id": row["line_id"], "product_id": row["product_id"],
                    "requested_product_id": row["requested_product_id"],
                    "kind": row["kind"], "batch_id": row["batch_id"],
                    "unit_price_cents": row["unit_price_cents"],
                    "qty_committed": row["qty_committed"],
                    "qty_delivered": row["qty_delivered"],
                    "qty_refunded": row["qty_refunded"],
                    "refunded_cents": row["refunded_cents"],
                    "committed_cents": row["qty_committed"] * row["unit_price_cents"],
                })
            deliveries = []
            for row in connection.execute("SELECT * FROM deliveries WHERE order_id=?",
                                          (order_id,)):
                items = [{"line_id": item["line_id"], "qty": item["qty"]} for item in
                         connection.execute("SELECT * FROM delivery_items WHERE delivery_id=?",
                                            (row["delivery_id"],))]
                deliveries.append({"delivery_id": row["delivery_id"],
                                   "courier_id": row["courier_id"], "units": row["units"],
                                   "status": row["status"], "items": items,
                                   "planned_at": row["planned_at"],
                                   "picked_at": row["picked_at"],
                                   "delivered_at": row["delivered_at"]})
            result = dict(order)
            result["lines"] = lines
            result["deliveries"] = deliveries
            delivered_cents = sum(line["qty_delivered"] * line["unit_price_cents"]
                                  for line in lines)
            result["delivered_cents"] = delivered_cents
            if order["status"] == "cancelled":
                # 未取货即取消：承诺整单解除，不产生应收，也不计退款。
                result["qty_outstanding"] = 0
                result["amount_outstanding_cents"] = 0
                result["qty_released"] = order["total_qty"]
                result["released_cents"] = order["total_cents"]
            else:
                # 数量一致性：承诺 = 已送达 + 已退款 + 未决；金额同理。
                result["qty_outstanding"] = order["total_qty"] - sum(
                    line["qty_delivered"] + line["qty_refunded"] for line in lines)
                result["amount_outstanding_cents"] = (
                    order["total_cents"] - order["refunded_cents"] - delivered_cents)
            return result

    def batch_snapshot(self, store_id: str) -> list[dict[str, Any]]:
        with self.database.reading() as connection:
            rows = connection.execute(
                "SELECT * FROM store_batches WHERE store_id=? ORDER BY product_id,lot_no",
                (store_id,),
            ).fetchall()
            return [{"batch_id": r["batch_id"], "product_id": r["product_id"],
                     "lot_no": r["lot_no"], "qty_on_hand": r["qty_on_hand"],
                     "qty_held": r["qty_held"], "qty_picked": r["qty_picked"],
                     "qty_sold": r["qty_sold"],
                     "available": r["qty_on_hand"] - r["qty_held"]
                     - r["qty_picked"] - r["qty_sold"],
                     "cold_chain_ok": bool(r["cold_chain_ok"]),
                     "expiry_date": r["expiry_date"], "active": bool(r["active"]),
                     "version": r["version"]} for r in rows]
