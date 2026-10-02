"""高峰并发、迟到回执、门店数据更新与进程重启恢复的验收测试。"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from beverage_ops_foundation.clock import MutableClock
from beverage_ops_foundation.fulfillment import FulfillmentService
from beverage_ops_foundation.maintenance import ReconcileLoop
from beverage_ops_foundation.service import DomainService
from beverage_ops_foundation.storage import Database

BASE = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


def build_world(database: Database, clock: MutableClock, *, couriers=6) -> tuple[DomainService, FulfillmentService]:
    base = DomainService(database, clock)
    fs = FulfillmentService(database, clock)
    base.register_organization(request_id="org", actor_id="bootstrap",
                               organization_id="o1", name="集团")
    base.register_actor(request_id="adm", actor_id="bootstrap", new_actor_id="a1",
                        display_name="管理员", role="admin", organization_id="o1")
    base.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                        display_name="运营", role="operator", organization_id="o1")
    fs.register_channel(request_id="ch", actor_id="op1", channel_id="night", name="夜间闪购")
    fs.register_product(request_id="pr-a", actor_id="op1", product_id="beer-a",
                        brand_id="brand-x", spec_name="高端A", tier="premium",
                        requires_cold=True, list_price_cents=1500)
    fs.register_product(request_id="pr-b", actor_id="op1", product_id="beer-b",
                        brand_id="brand-x", spec_name="高端B", tier="premium",
                        requires_cold=True, list_price_cents=1200)
    fs.register_store(request_id="st", actor_id="op1", store_id="store-1",
                      organization_id="o1", name="城东店", region_id="east")
    for index in range(couriers):
        fs.register_courier(request_id=f"co{index}", actor_id="op1",
                            courier_id=f"cou-{index}", name=f"骑手{index}",
                            region_id="east", capacity_units=50)
    fs.set_brand_channel_rule(request_id="bc", actor_id="op1", brand_id="brand-x",
                              channel_id="night", allowed=True)
    fs.set_brand_region_rule(request_id="br", actor_id="op1", brand_id="brand-x",
                             region_id="east", allowed=True)
    fs.set_substitution_rule(request_id="sub", actor_id="op1", product_id="beer-a",
                             channel_id="night", substitute_product_id="beer-b", priority=1)
    return base, fs


def seed_batches(fs: FulfillmentService) -> None:
    fs.upsert_batch(request_id="ba", actor_id="op1", batch_id="lot-a", store_id="store-1",
                    product_id="beer-a", lot_no="LA", qty_on_hand=40, cold_chain_ok=True,
                    expiry_date="2026-11-01", near_expiry_days=3, unit_price_cents=1500)
    fs.upsert_batch(request_id="bb", actor_id="op1", batch_id="lot-b", store_id="store-1",
                    product_id="beer-b", lot_no="LB", qty_on_hand=200, cold_chain_ok=True,
                    expiry_date="2026-11-05", near_expiry_days=3, unit_price_cents=1200)


class PeakConcurrencyTest(unittest.TestCase):
    def test_parallel_confirms_never_oversell_batches_or_capacity(self):
        database = Database()
        clock = MutableClock(BASE)
        _, fs = build_world(database, clock)
        seed_batches(fs)

        # 100 个消费者各抢购 1 件 beer-a；现货只有 40 件，其余用 beer-b 替代。
        # 报价不占库，高峰期必须按平台重试语义重新询价后再确认。
        outcome_lock = __import__("threading").Lock()
        outcomes: list[str] = []

        def worker(index: int) -> None:
            for attempt in range(10):
                try:
                    quote = fs.quote(
                        request_id=f"q-{index}-{attempt}", actor_id="op1",
                        channel_id="night", region_id="east",
                        customer_id=f"cust-{index}",
                        items=[{"product_id": "beer-a", "qty": 1}])
                    if quote["feasible"]:
                        fs.confirm_order(request_id=f"o-{index}-{attempt}", actor_id="op1",
                                         quote_id=quote["quote_id"])
                        kind = "primary"
                    else:
                        shortfall = quote["lines"][0]["shortfall"]
                        sub = next((s for s in quote["lines"][0]["substitutes"]
                                    if s["product_id"] == "beer-b"), None)
                        if sub is None or sub["available_qty"] < shortfall:
                            with outcome_lock:
                                outcomes.append("rejected")
                            return
                        fs.confirm_order(
                            request_id=f"o-{index}-{attempt}", actor_id="op1",
                            quote_id=quote["quote_id"],
                            substitutions=[{"for_product_id": "beer-a",
                                            "product_id": "beer-b",
                                            "qty": shortfall}])
                        kind = "substitute"
                    with outcome_lock:
                        outcomes.append(kind)
                    return
                except Exception:  # noqa: BLE001 - 高峰下重试
                    continue
            with outcome_lock:
                outcomes.append("rejected")

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(worker, range(100)))

        snap = {s["batch_id"]: s for s in fs.batch_snapshot("store-1")}
        # 库存守恒：held+picked+sold 永远不超过 on_hand
        for batch_id in ("lot-a", "lot-b"):
            s = snap[batch_id]
            self.assertGreaterEqual(s["qty_on_hand"],
                                    s["qty_held"] + s["qty_picked"] + s["qty_sold"])
            self.assertGreaterEqual(s["available"], 0)
        # beer-a 40 件 + beer-b 60 件，全部成交且不超卖
        self.assertEqual(100, len([o for o in outcomes if o != "rejected"]))
        self.assertEqual(0, outcomes.count("rejected"))
        self.assertEqual(40, snap["lot-a"]["qty_held"])
        self.assertEqual(60, snap["lot-b"]["qty_held"])

        # 运力守恒：每个骑手在途单位不超过 50
        rows = database.connection.execute(
            "SELECT c.courier_id, c.capacity_units, COALESCE(SUM(d.units),0) AS used "
            "FROM couriers c LEFT JOIN deliveries d ON d.courier_id=c.courier_id "
            "AND d.status IN ('planned','picked') GROUP BY c.courier_id").fetchall()
        for row in rows:
            self.assertLessEqual(row["used"], row["capacity_units"])

        database.close()

    def test_parallel_confirms_beyond_stock_partial_reject(self):
        database = Database()
        clock = MutableClock(BASE)
        _, fs = build_world(database, clock)
        seed_batches(fs)

        def worker(index: int):
            for attempt in range(10):
                try:
                    quote = fs.quote(
                        request_id=f"q-{index}-{attempt}", actor_id="op1",
                        channel_id="night", region_id="east",
                        customer_id=f"cust-{index}",
                        items=[{"product_id": "beer-a", "qty": 5}])
                    if quote["feasible"]:
                        fs.confirm_order(request_id=f"o-{index}-{attempt}", actor_id="op1",
                                         quote_id=quote["quote_id"])
                        return "ok"
                    sub_qty = quote["lines"][0]["shortfall"]
                    sub = next((s for s in quote["lines"][0]["substitutes"]
                                if s["product_id"] == "beer-b"), None)
                    if sub is None or sub["available_qty"] < sub_qty:
                        return "rejected"
                    fs.confirm_order(
                        request_id=f"o-{index}-{attempt}", actor_id="op1",
                        quote_id=quote["quote_id"],
                        substitutions=[{"for_product_id": "beer-a",
                                        "product_id": "beer-b", "qty": sub_qty}])
                    return "ok-sub"
                except Exception:  # noqa: BLE001 - 库存被其他线程抢先，重试或放弃
                    continue
            return "rejected"

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=20) as pool:
            outcomes = list(pool.map(worker, range(80)))

        accepted = outcomes.count("ok") + outcomes.count("ok-sub")
        snap = {s["batch_id"]: s for s in fs.batch_snapshot("store-1")}
        committed = snap["lot-a"]["qty_held"] + snap["lot-b"]["qty_held"]
        # 总可售 40+200=240 件，80*5=400 需求 -> 只有 48 单成功
        self.assertEqual(48, accepted)
        self.assertEqual(240, committed)
        self.assertEqual(0, snap["lot-a"]["available"])
        self.assertEqual(0, snap["lot-b"]["available"])
        database.close()


class LateReceiptConcurrencyTest(unittest.TestCase):
    def test_duplicate_late_receipts_refund_exactly_once(self):
        database = Database()
        clock = MutableClock(BASE)
        _, fs = build_world(database, clock)
        seed_batches(fs)
        quote = fs.quote(request_id="q1", actor_id="op1", channel_id="night",
                         region_id="east", customer_id="c1",
                         items=[{"product_id": "beer-a", "qty": 3}])
        order = fs.confirm_order(request_id="o1", actor_id="op1",
                                 quote_id=quote["quote_id"])
        delivery_id = order["deliveries"][0]["delivery_id"]
        line_id = order["lines"][0]["line_id"]
        fs.mark_picked(request_id="pk", actor_id="op1", delivery_id=delivery_id)
        payload = [{"line_id": line_id, "qty": 2}]

        from concurrent.futures import ThreadPoolExecutor

        def worker():
            return fs.complete_delivery(request_id="late-dn", actor_id="op1",
                                        delivery_id=delivery_id,
                                        delivered_items=payload)

        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda _: worker(), range(8)))
        # 只有一个首次执行，其余全部回放同一结果
        first_runs = [r for r in responses if not r["replayed"]]
        replays = [r for r in responses if r["replayed"]]
        self.assertEqual(1, len(first_runs))
        self.assertEqual(7, len(replays))
        for response in responses:
            self.assertEqual("partial", response["order_status"])
            self.assertEqual(1500, response["refunded_cents_total"])
        detail = fs.get_order(order["order_id"])
        self.assertEqual(1500, detail["refunded_cents"])
        self.assertEqual(2, detail["lines"][0]["qty_delivered"])
        self.assertEqual(1, detail["lines"][0]["qty_refunded"])
        database.close()


class StoreDataUpdateTest(unittest.TestCase):
    def test_concurrent_stock_update_cannot_violate_hold(self):
        database = Database()
        clock = MutableClock(BASE)
        _, fs = build_world(database, clock)
        seed_batches(fs)
        quote = fs.quote(request_id="q1", actor_id="op1", channel_id="night",
                         region_id="east", customer_id="c1",
                         items=[{"product_id": "beer-a", "qty": 10}])
        fs.confirm_order(request_id="o1", actor_id="op1", quote_id=quote["quote_id"])
        # 门店试图把现货下调到低于已占用的 10 件
        from beverage_ops_foundation.errors import InsufficientCapacityError

        with self.assertRaises(InsufficientCapacityError):
            fs.upsert_batch(request_id="ba-low", actor_id="op1", batch_id="lot-a",
                            store_id="store-1", product_id="beer-a", lot_no="LA",
                            qty_on_hand=5, cold_chain_ok=True, expiry_date="2026-11-01",
                            near_expiry_days=3, unit_price_cents=1500)
        # 增补库存是允许的
        fs.upsert_batch(request_id="ba-more", actor_id="op1", batch_id="lot-a",
                        store_id="store-1", product_id="beer-a", lot_no="LA",
                        qty_on_hand=60, cold_chain_ok=True, expiry_date="2026-11-01",
                        near_expiry_days=3, unit_price_cents=1500)
        snap = {s["batch_id"]: s for s in fs.batch_snapshot("store-1")}
        self.assertEqual(60, snap["lot-a"]["qty_on_hand"])
        self.assertEqual(10, snap["lot-a"]["qty_held"])
        self.assertEqual(50, snap["lot-a"]["available"])
        database.close()


class RestartRecoveryTest(unittest.TestCase):
    def test_pending_orders_survive_restart_and_expired_holds_release(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "night.sqlite3"
            clock = MutableClock(BASE)

            database = Database(db_path)
            _, fs = build_world(database, clock)
            seed_batches(fs)
            # 即将到期的 held 订单
            q_held = fs.quote(request_id="q-held", actor_id="op1", channel_id="night",
                              region_id="east", customer_id="c1",
                              items=[{"product_id": "beer-a", "qty": 2}])
            held_order = fs.confirm_order(request_id="o-held", actor_id="op1",
                                          quote_id=q_held["quote_id"])
            # 已取货订单
            q_pick = fs.quote(request_id="q-pick", actor_id="op1", channel_id="night",
                              region_id="east", customer_id="c2",
                              items=[{"product_id": "beer-a", "qty": 3}])
            picked_order = fs.confirm_order(request_id="o-pick", actor_id="op1",
                                            quote_id=q_pick["quote_id"])
            picked_delivery = picked_order["deliveries"][0]["delivery_id"]
            fs.mark_picked(request_id="pk-pick", actor_id="op1",
                           delivery_id=picked_delivery)
            audit_before = database.connection.execute(
                "SELECT COUNT(*) AS c FROM audit_events").fetchone()["c"]
            database.close()

            # 模拟进程重启：时钟推进超过报价有效期后重新打开数据库
            clock.advance(seconds=300)
            database2 = Database(db_path)
            fs2 = FulfillmentService(database2, clock)
            recovery = ReconcileLoop(fs2).startup_recovery()
            self.assertIn(held_order["order_id"], recovery["expired_orders"])
            self.assertEqual(1, recovery["picked_orders_protected"])

            held_detail = fs2.get_order(held_order["order_id"])
            picked_detail = fs2.get_order(picked_order["order_id"])
            self.assertEqual("cancelled", held_detail["status"])
            self.assertEqual("picked", picked_detail["status"])
            # 已取货的 3 件仍保留在 qty_picked，未被超时释放
            self.assertEqual(3, sum(line["qty_committed"] - line["qty_delivered"]
                                    - line["qty_refunded"] for line in picked_detail["lines"]))

            # 重启后迟到的送达回执仍然可以正常入账
            line_id = picked_detail["lines"][0]["line_id"]
            result = fs2.complete_delivery(
                request_id="dn-restart", actor_id="op1",
                delivery_id=picked_delivery,
                delivered_items=[{"line_id": line_id, "qty": 3}])
            self.assertEqual("delivered", result["order_status"])
            final = fs2.get_order(picked_order["order_id"])
            self.assertEqual(0, final["qty_outstanding"])

            # 审计链在重启后依然连续
            from beverage_ops_foundation.audit import verify_chain

            valid, count = verify_chain(database2.connection)
            self.assertTrue(valid)
            self.assertGreaterEqual(count, audit_before)
            database2.close()


if __name__ == "__main__":
    unittest.main()
