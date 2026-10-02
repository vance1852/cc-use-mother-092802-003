"""夜间履约规则的单元测试：报价解释、占用、结算与恢复。"""

import unittest
from datetime import datetime, timezone

from beverage_ops_foundation.clock import MutableClock
from beverage_ops_foundation.errors import (
    ConflictError,
    InsufficientCapacityError,
    OrderStateError,
    QuoteExpiredError,
    ValidationError,
)
from beverage_ops_foundation.fulfillment import FulfillmentService
from beverage_ops_foundation.service import DomainService
from beverage_ops_foundation.storage import Database

BASE = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


class FulfillmentFixture(unittest.TestCase):
    """搭建夜间闪购渠道的标准目录。"""

    quote_ttl = 120

    def setUp(self) -> None:
        self.database = Database()
        self.clock = MutableClock(BASE)
        self.base = DomainService(self.database, self.clock)
        self.fs = FulfillmentService(self.database, self.clock,
                                     quote_ttl_seconds=self.quote_ttl)
        self.base.register_organization(request_id="org", actor_id="bootstrap",
                                        organization_id="o1", name="集团")
        self.base.register_actor(request_id="adm", actor_id="bootstrap", new_actor_id="a1",
                                 display_name="管理员", role="admin", organization_id="o1")
        self.base.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                 display_name="运营", role="operator", organization_id="o1")
        self.fs.register_channel(request_id="ch", actor_id="op1", channel_id="night",
                                 name="夜间闪购")
        self.fs.register_channel(request_id="ch2", actor_id="op1", channel_id="wholesale",
                                 name="批发渠道")
        for pid, name, price in (("beer-a", "高端A 500ml", 1500),
                                 ("beer-b", "高端B 330ml", 1200),
                                 ("beer-c", "高端C 罐装", 1100)):
            self.fs.register_product(request_id=f"prod-{pid}", actor_id="op1", product_id=pid,
                                     brand_id="brand-x", spec_name=name, tier="premium",
                                     requires_cold=True, list_price_cents=price)
        self.fs.register_store(request_id="st1", actor_id="op1", store_id="store-1",
                               organization_id="o1", name="城东店", region_id="east")
        self.fs.register_store(request_id="st2", actor_id="op1", store_id="store-2",
                               organization_id="o1", name="城西店", region_id="west")
        self.fs.register_courier(request_id="co1", actor_id="op1", courier_id="cou-1",
                                 name="骑手甲", region_id="east", capacity_units=4)
        self.fs.register_courier(request_id="co2", actor_id="op1", courier_id="cou-2",
                                 name="骑手乙", region_id="east", capacity_units=4)
        self.fs.set_brand_channel_rule(request_id="bc-night", actor_id="op1",
                                       brand_id="brand-x", channel_id="night", allowed=True)
        self.fs.set_brand_region_rule(request_id="br-east", actor_id="op1",
                                      brand_id="brand-x", region_id="east", allowed=True)
        self.fs.set_substitution_rule(request_id="sub-ab", actor_id="op1", product_id="beer-a",
                                      channel_id="night", substitute_product_id="beer-b",
                                      priority=1)
        self.fs.set_substitution_rule(request_id="sub-ac", actor_id="op1", product_id="beer-a",
                                      channel_id="night", substitute_product_id="beer-c",
                                      priority=2)
        self.fs.set_substitution_rule(request_id="sub-ba-ws", actor_id="op1",
                                      product_id="beer-a", channel_id="wholesale",
                                      substitute_product_id="beer-c", priority=1)

    def tearDown(self) -> None:
        self.database.close()

    def batch(self, request_id: str, batch_id: str, store: str, product: str, lot: str,
              qty: int, *, cold: bool = True, expiry: str = "2026-11-01",
              near_days: int = 3, price: int = 1500, tags: list[str] | None = None,
              active: bool = True) -> None:
        self.fs.upsert_batch(request_id=request_id, actor_id="op1", batch_id=batch_id,
                             store_id=store, product_id=product, lot_no=lot,
                             qty_on_hand=qty, cold_chain_ok=cold, expiry_date=expiry,
                             near_expiry_days=near_days, unit_price_cents=price,
                             channel_tags=tags, active=active)

    def quote(self, request_id: str, items, region: str = "east", channel: str = "night"):
        return self.fs.quote(request_id=request_id, actor_id="op1", channel_id=channel,
                            region_id=region, customer_id="cust-1", items=items)


class QuoteExplanationTest(FulfillmentFixture):
    def test_feasible_quote_carries_batches_and_expiry(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 3)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 2}])
        self.assertTrue(quote["feasible"])
        self.assertEqual("offered", quote["status"])
        line = quote["lines"][0]
        self.assertEqual(2, line["qty_matched"])
        self.assertEqual(0, line["shortfall"])
        self.assertEqual("lot-1", line["batches"][0]["batch_id"])
        self.assertEqual(3000, quote["total_cents"])
        self.assertIn("expires_at", quote)

    def test_cold_chain_broken_batch_is_excluded_with_reason(self):
        self.batch("b1", "lot-ok", "store-1", "beer-a", "L1", 2, cold=True)
        self.batch("b2", "lot-warm", "store-1", "beer-a", "L2", 2, cold=False)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 3}])
        self.assertFalse(quote["feasible"])
        line = quote["lines"][0]
        self.assertEqual(2, line["qty_matched"])
        excluded = {e["batch_id"]: e["reasons"] for e in line["excluded_batches"]}
        self.assertIn("cold_chain_broken", excluded["lot-warm"])
        self.assertNotIn("lot-ok", excluded)

    def test_near_expiry_batch_is_excluded(self):
        self.clock.advance(seconds=0)
        self.batch("b1", "lot-fresh", "store-1", "beer-a", "L1", 2,
                   expiry="2026-10-20", near_days=3)
        self.batch("b2", "lot-near", "store-1", "beer-a", "L2", 2,
                   expiry="2026-10-04", near_days=3)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 3}])
        excluded = {e["batch_id"]: e["reasons"] for e in quote["lines"][0]["excluded_batches"]}
        self.assertIn("near_expiry", excluded["lot-near"])
        self.assertNotIn("lot-fresh", excluded)

    def test_channel_tagged_batch_hidden_from_other_channel(self):
        self.batch("b1", "lot-night", "store-1", "beer-a", "L1", 2, tags=["night"])
        self.batch("b2", "lot-ws", "store-1", "beer-a", "L2", 2, tags=["wholesale"])
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 3}])
        excluded = {e["batch_id"]: e["reasons"] for e in quote["lines"][0]["excluded_batches"]}
        self.assertIn("channel_not_allowed", excluded["lot-ws"])

    def test_brand_channel_rule_blocks_quote(self):
        self.fs.set_brand_channel_rule(request_id="bc-off", actor_id="op1",
                                       brand_id="brand-x", channel_id="night", allowed=False)
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 3)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 1}])
        self.assertFalse(quote["feasible"])
        reasons = quote["lines"][0]["excluded_batches"][0]["reasons"]
        self.assertIn("brand_channel_blocked", reasons)

    def test_region_rule_blocks_other_region(self):
        self.batch("b1", "lot-1", "store-2", "beer-a", "L1", 3)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 1}], region="west")
        self.assertFalse(quote["feasible"])
        reasons = quote["lines"][0]["excluded_batches"][0]["reasons"]
        self.assertIn("brand_region_blocked", reasons)

    def test_out_of_stock_reason_and_substitutes_only_when_allowed(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 1)
        self.batch("b2", "lot-2", "store-1", "beer-b", "L2", 5, price=1200)
        self.batch("b3", "lot-3", "store-1", "beer-c", "L3", 5, price=1100)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 4}])
        line = quote["lines"][0]
        self.assertEqual(1, line["qty_matched"])
        sub_ids = [s["product_id"] for s in line["substitutes"]]
        # 优先级顺序：beer-b 先于 beer-c
        self.assertEqual(["beer-b", "beer-c"], sub_ids)

    def test_substitute_not_recommended_for_channel_without_rule(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 1)
        self.batch("b3", "lot-3", "store-1", "beer-c", "L3", 5, price=1100)
        self.fs.set_brand_channel_rule(request_id="bc-ws", actor_id="op1",
                                       brand_id="brand-x", channel_id="wholesale",
                                       allowed=True)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 4}], channel="wholesale")
        sub_ids = [s["product_id"] for s in quote["lines"][0]["substitutes"]]
        # wholesale 渠道只配了 beer-c 作为替代；beer-b 规则仅属于 night
        self.assertEqual(["beer-c"], sub_ids)

    def test_substitute_with_cold_break_is_not_recommended(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 1)
        self.batch("b2", "lot-2", "store-1", "beer-b", "L2", 5, cold=False, price=1200)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 4}])
        sub_ids = [s["product_id"] for s in quote["lines"][0]["substitutes"]]
        self.assertNotIn("beer-b", sub_ids)

    def test_fefo_prefers_earlier_expiry(self):
        self.batch("b1", "lot-late", "store-1", "beer-a", "L1", 2,
                   expiry="2026-12-01", near_days=3)
        self.batch("b2", "lot-early", "store-1", "beer-a", "L2", 2,
                   expiry="2026-10-20", near_days=3)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 1}])
        self.assertEqual("lot-early", quote["lines"][0]["batches"][0]["batch_id"])


class ConfirmAndInventoryTest(FulfillmentFixture):
    def _prepare(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 2)
        self.batch("b2", "lot-2", "store-1", "beer-b", "L2", 5, price=1200)
        return self.quote("q1", [{"product_id": "beer-a", "qty": 5}])

    def test_confirm_atomically_holds_batches_and_courier_capacity(self):
        quote = self._prepare()
        order = self.fs.confirm_order(
            request_id="o1", actor_id="op1", quote_id=quote["quote_id"],
            substitutions=[{"for_product_id": "beer-a", "product_id": "beer-b", "qty": 3}])
        self.assertEqual("held", order["status"])
        snap = {s["batch_id"]: s for s in self.fs.batch_snapshot("store-1")}
        self.assertEqual(2, snap["lot-1"]["qty_held"])
        self.assertEqual(3, snap["lot-2"]["qty_held"])
        self.assertEqual(0, snap["lot-1"]["available"])
        self.assertEqual(2, snap["lot-2"]["available"])
        self.assertEqual(2, len(order["deliveries"]))

    def test_confirm_rejects_substitute_outside_quote_whitelist(self):
        quote = self._prepare()
        with self.assertRaises(ValidationError):
            self.fs.confirm_order(
                request_id="o1", actor_id="op1", quote_id=quote["quote_id"],
                substitutions=[{"for_product_id": "beer-a", "product_id": "beer-a", "qty": 1}])

    def test_second_confirm_after_stock_taken_fails_and_releases_nothing(self):
        quote1 = self._prepare()
        self.fs.confirm_order(
            request_id="o1", actor_id="op1", quote_id=quote1["quote_id"],
            substitutions=[{"for_product_id": "beer-a", "product_id": "beer-b", "qty": 3}])
        quote2 = self.quote("q2", [{"product_id": "beer-a", "qty": 5}])
        # lot-1 已被占满，替代规格 lot-2 仅剩 2 件，无法补齐 5 件缺口
        with self.assertRaises(InsufficientCapacityError):
            self.fs.confirm_order(
                request_id="o2", actor_id="op1", quote_id=quote2["quote_id"],
                substitutions=[{"for_product_id": "beer-a", "product_id": "beer-b", "qty": 2}])
        snap = {s["batch_id"]: s for s in self.fs.batch_snapshot("store-1")}
        self.assertEqual(2, snap["lot-2"]["available"])

    def test_expired_quote_cannot_be_confirmed(self):
        quote = self._prepare()
        self.clock.advance(seconds=self.quote_ttl + 1)
        with self.assertRaises(QuoteExpiredError):
            self.fs.confirm_order(
                request_id="o1", actor_id="op1", quote_id=quote["quote_id"],
                substitutions=[{"for_product_id": "beer-a", "product_id": "beer-b", "qty": 3}])

    def test_stock_decrease_below_obligation_rejected(self):
        quote = self._prepare()
        self.fs.confirm_order(
            request_id="o1", actor_id="op1", quote_id=quote["quote_id"],
            substitutions=[{"for_product_id": "beer-a", "product_id": "beer-b", "qty": 3}])
        with self.assertRaises(InsufficientCapacityError):
            self.batch("b2v", "lot-2", "store-1", "beer-b", "L2", 1, price=1200)

    def test_courier_capacity_blocks_confirm(self):
        self.batch("b2", "lot-2", "store-1", "beer-b", "L2", 20, price=1200)
        quote = self.quote("q1", [{"product_id": "beer-b", "qty": 9}])
        # 库存充足但两名骑手合计容量只有 8
        self.assertFalse(quote["feasible"])
        self.assertFalse(quote["courier"]["feasible"])
        with self.assertRaises(InsufficientCapacityError):
            self.fs.confirm_order(request_id="o1", actor_id="op1",
                                  quote_id=quote["quote_id"])


class IdempotencyTest(FulfillmentFixture):
    def test_retry_returns_original_result(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 3)
        q1 = self.quote("q-dup", [{"product_id": "beer-a", "qty": 2}])
        q2 = self.quote("q-dup", [{"product_id": "beer-a", "qty": 2}])
        self.assertFalse(q1["replayed"])
        self.assertTrue(q2["replayed"])
        self.assertEqual(q1["quote_id"], q2["quote_id"])

    def test_same_request_id_different_payload_conflicts(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 3)
        self.quote("q-dup", [{"product_id": "beer-a", "qty": 2}])
        with self.assertRaises(ConflictError):
            self.quote("q-dup", [{"product_id": "beer-a", "qty": 1}])

    def test_late_delivery_receipt_replays_original(self):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 3)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 3}])
        order = self.fs.confirm_order(request_id="o1", actor_id="op1",
                                      quote_id=quote["quote_id"])
        delivery_id = order["deliveries"][0]["delivery_id"]
        self.fs.mark_picked(request_id="pk", actor_id="op1", delivery_id=delivery_id)
        payload = [{"line_id": line["line_id"], "qty": line["qty"] - 1}
                   for line in order["lines"]]
        first = self.fs.complete_delivery(request_id="dn", actor_id="op1",
                                          delivery_id=delivery_id, delivered_items=payload)
        second = self.fs.complete_delivery(request_id="dn", actor_id="op1",
                                           delivery_id=delivery_id, delivered_items=payload)
        self.assertTrue(second["replayed"])
        self.assertEqual(first["refunded_cents_total"], second["refunded_cents_total"])
        self.assertEqual(first["order_status"], second["order_status"])
        detail = self.fs.get_order(order["order_id"])
        # 重放不能重复退款
        self.assertEqual(1500, detail["refunded_cents"])


class LifecycleTest(FulfillmentFixture):
    def _order(self, qty=3, substitute=0):
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", qty)
        if substitute:
            self.batch("b2", "lot-2", "store-1", "beer-b", "L2", 5, price=1200)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": qty + substitute}])
        subs = [{"for_product_id": "beer-a", "product_id": "beer-b", "qty": substitute}] \
            if substitute else None
        order = self.fs.confirm_order(request_id="o1", actor_id="op1",
                                      quote_id=quote["quote_id"], substitutions=subs)
        return order

    def test_cancel_before_pick_releases_inventory_and_capacity(self):
        order = self._order(qty=2)
        result = self.fs.cancel_order(request_id="cx", actor_id="op1",
                                      order_id=order["order_id"])
        self.assertEqual("cancelled", result["status"])
        self.assertEqual(2, result["released_qty"])
        snap = self.fs.batch_snapshot("store-1")[0]
        self.assertEqual(0, snap["qty_held"])
        self.assertEqual(2, snap["available"])
        detail = self.fs.get_order(order["order_id"])
        self.assertEqual("cancelled", detail["status"])

    def test_cancel_after_pick_rejected(self):
        order = self._order(qty=2)
        self.fs.mark_picked(request_id="pk", actor_id="op1",
                            delivery_id=order["deliveries"][0]["delivery_id"])
        with self.assertRaises(OrderStateError):
            self.fs.cancel_order(request_id="cx", actor_id="op1",
                                 order_id=order["order_id"])

    def test_timeout_releases_held_order_but_protects_picked(self):
        held = self._order(qty=2)
        self._build_second_order()
        self.clock.advance(seconds=self.quote_ttl + 1)
        result = self.fs.reconcile()
        self.assertIn(held["order_id"], result["expired_orders"])
        self.assertEqual(1, result["picked_orders_protected"])
        detail = self.fs.get_order(held["order_id"])
        self.assertEqual("cancelled", detail["status"])
        snap = self.fs.batch_snapshot("store-1")
        by_lot = {s["lot_no"]: s for s in snap}
        # held 订单的 2 件占位被释放；已取货订单的 2 件保持 picked
        self.assertEqual(0, by_lot["L1"]["qty_held"])
        self.assertEqual(2, by_lot["L9"]["qty_picked"])

    def _build_second_order(self):
        self.batch("b3", "lot-9", "store-1", "beer-a", "L9", 2)
        quote = self.quote("q9", [{"product_id": "beer-a", "qty": 2}])
        order = self.fs.confirm_order(request_id="o9", actor_id="op1",
                                      quote_id=quote["quote_id"])
        self.fs.mark_picked(request_id="pk9", actor_id="op1",
                            delivery_id=order["deliveries"][0]["delivery_id"])
        return order

    def test_batch_data_update_invalidates_held_order(self):
        order = self._order(qty=2)
        # 门店上报冷链失效
        self.fs.upsert_batch(request_id="b1cold", actor_id="op1", batch_id="lot-1",
                             store_id="store-1", product_id="beer-a", lot_no="L1",
                             qty_on_hand=2, cold_chain_ok=False, expiry_date="2026-11-01",
                             near_expiry_days=3, unit_price_cents=1500)
        result = self.fs.reconcile()
        self.assertIn(order["order_id"], result["invalidated_orders"])
        detail = self.fs.get_order(order["order_id"])
        self.assertEqual("cancelled", detail["status"])

    def test_pickup_blocked_when_batch_became_cold_broken(self):
        order = self._order(qty=2)
        self.fs.upsert_batch(request_id="b1cold", actor_id="op1", batch_id="lot-1",
                             store_id="store-1", product_id="beer-a", lot_no="L1",
                             qty_on_hand=2, cold_chain_ok=False, expiry_date="2026-11-01",
                             near_expiry_days=3, unit_price_cents=1500)
        with self.assertRaises(OrderStateError):
            self.fs.mark_picked(request_id="pk", actor_id="op1",
                                delivery_id=order["deliveries"][0]["delivery_id"])

    def test_partial_delivery_refund_keeps_qty_and_amount_consistent(self):
        order = self._order(qty=3)
        delivery_id = order["deliveries"][0]["delivery_id"]
        self.fs.mark_picked(request_id="pk", actor_id="op1", delivery_id=delivery_id)
        payload = [{"line_id": order["lines"][0]["line_id"], "qty": 2}]
        result = self.fs.complete_delivery(request_id="dn", actor_id="op1",
                                           delivery_id=delivery_id,
                                           delivered_items=payload)
        self.assertEqual("partial", result["order_status"])
        detail = self.fs.get_order(order["order_id"])
        self.assertEqual(3, detail["total_qty"])
        self.assertEqual(0, detail["qty_outstanding"])
        self.assertEqual(0, detail["amount_outstanding_cents"])
        self.assertEqual(1500, detail["refunded_cents"])
        self.assertEqual(3000, detail["delivered_cents"])
        self.assertEqual(4500, detail["total_cents"])

    def test_full_delivery_finalizes_delivered_status(self):
        order = self._order(qty=2)
        delivery_id = order["deliveries"][0]["delivery_id"]
        self.fs.mark_picked(request_id="pk", actor_id="op1", delivery_id=delivery_id)
        line_id = order["lines"][0]["line_id"]
        result = self.fs.complete_delivery(
            request_id="dn", actor_id="op1", delivery_id=delivery_id,
            delivered_items=[{"line_id": line_id, "qty": 2}])
        self.assertEqual("delivered", result["order_status"])
        self.assertEqual(0, result["refunded_cents_total"])

    def test_over_delivery_rejected(self):
        order = self._order(qty=2)
        delivery_id = order["deliveries"][0]["delivery_id"]
        self.fs.mark_picked(request_id="pk", actor_id="op1", delivery_id=delivery_id)
        with self.assertRaises(ValidationError):
            self.fs.complete_delivery(
                request_id="dn", actor_id="op1", delivery_id=delivery_id,
                delivered_items=[{"line_id": order["lines"][0]["line_id"], "qty": 3}])

    def test_split_delivery_both_completed_reconciles_totals(self):
        # 5 件拆到两个骑手（容量各 4）
        self.batch("b1", "lot-1", "store-1", "beer-a", "L1", 5)
        quote = self.quote("q1", [{"product_id": "beer-a", "qty": 5}])
        order = self.fs.confirm_order(request_id="o1", actor_id="op1",
                                      quote_id=quote["quote_id"])
        self.assertEqual(2, len(order["deliveries"]))
        detail0 = self.fs.get_order(order["order_id"])
        for index, delivery in enumerate(detail0["deliveries"]):
            self.fs.mark_picked(request_id=f"pk{index}", actor_id="op1",
                                delivery_id=delivery["delivery_id"])
        line_id = order["lines"][0]["line_id"]
        d0, d1 = detail0["deliveries"]
        self.fs.complete_delivery(
            request_id="dn0", actor_id="op1", delivery_id=d0["delivery_id"],
            delivered_items=[{"line_id": line_id, "qty": d0["units"]}])
        result1 = self.fs.complete_delivery(
            request_id="dn1", actor_id="op1", delivery_id=d1["delivery_id"],
            delivered_items=[{"line_id": line_id, "qty": d1["units"] - 1}])
        self.assertEqual("partial", result1["order_status"])
        detail = self.fs.get_order(order["order_id"])
        self.assertEqual(0, detail["qty_outstanding"])
        self.assertEqual(1500, detail["refunded_cents"])
        self.assertEqual(0, detail["amount_outstanding_cents"])
        # 全部送达后运力释放，可以再下一单 4 件
        self.batch("b2", "lot-2", "store-1", "beer-a", "L2", 4)
        quote2 = self.quote("q2", [{"product_id": "beer-a", "qty": 4}])
        self.assertTrue(quote2["feasible"])


if __name__ == "__main__":
    unittest.main()
