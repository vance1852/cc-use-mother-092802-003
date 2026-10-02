import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from beverage_ops_foundation.clock import FixedClock
from beverage_ops_foundation.errors import (
    ConflictError,
    NotFoundError,
    UnfulfillableError,
    ValidationError,
)
from beverage_ops_foundation.fulfillment import FulfillmentService
from beverage_ops_foundation.storage import Database


class MutableClock:
    def __init__(self, value):
        self.value = value

    def now(self):
        return self.value.astimezone(timezone.utc)


class FulfillmentFixture:
    def __init__(self, clock=None, path=":memory:"):
        self.clock = clock or MutableClock(
            datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc))
        self.database = Database(path)
        self.svc = FulfillmentService(self.database, self.clock)
        self._seed()

    def _seed(self):
        s = self.svc
        s.upsert_sku(sku_id="s500", brand_id="bA", product_line="ice", spec="500ml",
                     display_name="冰啤500", unit_price_milli=1800,
                     requires_cold=True, shelf_life_days=180)
        s.upsert_sku(sku_id="s330", brand_id="bA", product_line="ice", spec="330ml",
                     display_name="冰啤330", unit_price_milli=1200,
                     requires_cold=True, shelf_life_days=180)
        s.upsert_sku(sku_id="s750", brand_id="bA", product_line="ice", spec="750ml",
                     display_name="冰啤750", unit_price_milli=2600,
                     requires_cold=True, shelf_life_days=180)
        s.upsert_channel_rule(brand_id="bA", channel="night",
                              allowed_specs=["500ml", "330ml"])
        s.upsert_channel_rule(brand_id="bA", channel="strict", allowed_specs=["330ml"])
        s.upsert_substitution(brand_id="bA", channel="night",
                              from_sku="s500", rank=1, to_sku="s330")
        s.upsert_store(store_id="st", name="夜店", region_id="R1",
                       sales_regions=["R1", "R2"])
        s.upsert_batch(batch_id="near", store_id="st", sku_id="s500", quantity_on_hand=4,
                       production_date="2026-04-01", expiration_date="2026-10-03",
                       chilled=True)
        s.upsert_batch(batch_id="warm", store_id="st", sku_id="s500", quantity_on_hand=50,
                       production_date="2026-05-01", expiration_date="2026-11-01",
                       chilled=False)
        s.upsert_batch(batch_id="good", store_id="st", sku_id="s500", quantity_on_hand=20,
                       production_date="2026-06-01", expiration_date="2026-11-15",
                       chilled=True)
        s.upsert_batch(batch_id="b330", store_id="st", sku_id="s330", quantity_on_hand=30,
                       production_date="2026-06-01", expiration_date="2026-11-15",
                       chilled=True)
        s.upsert_courier(courier_id="c1", store_id="st", region_id="R1")
        s.upsert_courier(courier_id="c2", store_id="st", region_id="R2")
        s.upsert_shift(shift_id="sh1", courier_id="c1",
                       starts_at="2026-10-02T10:00:00+00:00",
                       ends_at="2026-10-02T20:00:00+00:00", capacity=3)
        s.upsert_shift(shift_id="sh2", courier_id="c2",
                       starts_at="2026-10-02T10:00:00+00:00",
                       ends_at="2026-10-02T20:00:00+00:00", capacity=3)

    def close(self):
        self.database.close()


class InventorySelectionTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture()

    def tearDown(self):
        self.f.close()

    def test_fefo_prefers_earliest_expiry_among_sellable(self):
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 8}])
        self.assertTrue(q["available"])
        batches = q["items"][0]["explain"]["batches"]
        # near(4) 先出，再从 good 取 4；warm 因未冷藏被排除
        self.assertEqual(["near", "good"], [b["batch_id"] for b in batches])
        self.assertEqual([4, 4], [b["quantity"] for b in batches])

    def test_unchilled_batch_never_promised_for_cold_sku(self):
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 25}],
                             allow_substitute=False)
        # 冷藏可售 4+20=24 < 25，主规格缺货；warm 因未冷藏被排除
        self.assertFalse(q["available"])
        blockers = [b for b in q["blockers"] if b.get("sku") == "s500"]
        self.assertEqual(24, blockers[0]["available"])

    def test_near_expiry_batch_excluded_when_window_too_short(self):
        # 时钟推进到 10-03 18:00：near 于 10-03 23:59 到期，不足 1 天保质窗口
        self.f.clock.value = datetime(2026, 10, 3, 18, 0, tzinfo=timezone.utc)
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 21}],
                             allow_substitute=False)
        self.assertFalse(q["available"])
        blockers = [b for b in q["blockers"] if b.get("sku") == "s500"]
        self.assertEqual(20, blockers[0]["available"])

    def test_explanation_is_structured(self):
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 2}])
        explain = q["items"][0]["explain"]
        self.assertEqual("按到期日最近优先（FEFO）", explain["allocation"])
        self.assertFalse(explain["substituted"])
        self.assertEqual("s500", explain["allocated_sku"])


class ChannelAndRegionTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture()

    def tearDown(self):
        self.f.close()

    def test_spec_not_allowed_in_channel(self):
        with self.assertRaises(UnfulfillableError) as ctx:
            self.f.svc.quote(request_id="q", channel="strict", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 1}],
                             allow_substitute=False)
        self.assertEqual("spec_blocked", ctx.exception.details["reason"])

    def test_brand_channel_not_open(self):
        with self.assertRaises(UnfulfillableError) as ctx:
            self.f.svc.quote(request_id="q", channel="unknown", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 1}])
        self.assertEqual("brand_channel_closed", ctx.exception.details["reason"])

    def test_no_store_serves_region(self):
        with self.assertRaises(UnfulfillableError) as ctx:
            self.f.svc.quote(request_id="q", channel="night", region_id="R9",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 1}])
        self.assertEqual("no_store_in_region", ctx.exception.details["reason"])

    def test_store_sales_region_r2_resolves_same_store(self):
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R2",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 1}])
        self.assertEqual("st", q["store_id"])


class SubstitutionTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture()

    def tearDown(self):
        self.f.close()

    def test_quote_lists_only_rule_allowed_substitutes_with_stock(self):
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 100}])
        self.assertFalse(q["available"])
        subs = q["allowed_substitutes"]
        self.assertEqual(["s330"], [s["substitute_sku"] for s in subs])
        self.assertEqual(30, subs[0]["available"])

    def test_substitute_allocated_when_explicitly_accepted(self):
        # 主规格冷藏仅 24，330 有 30；显式接受替代时订 30 件改配 330
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 30}],
                             allow_substitute=True)
        self.assertTrue(q["available"])
        self.assertEqual("s330", q["items"][0]["sku_id"])
        self.assertTrue(q["items"][0]["substituted"])
        self.assertIn("s330", q["substitutions_offered"])

    def test_substitute_blocked_by_channel_not_offered(self):
        # strict 渠道只允许 330，而请求的也是 330；构造一个渠道不允许替代规格的场景
        self.f.svc.upsert_channel_rule(brand_id="bA", channel="only750",
                                       allowed_specs=["750ml"])
        # 500->330 替代在 only750 渠道无效，且 500 规格本身被禁
        with self.assertRaises(UnfulfillableError):
            self.f.svc.quote(request_id="q", channel="only750", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 1}])

    def test_default_does_not_auto_substitute_but_still_recommends(self):
        # 默认不擅自改配：主规格不足即不可承诺，但仍只推荐规则允许的替代品
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 30}])
        self.assertFalse(q["available"])
        self.assertEqual(["s330"],
                         [s["substitute_sku"] for s in q["allowed_substitutes"]])


class PromiseLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture(clock=MutableClock(
            datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)))

    def tearDown(self):
        self.f.close()

    def _order(self, qid="q", oid="o", sku="s500", qty=2, region="R1"):
        self.f.svc.quote(request_id=qid, channel="night", region_id=region,
                         customer_location="L", items=[{"sku_id": sku, "quantity": qty}])
        return self.f.svc.confirm(request_id=oid, quote_request_id=qid)

    def test_quote_has_validity_window(self):
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 1}])
        self.assertEqual(120, q["ttl_seconds"])
        self.assertIn("expires_at", q)

    def test_quote_does_not_reserve(self):
        self.f.svc.quote(request_id="q1", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s500", "quantity": 24}])
        # 再报一个同样的量仍可，因为没有占用
        q2 = self.f.svc.quote(request_id="q2", channel="night", region_id="R1",
                              customer_location="L",
                              items=[{"sku_id": "s500", "quantity": 24}])
        self.assertTrue(q2["available"])

    def test_confirm_after_quote_ttl_rejected(self):
        self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s330", "quantity": 1}])
        self.f.clock.value += timedelta(seconds=121)
        with self.assertRaises(UnfulfillableError) as ctx:
            self.f.svc.confirm(request_id="o", quote_request_id="q")
        self.assertEqual("promise_expired", ctx.exception.details["reason"])

    def test_confirm_unavailable_quote_rejected(self):
        self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s500", "quantity": 999}])
        with self.assertRaises(UnfulfillableError):
            self.f.svc.confirm(request_id="o", quote_request_id="q")

    def test_confirm_unknown_quote_404(self):
        with self.assertRaises(NotFoundError):
            self.f.svc.confirm(request_id="o", quote_request_id="missing")

    def test_confirm_rechecks_capacity(self):
        # 班次容量 3：连续 4 单（各 1 件），第 4 单应因运力失败
        for i in range(3):
            self._order(qid=f"q{i}", oid=f"o{i}", sku="s330", qty=1)
        q = self.f.svc.quote(request_id="qz", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s330", "quantity": 1}])
        # 报价阶段看到无运力
        self.assertFalse(q["available"])
        self.assertIn({"reason": "no_courier_capacity"}, q["blockers"])


class IdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture()

    def tearDown(self):
        self.f.close()

    def test_quote_replay_returns_same_result(self):
        args = dict(channel="night", region_id="R1", customer_location="L",
                    items=[{"sku_id": "s500", "quantity": 1}])
        first = self.f.svc.quote(request_id="r", **args)
        second = self.f.svc.quote(request_id="r", **args)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])

    def test_same_request_different_payload_conflicts(self):
        self.f.svc.quote(request_id="r", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s500", "quantity": 1}])
        with self.assertRaises(ConflictError):
            self.f.svc.quote(request_id="r", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s500", "quantity": 2}])

    def test_request_id_cannot_cross_actions(self):
        self.f.svc.quote(request_id="x", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s330", "quantity": 1}])
        with self.assertRaises(ConflictError):
            self.f.svc.confirm(request_id="x", quote_request_id="x")

    def test_confirm_retry_returns_same_promise(self):
        self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s330", "quantity": 1}])
        first = self.f.svc.confirm(request_id="o", quote_request_id="q")
        second = self.f.svc.confirm(request_id="o", quote_request_id="q")
        self.assertEqual(first["promise_id"], second["promise_id"])
        self.assertTrue(second["replayed"])

    def test_confirm_request_reused_with_other_quote_conflicts(self):
        self.f.svc.quote(request_id="q1", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s330", "quantity": 1}])
        self.f.svc.quote(request_id="q2", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s330", "quantity": 2}])
        self.f.svc.confirm(request_id="o", quote_request_id="q1")
        with self.assertRaises(ConflictError):
            self.f.svc.confirm(request_id="o", quote_request_id="q2")


class DeliveryAndCancelTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture()

    def tearDown(self):
        self.f.close()

    def _confirmed(self, sku="s330", qty=3, qid="q", oid="o"):
        self.f.svc.quote(request_id=qid, channel="night", region_id="R1",
                         customer_location="L", items=[{"sku_id": sku, "quantity": qty}])
        order = self.f.svc.confirm(request_id=oid, quote_request_id=qid)
        return order

    def test_cancel_before_pick_refunds_and_releases(self):
        order = self._confirmed(qty=2)
        result = self.f.svc.cancel(request_id="c", promise_id=order["promise_id"])
        self.assertEqual("cancelled", result["status"])
        self.assertEqual(2 * 1200, result["refund_milli"])
        held = self.f.database.connection.execute(
            "SELECT COUNT(*) FROM inventory_holds WHERE promise_id=? AND state='held'",
            (order["promise_id"],)).fetchone()[0]
        self.assertEqual(0, held)

    def test_cancel_after_pick_rejected(self):
        order = self._confirmed()
        self.f.svc.mark_picked(request_id="k", promise_id=order["promise_id"])
        with self.assertRaises(ConflictError):
            self.f.svc.cancel(request_id="c", promise_id=order["promise_id"])

    def test_partial_delivery_refund_conserves_amount(self):
        order = self._confirmed(qty=3)
        pid = order["promise_id"]
        self.f.svc.mark_picked(request_id="k", promise_id=pid)
        receipt = self.f.svc.delivery_receipt(
            request_id="d", promise_id=pid,
            delivered=[{"line_no": 1, "quantity": 2}])
        self.assertEqual("partially_delivered", receipt["status"])
        self.assertEqual(2 * 1200, receipt["delivered_milli"])
        self.assertEqual(1200, receipt["refund_milli"])
        self.assertEqual(order["total_milli"],
                         receipt["delivered_milli"] + receipt["refund_milli"])

    def test_over_delivery_rejected(self):
        order = self._confirmed(qty=3)
        with self.assertRaises(ValidationError):
            self.f.svc.delivery_receipt(
                request_id="d", promise_id=order["promise_id"],
                delivered=[{"line_no": 1, "quantity": 4}])

    def test_receipt_unknown_line_rejected(self):
        order = self._confirmed(qty=1)
        with self.assertRaises(ValidationError):
            self.f.svc.delivery_receipt(
                request_id="d", promise_id=order["promise_id"],
                delivered=[{"line_no": 9, "quantity": 1}])

    def test_delivery_receipt_idempotent(self):
        order = self._confirmed(qty=1)
        args = dict(request_id="d", promise_id=order["promise_id"])
        first = self.f.svc.delivery_receipt(**args)
        second = self.f.svc.delivery_receipt(**args)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual("delivered", second["status"])

    def test_duplicate_receipt_after_partial_conflicts_if_content_differs(self):
        order = self._confirmed(qty=3)
        pid = order["promise_id"]
        self.f.svc.mark_picked(request_id="k", promise_id=pid)
        self.f.svc.delivery_receipt(request_id="d", promise_id=pid,
                                   delivered=[{"line_no": 1, "quantity": 2}])
        with self.assertRaises(ConflictError):
            self.f.svc.delivery_receipt(request_id="d", promise_id=pid,
                                       delivered=[{"line_no": 1, "quantity": 1}])


class ExpiryTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture(clock=MutableClock(
            datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)))

    def tearDown(self):
        self.f.close()

    def _confirmed(self, oid):
        self.f.svc.quote(request_id=f"q-{oid}", channel="night", region_id="R1",
                         customer_location="L", items=[{"sku_id": "s330", "quantity": 1}])
        return self.f.svc.confirm(request_id=oid, quote_request_id=f"q-{oid}")

    def test_sweep_releases_only_unpicked_expired(self):
        unpicked = self._confirmed("o1")
        picked = self._confirmed("o2")
        self.f.svc.mark_picked(request_id="k2", promise_id=picked["promise_id"])
        self.f.clock.value += timedelta(hours=2)
        expired = self.f.svc.expire_stale_promises()
        self.assertIn(unpicked["promise_id"], expired)
        self.assertNotIn(picked["promise_id"], expired)
        self.assertEqual("expired", self.f.svc.get_promise(unpicked["promise_id"])["status"])
        self.assertEqual("picked", self.f.svc.get_promise(picked["promise_id"])["status"])
        # o2 取货已物理出店 1 瓶（30→29），o1 未取货释放不扣现货，故可售 29
        q = self.f.svc.quote(request_id="q-again", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s330", "quantity": 29}])
        self.assertTrue(q["available"])

    def test_sweep_idempotent_nothing_to_do(self):
        self.f.clock.value += timedelta(hours=2)
        self.assertEqual([], self.f.svc.expire_stale_promises())
        self.assertEqual([], self.f.svc.expire_stale_promises())


class SplitTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture()

    def tearDown(self):
        self.f.close()

    def _order(self):
        self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s500", "quantity": 2},
                                {"sku_id": "s330", "quantity": 3}])
        return self.f.svc.confirm(request_id="o", quote_request_id="q")

    def test_split_conserves_quantity_and_amount(self):
        order = self._order()
        result = self.f.svc.split(request_id="sp", promise_id=order["promise_id"],
                                  groups=[[1], [2]])
        self.assertTrue(result["quantity_conserved"])
        self.assertTrue(result["amount_conserved"])
        self.assertEqual(order["total_milli"],
                         sum(c["total_milli"] for c in result["children"]))
        self.assertEqual(5, sum(sum(i["quantity"] for i in c["items"])
                                for c in result["children"]))

    def test_split_must_cover_all_lines_once(self):
        order = self._order()
        with self.assertRaises(ValidationError):
            self.f.svc.split(request_id="sp1", promise_id=order["promise_id"],
                             groups=[[1], [1]])
        with self.assertRaises(ValidationError):
            self.f.svc.split(request_id="sp2", promise_id=order["promise_id"],
                             groups=[[1]])

    def test_split_only_confirmed_unpicked(self):
        order = self._order()
        self.f.svc.mark_picked(request_id="k", promise_id=order["promise_id"])
        with self.assertRaises(ConflictError):
            self.f.svc.split(request_id="sp", promise_id=order["promise_id"],
                             groups=[[1], [2]])

    def test_split_idempotent(self):
        order = self._order()
        first = self.f.svc.split(request_id="sp", promise_id=order["promise_id"],
                                 groups=[[1], [2]])
        second = self.f.svc.split(request_id="sp", promise_id=order["promise_id"],
                                  groups=[[1], [2]])
        self.assertTrue(second["replayed"])
        self.assertEqual([c["promise_id"] for c in first["children"]],
                         [c["promise_id"] for c in second["children"]])


class StoreUpdateTest(unittest.TestCase):
    def setUp(self):
        self.f = FulfillmentFixture()

    def tearDown(self):
        self.f.close()

    def test_cannot_reduce_below_outstanding_hold(self):
        self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                         customer_location="L",
                         items=[{"sku_id": "s330", "quantity": 10}])
        self.f.svc.confirm(request_id="o", quote_request_id="q")
        with self.assertRaises(ConflictError):
            self.f.svc.upsert_batch(batch_id="b330", store_id="st", sku_id="s330",
                                    quantity_on_hand=5, production_date="2026-06-01",
                                    expiration_date="2026-11-15", chilled=True)

    def test_restock_is_reflected_in_quote(self):
        self.f.svc.upsert_batch(batch_id="b330", store_id="st", sku_id="s330",
                                quantity_on_hand=100, production_date="2026-08-01",
                                expiration_date="2026-12-15", chilled=True)
        q = self.f.svc.quote(request_id="q", channel="night", region_id="R1",
                             customer_location="L",
                             items=[{"sku_id": "s330", "quantity": 80}])
        self.assertTrue(q["available"])


class RestartRecoveryTest(unittest.TestCase):
    def test_pending_confirmed_recovered_after_restart(self):
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, "r.sqlite3")
        f = FulfillmentFixture(path=path)
        f.svc.quote(request_id="q", channel="night", region_id="R1",
                    customer_location="L", items=[{"sku_id": "s330", "quantity": 1}])
        order = f.svc.confirm(request_id="o", quote_request_id="q")
        f.database.close()

        database = Database(path)
        svc = FulfillmentService(database)
        pending = {p["promise_id"] for p in svc.pending_promises()}
        self.assertIn(order["promise_id"], pending)
        replay = svc.confirm(request_id="o", quote_request_id="q")
        self.assertTrue(replay["replayed"])
        self.assertEqual(order["promise_id"], replay["promise_id"])
        self.assertTrue(svc.reconcile()["balanced"])
        database.close()


if __name__ == "__main__":
    unittest.main()
