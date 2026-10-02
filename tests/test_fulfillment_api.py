import unittest
from datetime import datetime, timezone

from beverage_ops_foundation.api import route_fulfillment
from beverage_ops_foundation.clock import FixedClock
from beverage_ops_foundation.fulfillment import FulfillmentService
from beverage_ops_foundation.storage import Database


def seed(svc):
    svc.upsert_sku(sku_id="s500", brand_id="bA", product_line="ice", spec="500ml",
                   display_name="冰啤500", unit_price_milli=1800,
                   requires_cold=True, shelf_life_days=180)
    svc.upsert_channel_rule(brand_id="bA", channel="night", allowed_specs=["500ml"])
    svc.upsert_store(store_id="st", name="夜店", region_id="R1", sales_regions=["R1"])
    svc.upsert_batch(batch_id="b1", store_id="st", sku_id="s500", quantity_on_hand=5,
                     production_date="2026-06-01", expiration_date="2026-11-15",
                     chilled=True)
    svc.upsert_courier(courier_id="c1", store_id="st", region_id="R1")
    svc.upsert_shift(shift_id="sh1", courier_id="c1",
                     starts_at="2026-10-02T10:00:00+00:00",
                     ends_at="2026-10-02T20:00:00+00:00", capacity=3)


class FulfillmentApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        clock = FixedClock(datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc))
        self.svc = FulfillmentService(self.database, clock)
        seed(self.svc)

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None):
        return route_fulfillment(self.svc, method, path, body)

    def test_quote_then_confirm_flow(self):
        status, quote = self.call("POST", "/fulfillment/quotes", {
            "request_id": "q1", "channel": "night", "region_id": "R1",
            "customer_location": "L", "items": [{"sku_id": "s500", "quantity": 2}]})
        self.assertEqual(200, status)
        self.assertTrue(quote["available"])
        status, order = self.call("POST", "/fulfillment/confirms", {
            "request_id": "o1", "quote_request_id": "q1"})
        self.assertEqual(201, status)
        self.assertEqual("confirmed", order["status"])
        pid = order["promise_id"]

        status, picked = self.call("POST", "/fulfillment/picks",
                                   {"request_id": "p1", "promise_id": pid})
        self.assertEqual(200, status)
        self.assertEqual("picked", picked["status"])

        status, receipt = self.call("POST", "/fulfillment/delivery-receipts",
                                    {"request_id": "d1", "promise_id": pid})
        self.assertEqual(200, status)
        self.assertEqual("delivered", receipt["status"])

        status, fetched = self.call("GET", f"/fulfillment/promises/{pid}")
        self.assertEqual(200, status)
        self.assertEqual("delivered", fetched["status"])

    def test_unavailable_quote_lists_blockers(self):
        status, body = self.call("POST", "/fulfillment/quotes", {
            "request_id": "q", "channel": "night", "region_id": "R1",
            "customer_location": "L", "items": [{"sku_id": "s500", "quantity": 99}]})
        self.assertEqual(200, status)
        self.assertFalse(body["available"])
        self.assertTrue(body["blockers"])

    def test_confirm_unavailable_quote_returns_422_with_details(self):
        self.call("POST", "/fulfillment/quotes", {
            "request_id": "q", "channel": "night", "region_id": "R1",
            "customer_location": "L", "items": [{"sku_id": "s500", "quantity": 99}]})
        status, body = self.call("POST", "/fulfillment/confirms",
                                 {"request_id": "o", "quote_request_id": "q"})
        self.assertEqual(422, status)
        self.assertEqual("unfulfillable", body["error"])
        self.assertIn("details", body)

    def test_idempotent_conflict_returns_409(self):
        self.call("POST", "/fulfillment/quotes", {
            "request_id": "q1", "channel": "night", "region_id": "R1",
            "customer_location": "L", "items": [{"sku_id": "s500", "quantity": 1}]})
        self.call("POST", "/fulfillment/confirms",
                  {"request_id": "o", "quote_request_id": "q1"})
        self.call("POST", "/fulfillment/quotes", {
            "request_id": "q2", "channel": "night", "region_id": "R1",
            "customer_location": "L", "items": [{"sku_id": "s500", "quantity": 2}]})
        status, body = self.call("POST", "/fulfillment/confirms",
                                 {"request_id": "o", "quote_request_id": "q2"})
        self.assertEqual(409, status)
        self.assertEqual("conflict", body["error"])

    def test_invalid_body_returns_400(self):
        status, body = self.call("POST", "/fulfillment/confirms", {"request_id": "x"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", body["error"])

    def test_reconcile_and_pending_endpoints(self):
        self.call("POST", "/fulfillment/quotes", {
            "request_id": "q", "channel": "night", "region_id": "R1",
            "customer_location": "L", "items": [{"sku_id": "s500", "quantity": 1}]})
        self.call("POST", "/fulfillment/confirms",
                  {"request_id": "o", "quote_request_id": "q"})
        status, pending = self.call("GET", "/fulfillment/pending")
        self.assertEqual(200, status)
        self.assertEqual(1, len(pending["items"]))
        status, recon = self.call("GET", "/fulfillment/reconcile")
        self.assertEqual(200, status)
        self.assertTrue(recon["balanced"])

    def test_unknown_route_404(self):
        status, body = self.call("GET", "/fulfillment/nope", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", body["error"])

    def test_admin_batch_update(self):
        status, body = self.call("POST", "/fulfillment/admin/batches", {
            "batch_id": "b1", "store_id": "st", "sku_id": "s500",
            "quantity_on_hand": 8, "production_date": "2026-06-01",
            "expiration_date": "2026-11-15", "chilled": True})
        self.assertEqual(200, status)
        self.assertEqual("ok", body["status"])


if __name__ == "__main__":
    unittest.main()
