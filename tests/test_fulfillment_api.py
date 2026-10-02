"""履约 HTTP 路由测试。"""

import json
import unittest

from beverage_ops_foundation.api import route
from beverage_ops_foundation.clock import MutableClock
from beverage_ops_foundation.fulfillment import FulfillmentService
from beverage_ops_foundation.service import DomainService
from beverage_ops_foundation.storage import Database
from datetime import datetime, timezone


class FulfillmentApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = MutableClock(datetime(2026, 10, 2, tzinfo=timezone.utc))
        self.service = DomainService(self.database, self.clock)
        self.fs = FulfillmentService(self.database, self.clock)
        self.headers = {"X-Actor-Id": "op1"}
        self.service.register_organization(request_id="org", actor_id="bootstrap",
                                           organization_id="o1", name="集团")
        self.service.register_actor(request_id="adm", actor_id="bootstrap", new_actor_id="a1",
                                   display_name="管理员", role="admin", organization_id="o1")
        self.service.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                   display_name="运营", role="operator", organization_id="o1")

    def tearDown(self):
        self.database.close()

    def post(self, path, body):
        return route(self.service, "POST", path, body, self.headers, fulfillment=self.fs)

    def get(self, path):
        return route(self.service, "GET", path, None, self.headers, fulfillment=self.fs)

    def _catalog(self):
        self.post("/channels", {"request_id": "ch", "channel_id": "night", "name": "夜购"})
        self.post("/products", {"request_id": "p", "product_id": "beer-a", "brand_id": "b",
                                "spec_name": "A", "tier": "premium", "requires_cold": True,
                                "list_price_cents": 1500})
        self.post("/stores", {"request_id": "s", "store_id": "st1", "organization_id": "o1",
                              "name": "店", "region_id": "east"})
        self.post("/couriers", {"request_id": "c", "courier_id": "k1", "name": "骑手",
                                "region_id": "east", "capacity_units": 5})
        self.post("/brand-channel-rules", {"request_id": "bc", "brand_id": "b",
                                           "channel_id": "night", "allowed": True})
        self.post("/brand-region-rules", {"request_id": "br", "brand_id": "b",
                                          "region_id": "east", "allowed": True})
        self.post("/batches", {"request_id": "b1", "batch_id": "lot1", "store_id": "st1",
                               "product_id": "beer-a", "lot_no": "L1", "qty_on_hand": 3,
                               "cold_chain_ok": True, "expiry_date": "2026-11-01",
                               "near_expiry_days": 3, "unit_price_cents": 1500})

    def test_quote_confirm_delivery_http_chain(self):
        self._catalog()
        status, quote = self.post("/quotes", {
            "request_id": "q1", "channel_id": "night", "region_id": "east",
            "customer_id": "cu1", "items": [{"product_id": "beer-a", "qty": 2}]})
        self.assertEqual(201, status)
        self.assertTrue(quote["feasible"])

        status, order = self.post("/orders/confirm", {
            "request_id": "o1", "quote_id": quote["quote_id"]})
        self.assertEqual(201, status)
        self.assertEqual("held", order["status"])

        delivery_id = order["deliveries"][0]["delivery_id"]
        status, picked = self.post("/deliveries/pick", {
            "request_id": "pk1", "delivery_id": delivery_id})
        self.assertEqual(201, status)
        self.assertEqual("picked", picked["status"])

        line_id = order["lines"][0]["line_id"]
        status, done = self.post("/deliveries/complete", {
            "request_id": "dn1", "delivery_id": delivery_id,
            "delivered_items": [{"line_id": line_id, "qty": 2}]})
        self.assertEqual(201, status)
        self.assertEqual("delivered", done["order_status"])

        status, fetched = self.get(f"/orders/{order['order_id']}")
        self.assertEqual(200, status)
        self.assertEqual("delivered", fetched["status"])

    def test_replay_returns_200_and_same_resource(self):
        self._catalog()
        body = {"request_id": "q1", "channel_id": "night", "region_id": "east",
                "customer_id": "cu1", "items": [{"product_id": "beer-a", "qty": 1}]}
        s1, q1 = self.post("/quotes", body)
        s2, q2 = self.post("/quotes", body)
        self.assertEqual(201, s1)
        self.assertEqual(200, s2)
        self.assertTrue(q2["replayed"])
        self.assertEqual(q1["quote_id"], q2["quote_id"])

    def test_same_request_id_different_body_returns_409(self):
        self._catalog()
        self.post("/quotes", {"request_id": "q1", "channel_id": "night", "region_id": "east",
                              "customer_id": "cu1",
                              "items": [{"product_id": "beer-a", "qty": 1}]})
        status, payload = self.post("/quotes", {
            "request_id": "q1", "channel_id": "night", "region_id": "east",
            "customer_id": "cu1", "items": [{"product_id": "beer-a", "qty": 2}]})
        self.assertEqual(409, status)
        self.assertEqual("conflict", payload["error"])

    def test_reconcile_endpoint(self):
        status, payload = self.post("/reconcile", {})
        self.assertEqual(200, status)
        self.assertIn("expired_quotes", payload)


if __name__ == "__main__":
    unittest.main()
