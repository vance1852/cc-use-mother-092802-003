"""夜间即时零售履约系统的离线端到端验收。

覆盖：
1. 高峰并发下库存与运力不超卖；
2. 迟到配送回执仍可登记，短少转为部分送达并退款，金额守恒；
3. 门店现货数据更新受在途占用约束，更新后确认能看到最新库存；
4. 进程重启后待履约订单恢复、幂等重放返回原结果、超时释放不影响已取货订单。
"""

from __future__ import annotations

import json
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .clock import Clock
from .errors import UnfulfillableError
from .fulfillment import FulfillmentService
from .storage import Database


class MutableClock(Clock):
    """可在验收中向后推进的时钟。"""

    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value.astimezone(timezone.utc)


def _seed(svc: FulfillmentService) -> None:
    svc.upsert_sku(sku_id="beer-500", brand_id="brand-a", product_line="premium-ice",
                   spec="500ml", display_name="高端冰啤500ml", unit_price_milli=1800,
                   requires_cold=True, shelf_life_days=180)
    svc.upsert_sku(sku_id="beer-330", brand_id="brand-a", product_line="premium-ice",
                   spec="330ml", display_name="高端冰啤330ml", unit_price_milli=1200,
                   requires_cold=True, shelf_life_days=180)
    svc.upsert_channel_rule(brand_id="brand-a", channel="night_retail",
                            allowed_specs=["500ml", "330ml"])
    svc.upsert_substitution(brand_id="brand-a", channel="night_retail",
                            from_sku="beer-500", rank=1, to_sku="beer-330")
    svc.upsert_store(store_id="store-1", name="夜间旗舰店", region_id="north-1",
                     sales_regions=["north-1", "north-2"])
    svc.upsert_batch(batch_id="batch-near", store_id="store-1", sku_id="beer-500",
                     quantity_on_hand=4, production_date="2026-04-02",
                     expiration_date="2026-10-04", chilled=True)
    svc.upsert_batch(batch_id="batch-good", store_id="store-1", sku_id="beer-500",
                     quantity_on_hand=20, production_date="2026-07-01",
                     expiration_date="2026-12-01", chilled=True)
    svc.upsert_batch(batch_id="batch-warm", store_id="store-1", sku_id="beer-500",
                     quantity_on_hand=50, production_date="2026-07-01",
                     expiration_date="2026-12-01", chilled=False)
    svc.upsert_batch(batch_id="batch-330", store_id="store-1", sku_id="beer-330",
                     quantity_on_hand=30, production_date="2026-07-01",
                     expiration_date="2026-12-01", chilled=True)
    svc.upsert_courier(courier_id="courier-1", store_id="store-1", region_id="north-1")
    svc.upsert_shift(shift_id="shift-night", courier_id="courier-1",
                     starts_at="2026-10-02T10:00:00+00:00",
                     ends_at="2026-10-03T02:00:00+00:00", capacity=100)


def run() -> dict[str, object]:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "fulfillment.sqlite3"
        base = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
        clock = MutableClock(base)
        database = Database(path)
        svc = FulfillmentService(database, clock)
        _seed(svc)

        checks: dict[str, bool] = {}

        # 1) 高峰并发：30 个订单各抢 1 瓶 500ml，冷藏可售仅 24（4 临期 + 20 正常）
        outcomes = {"confirmed": 0, "rejected": 0}
        lock = threading.Lock()

        def rush(index: int) -> None:
            try:
                svc.quote(request_id=f"rush-q-{index}", channel="night_retail",
                          region_id="north-1", customer_location=f"loc-{index}",
                          items=[{"sku_id": "beer-500", "quantity": 1}])
                svc.confirm(request_id=f"rush-o-{index}", quote_request_id=f"rush-q-{index}")
                with lock:
                    outcomes["confirmed"] += 1
            except UnfulfillableError:
                with lock:
                    outcomes["rejected"] += 1

        threads = [threading.Thread(target=rush, args=(i,)) for i in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        checks["peak_no_oversell"] = outcomes == {"confirmed": 24, "rejected": 6}
        held = database.connection.execute(
            "SELECT COALESCE(SUM(quantity),0) FROM inventory_holds WHERE state='held'"
        ).fetchone()[0]
        checks["peak_holds_match_orders"] = held == 24

        # 2) 迟到配送回执：取一单取货，时钟推进 2 小时后回执短少 1 件？
        #    rush 订单都是 1 件，这里单独下一单 3 件以演示部分送达
        quote = svc.quote(request_id="late-q", channel="night_retail", region_id="north-1",
                          customer_location="late-loc",
                          items=[{"sku_id": "beer-330", "quantity": 3}])
        order = svc.confirm(request_id="late-o", quote_request_id="late-q")
        late_promise = order["promise_id"]
        svc.mark_picked(request_id="late-pick", promise_id=late_promise)
        clock.value = base + timedelta(hours=2)
        receipt = svc.delivery_receipt(
            request_id="late-receipt", promise_id=late_promise,
            delivered=[{"line_no": 1, "quantity": 2}])
        checks["late_receipt_partial"] = receipt["status"] == "partially_delivered"
        checks["late_amount_conserved"] = (
            receipt["delivered_milli"] == 2 * 1200
            and receipt["refund_milli"] == 1200
            and receipt["delivered_milli"] + receipt["refund_milli"] == order["total_milli"])
        # 重试迟到回执返回原结果
        replay = svc.delivery_receipt(
            request_id="late-receipt", promise_id=late_promise,
            delivered=[{"line_no": 1, "quantity": 2}])
        checks["late_receipt_idempotent"] = replay["replayed"] is True

        # 3) 门店数据更新：在途占用 24 瓶 500ml，把现货改到 24 以下必须被拒
        try:
            svc.upsert_batch(batch_id="batch-good", store_id="store-1", sku_id="beer-500",
                             quantity_on_hand=10, production_date="2026-07-01",
                             expiration_date="2026-12-01", chilled=True)
            checks["store_update_guard"] = False
        except Exception:
            checks["store_update_guard"] = True
        # 补货后新的报价可以使用增加的库存
        svc.upsert_batch(batch_id="batch-good", store_id="store-1", sku_id="beer-500",
                         quantity_on_hand=40, production_date="2026-07-01",
                         expiration_date="2026-12-01", chilled=True)
        restock_q = svc.quote(request_id="restock-q", channel="night_retail",
                              region_id="north-1", customer_location="r-loc",
                              items=[{"sku_id": "beer-500", "quantity": 10}])
        checks["store_update_visible"] = restock_q["available"] is True

        # 4) 超时释放：一单确认不取货，一单确认并取货；推进时钟超过承诺窗口
        clock.value = base
        exp_q = svc.quote(request_id="exp-q", channel="night_retail", region_id="north-1",
                          customer_location="e", items=[{"sku_id": "beer-330", "quantity": 1}])
        exp_order = svc.confirm(request_id="exp-o", quote_request_id="exp-q")
        pick_q = svc.quote(request_id="pick-q", channel="night_retail", region_id="north-1",
                           customer_location="p", items=[{"sku_id": "beer-330", "quantity": 1}])
        pick_order = svc.confirm(request_id="pick-o", quote_request_id="pick-q")
        svc.mark_picked(request_id="pick-k", promise_id=pick_order["promise_id"])
        clock.value = base + timedelta(hours=3)
        expired_ids = svc.expire_stale_promises()
        checks["expiry_releases_unpicked"] = exp_order["promise_id"] in expired_ids
        checks["expiry_keeps_picked"] = (
            svc.get_promise(pick_order["promise_id"])["status"] == "picked")
        checks["picked_still_delivers"] = (
            svc.delivery_receipt(request_id="pick-d",
                                 promise_id=pick_order["promise_id"])["status"] == "delivered")
        reconciled = svc.reconcile()
        checks["reconcile_balanced"] = reconciled["balanced"]

        # 5) 模拟进程重启：关闭再打开同一数据库
        database.close()
        database = Database(path)
        restarted = FulfillmentService(database, clock)
        pending = restarted.pending_promises()
        pending_ids = {p["promise_id"] for p in pending}
        checks["restart_recovers_pending"] = pick_order["promise_id"] not in pending_ids
        # 已取货并送达的为 delivered（终态），不在待履约；构造一个重启前仍 confirmed 的单子
        clock.value = base
        live_quote = restarted.quote(
            request_id="live-q", channel="night_retail", region_id="north-1",
            customer_location="live", items=[{"sku_id": "beer-330", "quantity": 1}])
        live_order = restarted.confirm(request_id="live-o", quote_request_id="live-q")
        database.close()
        database = Database(path)
        restarted2 = FulfillmentService(database, clock)
        pending2 = {p["promise_id"] for p in restarted2.pending_promises()}
        checks["restart_pending_confirmed"] = live_order["promise_id"] in pending2
        # 幂等跨重启：重复确认返回原 promise
        replay_confirm = restarted2.confirm(request_id="live-o", quote_request_id="live-q")
        checks["restart_idempotent_replay"] = (
            replay_confirm["replayed"] is True
            and replay_confirm["promise_id"] == live_order["promise_id"])
        checks["restart_reconcile_balanced"] = restarted2.reconcile()["balanced"]
        database.close()

        result = {
            "status": "ok" if all(checks.values()) else "failed",
            "checks": checks,
            "peak_outcomes": outcomes,
        }
        return result


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
