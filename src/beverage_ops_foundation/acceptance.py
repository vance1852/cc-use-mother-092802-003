"""运行基础服务与夜间即时零售履约的离线端到端验收。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .audit import verify_chain
from .clock import MutableClock
from .fulfillment import FulfillmentService
from .maintenance import ReconcileLoop
from .service import DomainService
from .storage import Database


def _run_night_scenario(database: Database, clock: MutableClock,
                        service: DomainService) -> dict[str, object]:
    """覆盖报价解释、原子占用、超时保护、部分送达、幂等与重启恢复。"""

    fs = FulfillmentService(database, clock, quote_ttl_seconds=120)
    actor = "operator-001"

    fs.register_channel(request_id="n-ch", actor_id=actor, channel_id="night",
                        name="夜间闪购")
    fs.register_product(request_id="n-pa", actor_id=actor, product_id="beer-a",
                        brand_id="brand-x", spec_name="高端啤酒A 500ml", tier="premium",
                        requires_cold=True, list_price_cents=1500)
    fs.register_product(request_id="n-pb", actor_id=actor, product_id="beer-b",
                        brand_id="brand-x", spec_name="高端啤酒B 330ml", tier="premium",
                        requires_cold=True, list_price_cents=1200)
    fs.register_store(request_id="n-st", actor_id=actor, store_id="night-store",
                      organization_id="org-001", name="夜供门店", region_id="region-1")
    fs.register_courier(request_id="n-c1", actor_id=actor, courier_id="ride-1",
                        name="夜骑一", region_id="region-1", capacity_units=4)
    fs.register_courier(request_id="n-c2", actor_id=actor, courier_id="ride-2",
                        name="夜骑二", region_id="region-1", capacity_units=4)
    fs.set_brand_channel_rule(request_id="n-bc", actor_id=actor, brand_id="brand-x",
                              channel_id="night", allowed=True)
    fs.set_brand_region_rule(request_id="n-br", actor_id=actor, brand_id="brand-x",
                             region_id="region-1", allowed=True)
    fs.set_substitution_rule(request_id="n-sub", actor_id=actor, product_id="beer-a",
                             channel_id="night", substitute_product_id="beer-b", priority=1)

    # 合格批次 2 件、失温批次 1 件（必须被解释排除），替代规格批次 5 件。
    fs.upsert_batch(request_id="n-b1", actor_id=actor, batch_id="night-lot-ok",
                    store_id="night-store", product_id="beer-a", lot_no="OK1",
                    qty_on_hand=2, cold_chain_ok=True, expiry_date="2026-11-01",
                    near_expiry_days=3, unit_price_cents=1500)
    fs.upsert_batch(request_id="n-b2", actor_id=actor, batch_id="night-lot-warm",
                    store_id="night-store", product_id="beer-a", lot_no="WARM1",
                    qty_on_hand=1, cold_chain_ok=False, expiry_date="2026-11-01",
                    near_expiry_days=3, unit_price_cents=1500)
    fs.upsert_batch(request_id="n-b3", actor_id=actor, batch_id="night-lot-sub",
                    store_id="night-store", product_id="beer-b", lot_no="SUB1",
                    qty_on_hand=5, cold_chain_ok=True, expiry_date="2026-11-20",
                    near_expiry_days=3, unit_price_cents=1200)

    quote = fs.quote(request_id="n-quote", actor_id=actor, channel_id="night",
                     region_id="region-1", customer_id="night-customer",
                     items=[{"product_id": "beer-a", "qty": 5}])
    qline = quote["lines"][0]
    excluded = {e["batch_id"]: e["reasons"] for e in qline["excluded_batches"]}
    checks = {
        "quote_infeasible_as_expected": not quote["feasible"],
        "quote_matched_two": qline["qty_matched"] == 2,
        "cold_batch_explained": "cold_chain_broken" in excluded.get("night-lot-warm", []),
        "substitute_offered": [s["product_id"] for s in qline["substitutes"]] == ["beer-b"],
        "quote_ttl_seconds": quote["ttl_seconds"],
    }

    order = fs.confirm_order(
        request_id="n-order", actor_id=actor, quote_id=quote["quote_id"],
        substitutions=[{"for_product_id": "beer-a", "product_id": "beer-b", "qty": 3}])
    checks["order_held"] = order["status"] == "held"
    checks["split_into_two_couriers"] = len(order["deliveries"]) == 2
    checks["total_cents"] = order["total_cents"]  # 2*1500 + 3*1200 = 6600

    # 同请求号不同内容必须报冲突。
    from .errors import ConflictError

    try:
        fs.quote(request_id="n-quote", actor_id=actor, channel_id="night",
                 region_id="region-1", customer_id="night-customer",
                 items=[{"product_id": "beer-a", "qty": 4}])
        checks["request_conflict_detected"] = False
    except ConflictError:
        checks["request_conflict_detected"] = True

    # 第一个配送腿取货；第二个保持 planned，用于验证超时释放不影响已取货。
    picked_delivery = order["deliveries"][0]["delivery_id"]
    held_delivery = order["deliveries"][1]["delivery_id"]
    fs.mark_picked(request_id="n-pick", actor_id=actor, delivery_id=picked_delivery)

    # 第二个订单：未取货，超时后应被释放。
    quote2 = fs.quote(request_id="n-quote-2", actor_id=actor, channel_id="night",
                      region_id="region-1", customer_id="night-customer-2",
                      items=[{"product_id": "beer-b", "qty": 2}])
    order2 = fs.confirm_order(request_id="n-order-2", actor_id=actor,
                              quote_id=quote2["quote_id"])

    clock.advance(seconds=200)
    reconcile_result = ReconcileLoop(fs).startup_recovery()
    checks["expired_held_order_released"] = order2["order_id"] in \
        reconcile_result["expired_orders"]
    picked_detail = fs.get_order(order["order_id"])
    checks["picked_order_protected"] = picked_detail["status"] == "picked"
    checks["picked_qty_intact"] = sum(
        line["qty_committed"] for line in picked_detail["lines"]) == 5

    # 已取货腿部分送达：按携带明细每行少送 1 件，其余退款。
    picked_info = next(d for d in picked_detail["deliveries"]
                       if d["delivery_id"] == picked_delivery)
    delivered_items = [{"line_id": item["line_id"], "qty": max(item["qty"] - 1, 0)}
                       for item in picked_info["items"]]
    completed = fs.complete_delivery(request_id="n-complete", actor_id=actor,
                                     delivery_id=picked_delivery,
                                     delivered_items=delivered_items)
    # 迟到回执（平台重试）必须返回原结果。
    replayed = fs.complete_delivery(request_id="n-complete", actor_id=actor,
                                    delivery_id=picked_delivery,
                                    delivered_items=delivered_items)
    checks["late_receipt_replayed"] = bool(replayed["replayed"])
    checks["replay_same_status"] = replayed["order_status"] == completed["order_status"]

    # 订单已是 picked，其第二条 planned 配送腿不被超时回收；
    # 模拟该腿的迟到送达回执（携带 1 件，全部送达）。
    held_info = next(d for d in picked_detail["deliveries"]
                     if d["delivery_id"] == held_delivery)
    late_items = [{"line_id": item["line_id"], "qty": item["qty"]}
                  for item in held_info["items"]]
    late = fs.complete_delivery(request_id="n-late-leg", actor_id=actor,
                                delivery_id=held_delivery,
                                delivered_items=late_items)
    checks["late_leg_accepted"] = late["order_status"] == "partial"

    detail = fs.get_order(order["order_id"])
    committed_cents = sum(line["qty_committed"] * line["unit_price_cents"]
                          for line in detail["lines"])
    settled_qty = sum(line["qty_delivered"] + line["qty_refunded"]
                      for line in detail["lines"]) + detail["qty_outstanding"]
    # 5 件：送达 3（1500 + 2*1200 = 3900），退款 2（1500 + 1200 = 2700）
    checks["qty_balanced"] = settled_qty == detail["total_qty"] == 5
    checks["amount_balanced"] = committed_cents == detail["total_cents"] == 6600
    checks["refund_amount_exact"] = detail["refunded_cents"] == 2700
    checks["delivered_amount_exact"] = detail["delivered_cents"] == 3900
    checks["fully_settled"] = detail["qty_outstanding"] == 0
    checks["final_status_partial"] = detail["status"] == "partial"

    return {"night_checks": checks, "order_id": order["order_id"],
            "reconcile": reconcile_result}


def run() -> dict[str, object]:
    """执行一条完整登记链与夜间履约链并返回结果。"""

    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / "acceptance.sqlite3"
        database = Database(db_path)
        clock = MutableClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc))
        service = DomainService(database, clock)
        service.register_organization(request_id="req-org", actor_id="bootstrap",
                                      organization_id="org-001", name="示范经营主体")
        service.register_actor(request_id="req-admin", actor_id="bootstrap",
                               new_actor_id="admin-001", display_name="系统管理员",
                               role="admin", organization_id="org-001")
        service.register_actor(request_id="req-operator", actor_id="admin-001",
                               new_actor_id="operator-001", display_name="业务负责人",
                               role="operator", organization_id="org-001")
        service.register_site(request_id="req-site", actor_id="operator-001",
                              site_id="site-001", organization_id="org-001",
                              name="一号生产经营站点", timezone_name="Asia/Shanghai")
        first = service.record_domain_data(
            request_id="req-data", actor_id="operator-001", site_id="site-001",
            category="company_profile", external_key="record-001",
            data={"name": "基础资料", "enabled": True})
        replay = service.record_domain_data(
            request_id="req-data", actor_id="operator-001", site_id="site-001",
            category="company_profile", external_key="record-001",
            data={"name": "基础资料", "enabled": True})
        records = service.list_domain_data("site-001")

        night = _run_night_scenario(database, clock, service)
        order_id = night.pop("order_id")
        night.pop("reconcile")

        # 进程重启恢复：关闭后用新进程视角重新打开同一数据库。
        clock.advance(seconds=600)
        database.close()
        restarted_db = Database(db_path)
        restarted_fs = FulfillmentService(restarted_db, clock)
        recovery = ReconcileLoop(restarted_fs).startup_recovery()
        survived = restarted_fs.get_order(order_id)
        night["night_checks"]["restart_order_visible"] = survived["order_id"] == order_id
        night["night_checks"]["restart_qty_balanced"] = survived["qty_outstanding"] == 0
        night["night_checks"]["restart_status_partial"] = survived["status"] == "partial"
        valid, event_count = verify_chain(restarted_db.connection)
        restarted_db.close()

        result = {"status": "ok", "records": len(records), "audit_events": event_count,
                  "audit_valid": valid, "first_replayed": first.replayed,
                  "second_replayed": replay.replayed,
                  "restart_recovery": {"expired_orders": len(recovery["expired_orders"]),
                                       "picked_protected": recovery["picked_orders_protected"]},
                  **night}
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    checks = result["night_checks"]
    all_checks_ok = all(value is True or isinstance(value, (int, float, list, dict))
                        for value in checks.values())
    ok = (result["status"] == "ok" and result["audit_valid"]
          and not result["first_replayed"] and result["second_replayed"]
          and all_checks_ok)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
