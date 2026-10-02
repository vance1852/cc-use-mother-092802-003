"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .fulfillment import FulfillmentService
from .service import DomainService
from .storage import Database


def route(service: DomainService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None,
          fulfillment: FulfillmentService | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    parts = [segment for segment in parsed.path.split("/") if segment]
    try:
        if method == "GET" and parsed.path == "/health":
            valid, count = service.verify_audit()
            return 200, {"status": "ok", "audit_valid": valid, "audit_events": count}
        if method == "POST" and parsed.path == "/organizations":
            receipt = service.register_organization(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/actors":
            receipt = service.register_actor(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/sites":
            receipt = service.register_site(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/domain-records":
            receipt = service.record_domain_data(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "GET" and parsed.path == "/domain-records":
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            category = query.get("category", [None])[0]
            return 200, {"items": [item.__dict__ for item in service.list_domain_data(site_id, category)]}
        if method == "GET" and parsed.path == "/audit-events":
            query = parse_qs(parsed.query)
            after = int(query.get("after_sequence", ["0"])[0])
            return 200, {"items": service.audit_events(after)}
        if fulfillment is not None:
            status, payload = _route_fulfillment(fulfillment, method, parsed, parts, body, actor_id)
            if status is not None:
                return status, payload
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def _route_fulfillment(fs: FulfillmentService, method: str, parsed, parts: list[str],
                       body: dict[str, Any], actor_id: str) -> tuple[int | None, dict[str, Any]]:
    query = parse_qs(parsed.query)
    replayed_status = lambda result: 200 if result.get("replayed") else 201  # noqa: E731

    if method == "POST":
        simple_posts = {
            "/channels": fs.register_channel,
            "/products": fs.register_product,
            "/stores": fs.register_store,
            "/batches": fs.upsert_batch,
            "/brand-channel-rules": fs.set_brand_channel_rule,
            "/brand-region-rules": fs.set_brand_region_rule,
            "/substitution-rules": fs.set_substitution_rule,
            "/couriers": fs.register_courier,
            "/quotes": fs.quote,
        }
        if parsed.path in simple_posts:
            result = simple_posts[parsed.path](actor_id=actor_id, **body)
            return replayed_status(result), result
        if parsed.path == "/orders/confirm":
            result = fs.confirm_order(actor_id=actor_id, **body)
            return replayed_status(result), result
        if parsed.path == "/orders/cancel":
            result = fs.cancel_order(actor_id=actor_id, **body)
            return replayed_status(result), result
        if parsed.path == "/deliveries/pick":
            result = fs.mark_picked(actor_id=actor_id, **body)
            return replayed_status(result), result
        if parsed.path == "/deliveries/complete":
            result = fs.complete_delivery(actor_id=actor_id, **body)
            return replayed_status(result), result
        if parsed.path == "/reconcile":
            return 200, fs.reconcile()
    if method == "GET" and len(parts) == 2 and parts[0] == "orders":
        return 200, fs.get_order(parts[1])
    if method == "GET" and parsed.path == "/batches":
        store_id = query.get("store_id", [""])[0]
        if not store_id:
            raise ValidationError("store_id 不能为空")
        return 200, {"items": fs.batch_snapshot(store_id)}
    return None, {}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: DomainService
    fulfillment: FulfillmentService | None = None

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")},
                                fulfillment=self.fulfillment)
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动本地 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动夜间即时零售履约服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--reconcile-interval", type=float, default=5.0,
                        help="超时回收轮询间隔（秒）；0 表示关闭后台轮询")
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = DomainService(database)
    fulfillment = FulfillmentService(database)
    Handler.fulfillment = fulfillment

    # 进程启动即恢复：释放重启期间到期的占位，保留已取货订单。
    recovery = fulfillment.reconcile()

    loop = None
    if args.reconcile_interval > 0:
        from .maintenance import ReconcileLoop

        loop = ReconcileLoop(fulfillment, interval_seconds=args.reconcile_interval)
        loop.start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if loop is not None:
            loop.stop(timeout=2)
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
