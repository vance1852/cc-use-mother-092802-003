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
          headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
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
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        result = {"error": exc.code, "message": str(exc)}
        if getattr(exc, "details", None):
            result["details"] = exc.details
        return exc.status, result
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def route_fulfillment(fs: FulfillmentService, method: str, path: str,
                      body: dict[str, Any] | None) -> tuple[int, dict[str, Any]]:
    """分派夜间即时零售履约请求。"""

    body = body or {}
    parsed = urlparse(path)
    try:
        if method == "POST" and parsed.path == "/fulfillment/quotes":
            return 200, fs.quote(**body)
        if method == "POST" and parsed.path == "/fulfillment/confirms":
            result = fs.confirm(**body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/fulfillment/picks":
            return 200, fs.mark_picked(**body)
        if method == "POST" and parsed.path == "/fulfillment/delivery-receipts":
            return 200, fs.delivery_receipt(**body)
        if method == "POST" and parsed.path == "/fulfillment/cancellations":
            return 200, fs.cancel(**body)
        if method == "POST" and parsed.path == "/fulfillment/splits":
            return 200, fs.split(**body)
        if method == "POST" and parsed.path == "/fulfillment/sweep-expirations":
            return 200, {"expired": fs.expire_stale_promises()}
        if method == "GET" and parsed.path == "/fulfillment/pending":
            return 200, {"items": fs.pending_promises()}
        if method == "GET" and parsed.path == "/fulfillment/reconcile":
            return 200, fs.reconcile()
        if method == "GET" and parsed.path.startswith("/fulfillment/promises/"):
            promise_id = parsed.path.rsplit("/", 1)[-1]
            return 200, fs.get_promise(promise_id)
        # 主数据维护
        if method == "POST" and parsed.path == "/fulfillment/admin/skus":
            fs.upsert_sku(**body)
            return 200, {"status": "ok"}
        if method == "POST" and parsed.path == "/fulfillment/admin/channel-rules":
            fs.upsert_channel_rule(**body)
            return 200, {"status": "ok"}
        if method == "POST" and parsed.path == "/fulfillment/admin/substitutions":
            fs.upsert_substitution(**body)
            return 200, {"status": "ok"}
        if method == "POST" and parsed.path == "/fulfillment/admin/stores":
            fs.upsert_store(**body)
            return 200, {"status": "ok"}
        if method == "POST" and parsed.path == "/fulfillment/admin/batches":
            fs.upsert_batch(**body)
            return 200, {"status": "ok"}
        if method == "POST" and parsed.path == "/fulfillment/admin/couriers":
            fs.upsert_courier(**body)
            return 200, {"status": "ok"}
        if method == "POST" and parsed.path == "/fulfillment/admin/shifts":
            fs.upsert_shift(**body)
            return 200, {"status": "ok"}
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        result = {"error": exc.code, "message": str(exc)}
        if getattr(exc, "details", None):
            result["details"] = exc.details
        return exc.status, result
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    protocol_version = "HTTP/1.1"
    service: DomainService
    fulfillment: FulfillmentService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        try:
            path = self.path
            if path.startswith("/fulfillment/"):
                status, payload = route_fulfillment(self.fulfillment, self.command, path, body)
            else:
                status, payload = route(self.service, self.command, path, body,
                                        {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
        except Exception as exc:  # noqa: BLE001 - 兜底，避免连接被重置
            status, payload = 500, {"error": "internal_error", "message": str(exc)}
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

    import threading

    parser = argparse.ArgumentParser(description="启动夜间即时零售履约服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--sweep-interval", type=float, default=5.0,
                        help="超时承诺扫描间隔（秒）")
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = DomainService(database)
    Handler.fulfillment = FulfillmentService(database)

    stop_sweeper = threading.Event()

    def sweeper() -> None:
        while not stop_sweeper.wait(args.sweep_interval):
            try:
                Handler.fulfillment.expire_stale_promises()
            except Exception:
                # 单次扫描失败不影响服务与下一次扫描
                continue

    sweep_thread = threading.Thread(target=sweeper, name="promise-sweeper", daemon=True)
    sweep_thread.start()

    class HighBacklogServer(ThreadingHTTPServer):
        # 夜间高峰瞬时并发可能超过默认连接队列（5）
        request_queue_size = 256
        daemon_threads = True

    server = HighBacklogServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_sweeper.set()
        server.shutdown()
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
