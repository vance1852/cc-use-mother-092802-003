"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    brand_id TEXT NOT NULL,
    spec_name TEXT NOT NULL,
    tier TEXT NOT NULL,
    requires_cold INTEGER NOT NULL CHECK(requires_cold IN (0, 1)),
    list_price_cents INTEGER NOT NULL CHECK(list_price_cents >= 0),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stores (
    store_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    name TEXT NOT NULL,
    region_id TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS store_batches (
    batch_id TEXT PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    product_id TEXT NOT NULL REFERENCES products(product_id),
    lot_no TEXT NOT NULL,
    qty_on_hand INTEGER NOT NULL CHECK(qty_on_hand >= 0),
    qty_held INTEGER NOT NULL DEFAULT 0 CHECK(qty_held >= 0),
    qty_picked INTEGER NOT NULL DEFAULT 0 CHECK(qty_picked >= 0),
    qty_sold INTEGER NOT NULL DEFAULT 0 CHECK(qty_sold >= 0),
    cold_chain_ok INTEGER NOT NULL CHECK(cold_chain_ok IN (0, 1)),
    production_date TEXT,
    expiry_date TEXT,
    near_expiry_days INTEGER NOT NULL CHECK(near_expiry_days >= 0),
    channel_tags_json TEXT NOT NULL,
    unit_price_cents INTEGER NOT NULL CHECK(unit_price_cents >= 0),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_id, product_id, lot_no),
    CHECK(qty_held + qty_picked + qty_sold <= qty_on_hand)
);
CREATE TABLE IF NOT EXISTS brand_channel_rules (
    brand_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    allowed INTEGER NOT NULL CHECK(allowed IN (0, 1)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(brand_id, channel_id)
);
CREATE TABLE IF NOT EXISTS brand_region_rules (
    brand_id TEXT NOT NULL,
    region_id TEXT NOT NULL,
    allowed INTEGER NOT NULL CHECK(allowed IN (0, 1)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(brand_id, region_id)
);
CREATE TABLE IF NOT EXISTS substitution_rules (
    product_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    substitute_product_id TEXT NOT NULL,
    priority INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(product_id, channel_id, substitute_product_id)
);
CREATE TABLE IF NOT EXISTS couriers (
    courier_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    region_id TEXT NOT NULL,
    capacity_units INTEGER NOT NULL CHECK(capacity_units >= 0),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quotes (
    quote_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    region_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    store_id TEXT,
    feasible INTEGER NOT NULL CHECK(feasible IN (0, 1)),
    status TEXT NOT NULL CHECK(status IN ('offered', 'converted', 'expired')),
    total_qty INTEGER NOT NULL,
    total_cents INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    quote_id TEXT NOT NULL REFERENCES quotes(quote_id),
    customer_id TEXT NOT NULL,
    region_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('held', 'picked', 'partial', 'delivered', 'cancelled')),
    total_qty INTEGER NOT NULL CHECK(total_qty >= 0),
    total_cents INTEGER NOT NULL CHECK(total_cents >= 0),
    refunded_cents INTEGER NOT NULL DEFAULT 0 CHECK(refunded_cents >= 0),
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finalized_at TEXT
);
CREATE TABLE IF NOT EXISTS order_lines (
    line_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    product_id TEXT NOT NULL,
    requested_product_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('primary', 'substitute')),
    batch_id TEXT NOT NULL,
    unit_price_cents INTEGER NOT NULL CHECK(unit_price_cents >= 0),
    qty_committed INTEGER NOT NULL CHECK(qty_committed > 0),
    qty_delivered INTEGER NOT NULL DEFAULT 0 CHECK(qty_delivered >= 0),
    qty_refunded INTEGER NOT NULL DEFAULT 0 CHECK(qty_refunded >= 0),
    refunded_cents INTEGER NOT NULL DEFAULT 0 CHECK(refunded_cents >= 0),
    CHECK(qty_delivered + qty_refunded <= qty_committed)
);
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    courier_id TEXT NOT NULL REFERENCES couriers(courier_id),
    units INTEGER NOT NULL CHECK(units > 0),
    status TEXT NOT NULL CHECK(status IN ('planned', 'picked', 'delivered', 'cancelled')),
    planned_at TEXT NOT NULL,
    picked_at TEXT,
    delivered_at TEXT,
    cancelled_at TEXT
);
CREATE TABLE IF NOT EXISTS delivery_items (
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    line_id TEXT NOT NULL REFERENCES order_lines(line_id),
    qty INTEGER NOT NULL CHECK(qty > 0),
    PRIMARY KEY(delivery_id, line_id)
);
CREATE TABLE IF NOT EXISTS order_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_product ON store_batches(store_id, product_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_lines_order ON order_lines(order_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_order ON deliveries(order_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_courier ON deliveries(courier_id, status);
CREATE INDEX IF NOT EXISTS idx_delivery_items_line ON delivery_items(line_id);
CREATE INDEX IF NOT EXISTS idx_quotes_status ON quotes(status, expires_at);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 10000")
        if self.path != ":memory:":
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.executescript(SCHEMA)

    @property
    def lock(self) -> threading.RLock:
        """暴露连接级互斥锁，供需要跨语句串行化的调用方使用。"""

        return self._lock

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交；同一连接上的事务按锁串行。"""

        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield self.connection
            except Exception:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()

    @contextmanager
    def reading(self) -> Iterator[sqlite3.Connection]:
        """为多线程下的只读查询提供连接互斥。"""

        with self._lock:
            yield self.connection

    def close(self) -> None:
        """关闭底层连接。"""

        with self._lock:
            if self.path != ":memory:":
                try:
                    self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
            self.connection.close()
