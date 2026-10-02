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
-- 夜间即时零售履约 ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS skus (
    sku_id TEXT PRIMARY KEY,
    brand_id TEXT NOT NULL,
    product_line TEXT NOT NULL,
    spec TEXT NOT NULL,
    display_name TEXT NOT NULL,
    unit_price_milli INTEGER NOT NULL CHECK(unit_price_milli >= 0),
    requires_cold INTEGER NOT NULL CHECK(requires_cold IN (0, 1)),
    shelf_life_days INTEGER NOT NULL CHECK(shelf_life_days > 0),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    version INTEGER NOT NULL CHECK(version >= 1),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_rules (
    brand_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    allowed_specs_json TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    version INTEGER NOT NULL CHECK(version >= 1),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(brand_id, channel)
);
CREATE TABLE IF NOT EXISTS substitution_rules (
    brand_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    from_sku TEXT NOT NULL,
    rank INTEGER NOT NULL,
    to_sku TEXT NOT NULL,
    PRIMARY KEY(brand_id, channel, from_sku, rank)
);
CREATE TABLE IF NOT EXISTS stores (
    store_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    region_id TEXT NOT NULL,
    sales_regions_json TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    version INTEGER NOT NULL CHECK(version >= 1),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory_batches (
    batch_id TEXT PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    sku_id TEXT NOT NULL REFERENCES skus(sku_id),
    quantity_on_hand INTEGER NOT NULL CHECK(quantity_on_hand >= 0),
    production_date TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    chilled INTEGER NOT NULL CHECK(chilled IN (0, 1)),
    data_version INTEGER NOT NULL CHECK(data_version >= 1),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS couriers (
    courier_id TEXT PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    region_id TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1))
);
CREATE TABLE IF NOT EXISTS courier_shifts (
    shift_id TEXT PRIMARY KEY,
    courier_id TEXT NOT NULL REFERENCES couriers(courier_id),
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK(capacity > 0),
    active INTEGER NOT NULL CHECK(active IN (0, 1))
);
CREATE TABLE IF NOT EXISTS promises (
    promise_id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    parent_id TEXT,
    channel TEXT NOT NULL,
    region_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    customer_location TEXT NOT NULL,
    status TEXT NOT NULL,
    shift_id TEXT,
    courier_id TEXT,
    expires_at TEXT NOT NULL,
    promised_from TEXT NOT NULL,
    promised_to TEXT NOT NULL,
    total_milli INTEGER NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS promise_items (
    promise_id TEXT NOT NULL REFERENCES promises(promise_id),
    line_no INTEGER NOT NULL,
    sku_id TEXT NOT NULL,
    requested_sku TEXT NOT NULL,
    substituted INTEGER NOT NULL CHECK(substituted IN (0, 1)),
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    unit_price_milli INTEGER NOT NULL,
    line_total_milli INTEGER NOT NULL,
    explain_json TEXT NOT NULL,
    PRIMARY KEY(promise_id, line_no)
);
CREATE TABLE IF NOT EXISTS inventory_holds (
    hold_id TEXT PRIMARY KEY,
    promise_id TEXT NOT NULL REFERENCES promises(promise_id),
    batch_id TEXT NOT NULL REFERENCES inventory_batches(batch_id),
    sku_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_holds_batch ON inventory_holds(batch_id, state);
CREATE INDEX IF NOT EXISTS idx_holds_promise ON inventory_holds(promise_id);
CREATE TABLE IF NOT EXISTS capacity_reservations (
    reservation_id TEXT PRIMARY KEY,
    promise_id TEXT NOT NULL REFERENCES promises(promise_id),
    shift_id TEXT NOT NULL REFERENCES courier_shifts(shift_id),
    courier_id TEXT NOT NULL,
    slots INTEGER NOT NULL CHECK(slots > 0),
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_res_shift ON capacity_reservations(shift_id, state);
CREATE TABLE IF NOT EXISTS order_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    promise_id TEXT NOT NULL,
    request_id TEXT,
    kind TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_promise ON order_events(promise_id, sequence);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        if self.path != ":memory:":
            self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.executescript(SCHEMA)
        self.write_lock = threading.RLock()

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        with self.write_lock:
            self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield self.connection
            except Exception:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
