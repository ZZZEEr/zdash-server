"""
SQLite database with connection pooling and cross-platform support.
"""

import sqlite3
import os
import threading
import logging
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
from queue import Queue, Empty

logger = logging.getLogger("zdash.database")

DEFAULT_DB_PATH = os.getenv("ZDASH_DB_PATH", "zdash.db")
SCHEMA_VERSION = 2

# Connection pool settings
POOL_SIZE = int(os.getenv("ZDASH_DB_POOL_SIZE", "5"))
POOL_TIMEOUT = float(os.getenv("ZDASH_DB_POOL_TIMEOUT", "30.0"))

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id         TEXT PRIMARY KEY,
    hostname        TEXT NOT NULL DEFAULT '',
    os              TEXT NOT NULL DEFAULT '',
    arch            TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'offline',
    last_seen       REAL NOT NULL DEFAULT 0,
    uptime_sec      REAL NOT NULL DEFAULT 0,
    cpu_usage_pct   REAL NOT NULL DEFAULT 0,
    mem_usage_pct   REAL NOT NULL DEFAULT 0,
    disk_usage_pct  REAL NOT NULL DEFAULT 0,
    net_rx_rate_kbps REAL NOT NULL DEFAULT 0,
    net_tx_rate_kbps REAL NOT NULL DEFAULT 0,
    last_report     TEXT NOT NULL DEFAULT '{}',
    computed        TEXT NOT NULL DEFAULT '{}',
    updated_at      REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS report_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id         TEXT NOT NULL,
    timestamp       REAL NOT NULL,
    cpu_usage_pct   REAL NOT NULL DEFAULT 0,
    mem_usage_pct   REAL NOT NULL DEFAULT 0,
    disk_usage_pct  REAL NOT NULL DEFAULT 0,
    net_rx_rate_kbps REAL NOT NULL DEFAULT 0,
    net_tx_rate_kbps REAL NOT NULL DEFAULT 0,
    uptime_sec      REAL NOT NULL DEFAULT 0,
    report_json     TEXT NOT NULL DEFAULT '{}',
    is_archived     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_history_node_time
    ON report_history (node_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_history_archived
    ON report_history (is_archived, timestamp);

CREATE TABLE IF NOT EXISTS alert_rules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL DEFAULT '',
    metric          TEXT NOT NULL,
    operator        TEXT NOT NULL DEFAULT 'gt',
    threshold       REAL NOT NULL DEFAULT 0,
    node_id         TEXT DEFAULT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      REAL NOT NULL DEFAULT 0,
    updated_at      REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS alert_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id         INTEGER NOT NULL,
    rule_name       TEXT NOT NULL DEFAULT '',
    node_id         TEXT NOT NULL,
    metric          TEXT NOT NULL,
    threshold       REAL NOT NULL DEFAULT 0,
    actual_value    REAL NOT NULL DEFAULT 0,
    message         TEXT NOT NULL DEFAULT '',
    triggered_at    REAL NOT NULL DEFAULT 0,
    acknowledged    INTEGER NOT NULL DEFAULT 0,
    acked_at        REAL DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_history_time
    ON alert_history (triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_history_node
    ON alert_history (node_id, triggered_at DESC);

CREATE TABLE IF NOT EXISTS node_tags (
    node_id         TEXT NOT NULL,
    tag             TEXT NOT NULL,
    PRIMARY KEY (node_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_node_tags_tag
    ON node_tags (tag);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class ConnectionPool:
    """Simple connection pool for SQLite."""

    def __init__(self, db_path: str, pool_size: int = 5):
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool: Queue = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._initialized = False

        # Pre-create connections
        for _ in range(pool_size):
            conn = self._create_connection()
            self._pool.put(conn)

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection with proper settings."""
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool."""
        try:
            return self._pool.get(timeout=POOL_TIMEOUT)
        except Empty:
            logger.warning("Connection pool exhausted, creating new connection")
            return self._create_connection()

    def return_connection(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool."""
        try:
            self._pool.put(conn, block=False)
        except Exception:
            conn.close()

    def close_all(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break


class Database:
    """Database manager with connection pooling and cross-platform support."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        # Normalize path for cross-platform compatibility
        self.db_path = str(Path(db_path).resolve())
        self._pool: Optional[ConnectionPool] = None
        self._initialized = False
        self._init_lock = threading.Lock()

        logger.info(f"Database path: {self.db_path}")

    def initialize(self) -> None:
        """Initialize database and connection pool."""
        with self._init_lock:
            if self._initialized:
                return

            # Ensure directory exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

            # Create connection pool
            self._pool = ConnectionPool(self.db_path, pool_size=POOL_SIZE)

            # Initialize schema
            conn = self._pool.get_connection()
            try:
                conn.executescript(SCHEMA_SQL)
                conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                    ("schema_version", str(SCHEMA_VERSION)),
                )
                conn.commit()
                logger.info(f"Database initialized (schema v{SCHEMA_VERSION})")
            finally:
                self._pool.return_connection(conn)

            self._initialized = True

    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor."""
        if not self._pool:
            raise RuntimeError("Database not initialized")

        conn = self._pool.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.return_connection(conn)

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute SQL and return cursor."""
        if not self._pool:
            raise RuntimeError("Database not initialized")

        conn = self._pool.get_connection()
        try:
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor
        finally:
            self._pool.return_connection(conn)

    def executemany(self, sql: str, params_list: list) -> None:
        """Execute multiple SQL statements."""
        if not self._pool:
            raise RuntimeError("Database not initialized")

        conn = self._pool.get_connection()
        try:
            conn.executemany(sql, params_list)
            conn.commit()
        finally:
            self._pool.return_connection(conn)

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """Fetch one row."""
        if not self._pool:
            raise RuntimeError("Database not initialized")

        conn = self._pool.get_connection()
        try:
            return conn.execute(sql, params).fetchone()
        finally:
            self._pool.return_connection(conn)

    def fetchall(self, sql: str, params: tuple = ()) -> list:
        """Fetch all rows."""
        if not self._pool:
            raise RuntimeError("Database not initialized")

        conn = self._pool.get_connection()
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            self._pool.return_connection(conn)

    def close(self) -> None:
        """Close all connections."""
        if self._pool:
            self._pool.close_all()
            logger.info("Database connections closed")


db = Database()