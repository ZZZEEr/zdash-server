"""
NodeStore — Hybrid storage layer (Phase 2 enhanced).

Additions over Phase 1:
  - Tag management (get/set per node)
  - Downsampled history queries
  - Batch ingest support
  - Returns tags in node list/detail
"""

from __future__ import annotations

import json
import logging
import time
import threading
from typing import Any, Dict, List, Optional, Tuple

from server.models.node import (
    AgentReport,
    ComputedRates,
    DiskIORateDetail,
    HistoryDataPoint,
    NetRateDetail,
    NodeDetailResponse,
    NodeHistoryResponse,
    NodeListResponse,
    NodeSummary,
)
from server.models.database import Database, db as default_db

logger = logging.getLogger("zdash.node_store")

ONLINE_THRESHOLD = 90
STALE_THRESHOLD = 300
HISTORY_RETENTION_DAYS = 7
HISTORY_RETENTION_SECONDS = HISTORY_RETENTION_DAYS * 86400
CLEANUP_INTERVAL = 3600


class _NodeCache:
    def __init__(self):
        self.last_report: Dict[str, Any] = {}
        self.prev_report: Dict[str, Any] = {}
        self.computed: ComputedRates = ComputedRates()
        self.last_seen: float = 0.0
        self.tags: List[str] = []


class NodeStore:
    def __init__(self, database: Optional[Database] = None):
        self._db = database or default_db
        self._cache: Dict[str, _NodeCache] = {}
        self._lock = threading.RLock()
        self._last_cleanup: float = 0.0
        self._started_at: float = time.time()

    # ── Initialization ──

    def initialize(self) -> None:
        self._db.initialize()
        self._hydrate_cache()
        logger.info(f"NodeStore initialized, {len(self._cache)} nodes loaded from DB")

    def _hydrate_cache(self) -> None:
        rows = self._db.fetchall("SELECT * FROM nodes")
        with self._lock:
            for row in rows:
                node_id = row["node_id"]
                entry = _NodeCache()
                entry.last_seen = row["last_seen"]
                try:
                    entry.last_report = json.loads(row["last_report"])
                except (json.JSONDecodeError, TypeError):
                    entry.last_report = {}
                try:
                    computed_data = json.loads(row["computed"])
                    entry.computed = ComputedRates(**computed_data)
                except (json.JSONDecodeError, TypeError):
                    entry.computed = ComputedRates()

                # Load tags
                tag_rows = self._db.fetchall(
                    "SELECT tag FROM node_tags WHERE node_id = ? ORDER BY tag",
                    (node_id,),
                )
                entry.tags = [r["tag"] for r in tag_rows]

                self._cache[node_id] = entry

    # ── Ingest ──

    def ingest(self, report: AgentReport) -> Dict[str, Any]:
        """
        Process incoming agent report. Returns summary dict for WebSocket broadcast.
        """
        node_id = report.node.node_id
        report_dict = report.model_dump()
        now = time.time()

        with self._lock:
            entry = self._cache.get(node_id)
            if entry is None:
                entry = _NodeCache()
                # Load existing tags if node was in DB but not cache
                tag_rows = self._db.fetchall(
                    "SELECT tag FROM node_tags WHERE node_id = ? ORDER BY tag",
                    (node_id,),
                )
                entry.tags = [r["tag"] for r in tag_rows]
                self._cache[node_id] = entry

            entry.prev_report = entry.last_report
            entry.last_report = report_dict
            entry.last_seen = report.timestamp or now
            entry.computed = self._compute_rates(entry.prev_report, entry.last_report)

        root_disk_pct = self._get_root_disk_usage(report)
        total_rx_rate, total_tx_rate = self._get_total_net_rates(entry.computed)

        computed_json = entry.computed.model_dump_json()
        report_json = json.dumps(report_dict)

        self._db.execute(
            """
            INSERT INTO nodes
                (node_id, hostname, os, arch, status, last_seen, uptime_sec,
                 cpu_usage_pct, mem_usage_pct, disk_usage_pct,
                 net_rx_rate_kbps, net_tx_rate_kbps,
                 last_report, computed, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                hostname=excluded.hostname, os=excluded.os, arch=excluded.arch,
                status=excluded.status, last_seen=excluded.last_seen,
                uptime_sec=excluded.uptime_sec, cpu_usage_pct=excluded.cpu_usage_pct,
                mem_usage_pct=excluded.mem_usage_pct, disk_usage_pct=excluded.disk_usage_pct,
                net_rx_rate_kbps=excluded.net_rx_rate_kbps, net_tx_rate_kbps=excluded.net_tx_rate_kbps,
                last_report=excluded.last_report, computed=excluded.computed,
                updated_at=excluded.updated_at
            """,
            (
                node_id, report.node.hostname, report.node.os, report.node.arch,
                "online", entry.last_seen, report.uptime_sec,
                report.cpu.usage_pct, report.memory.usage_pct, root_disk_pct,
                total_rx_rate, total_tx_rate, report_json, computed_json, now,
            ),
        )

        self._db.execute(
            """
            INSERT INTO report_history
                (node_id, timestamp, cpu_usage_pct, mem_usage_pct, disk_usage_pct,
                 net_rx_rate_kbps, net_tx_rate_kbps, uptime_sec, report_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id, entry.last_seen, report.cpu.usage_pct,
                report.memory.usage_pct, root_disk_pct,
                total_rx_rate, total_tx_rate, report.uptime_sec, report_json,
            ),
        )

        self._maybe_cleanup(now)

        # Return data for alert evaluation and WS broadcast
        return {
            "node_id": node_id,
            "status": "online",
            "cpu_usage_pct": report.cpu.usage_pct,
            "mem_usage_pct": report.memory.usage_pct,
            "disk_usage_pct": root_disk_pct,
            "net_rx_rate_kbps": total_rx_rate,
            "net_tx_rate_kbps": total_tx_rate,
        }

    # ── Rate Computation ──

    def _compute_rates(self, prev: Dict[str, Any], curr: Dict[str, Any]) -> ComputedRates:
        net_rates = self._compute_net_rates(prev, curr)
        disk_rates = self._compute_disk_io_rates(prev, curr)
        return ComputedRates(net_rates=net_rates, disk_io_rates=disk_rates)

    def _compute_net_rates(self, prev: Dict[str, Any], curr: Dict[str, Any]) -> List[NetRateDetail]:
        if not prev or not curr:
            return []
        prev_ts = prev.get("timestamp", 0)
        curr_ts = curr.get("timestamp", 0)
        dt = curr_ts - prev_ts
        if dt <= 0:
            return []

        prev_net = {n["iface"]: n for n in prev.get("network", [])}
        rates = []
        for iface_data in curr.get("network", []):
            iface = iface_data.get("iface", "")
            if iface not in prev_net:
                continue
            prev_iface = prev_net[iface]
            rx_diff = max(0, iface_data.get("rx_bytes", 0) - prev_iface.get("rx_bytes", 0))
            tx_diff = max(0, iface_data.get("tx_bytes", 0) - prev_iface.get("tx_bytes", 0))
            rates.append(NetRateDetail(
                iface=iface,
                rx_rate_kbps=round(rx_diff / dt / 1024, 2),
                tx_rate_kbps=round(tx_diff / dt / 1024, 2),
            ))
        return rates

    def _compute_disk_io_rates(self, prev: Dict[str, Any], curr: Dict[str, Any]) -> List[DiskIORateDetail]:
        if not prev or not curr:
            return []
        prev_ts = prev.get("timestamp", 0)
        curr_ts = curr.get("timestamp", 0)
        dt = curr_ts - prev_ts
        if dt <= 0:
            return []

        prev_dio = {d["device"]: d for d in prev.get("disk_io", [])}
        rates = []
        for dio in curr.get("disk_io", []):
            device = dio.get("device", "")
            if device not in prev_dio:
                continue
            prev_d = prev_dio[device]
            read_diff = max(0, dio.get("read_bytes", 0) - prev_d.get("read_bytes", 0))
            write_diff = max(0, dio.get("write_bytes", 0) - prev_d.get("write_bytes", 0))
            rates.append(DiskIORateDetail(
                device=device,
                read_rate_kbps=round(read_diff / dt / 1024, 2),
                write_rate_kbps=round(write_diff / dt / 1024, 2),
            ))
        return rates

    # ── Helpers ──

    @staticmethod
    def _get_root_disk_usage(report: AgentReport) -> float:
        for disk in report.disks:
            if disk.mount == "/":
                return disk.usage_pct
        if report.disks:
            return report.disks[0].usage_pct
        return 0.0

    @staticmethod
    def _get_total_net_rates(computed: ComputedRates) -> Tuple[float, float]:
        total_rx = sum(r.rx_rate_kbps for r in computed.net_rates)
        total_tx = sum(r.tx_rate_kbps for r in computed.net_rates)
        return round(total_rx, 2), round(total_tx, 2)

    def _get_node_status(self, last_seen: float, now: Optional[float] = None) -> str:
        now = now or time.time()
        age = now - last_seen
        if age < ONLINE_THRESHOLD:
            return "online"
        elif age < STALE_THRESHOLD:
            return "stale"
        return "offline"

    # ── Tags ──

    def set_tags(self, node_id: str, tags: List[str]) -> bool:
        with self._lock:
            entry = self._cache.get(node_id)
            if entry is None:
                return False
            # Normalize tags
            clean_tags = sorted(set(t.strip().lower() for t in tags if t.strip()))
            entry.tags = clean_tags

        # Persist
        self._db.execute("DELETE FROM node_tags WHERE node_id = ?", (node_id,))
        if clean_tags:
            self._db.executemany(
                "INSERT INTO node_tags (node_id, tag) VALUES (?, ?)",
                [(node_id, tag) for tag in clean_tags],
            )
        return True

    def get_tags(self, node_id: str) -> Optional[List[str]]:
        with self._lock:
            entry = self._cache.get(node_id)
            if entry is None:
                return None
            return list(entry.tags)

    # ── Query ──

    def get_node_list(self, tag: Optional[str] = None) -> NodeListResponse:
        now = time.time()
        nodes: List[NodeSummary] = []

        with self._lock:
            for node_id, entry in self._cache.items():
                # Tag filter
                if tag and tag.lower() not in entry.tags:
                    continue

                report = entry.last_report
                node_info = report.get("node", {})
                cpu = report.get("cpu", {})
                memory = report.get("memory", {})

                total_rx, total_tx = self._get_total_net_rates(entry.computed)

                root_pct = 0.0
                for d in report.get("disks", []):
                    if d.get("mount") == "/":
                        root_pct = d.get("usage_pct", 0.0)
                        break
                else:
                    disks = report.get("disks", [])
                    if disks:
                        root_pct = disks[0].get("usage_pct", 0.0)

                nodes.append(NodeSummary(
                    node_id=node_id,
                    hostname=node_info.get("hostname", ""),
                    os=node_info.get("os", ""),
                    arch=node_info.get("arch", ""),
                    status=self._get_node_status(entry.last_seen, now),
                    last_seen=entry.last_seen,
                    uptime_sec=report.get("uptime_sec", 0),
                    cpu_usage_pct=cpu.get("usage_pct", 0),
                    mem_usage_pct=memory.get("usage_pct", 0),
                    disk_usage_pct=root_pct,
                    net_rx_rate_kbps=total_rx,
                    net_tx_rate_kbps=total_tx,
                    tags=list(entry.tags),
                ))

        return NodeListResponse(count=len(nodes), nodes=nodes)

    def get_node_detail(self, node_id: str) -> Optional[NodeDetailResponse]:
        with self._lock:
            entry = self._cache.get(node_id)
            if entry is None:
                return None
            now = time.time()
            return NodeDetailResponse(
                node_id=node_id,
                status=self._get_node_status(entry.last_seen, now),
                last_seen=entry.last_seen,
                last_report=entry.last_report,
                computed=entry.computed,
                tags=list(entry.tags),
            )

    def get_node_history(
        self,
        node_id: str,
        hours: float = 24,
        max_points: Optional[int] = None,
    ) -> Optional[NodeHistoryResponse]:
        with self._lock:
            if node_id not in self._cache:
                return None

        cutoff = time.time() - (hours * 3600)

        rows = self._db.fetchall(
            """
            SELECT timestamp, cpu_usage_pct, mem_usage_pct, disk_usage_pct,
                   net_rx_rate_kbps, net_tx_rate_kbps, uptime_sec
            FROM report_history
            WHERE node_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            """,
            (node_id, cutoff),
        )

        data = [
            HistoryDataPoint(
                timestamp=row["timestamp"],
                cpu_usage_pct=row["cpu_usage_pct"],
                mem_usage_pct=row["mem_usage_pct"],
                disk_usage_pct=row["disk_usage_pct"],
                net_rx_rate_kbps=row["net_rx_rate_kbps"],
                net_tx_rate_kbps=row["net_tx_rate_kbps"],
                uptime_sec=row["uptime_sec"],
            )
            for row in rows
        ]

        # Downsample if too many points
        if max_points and len(data) > max_points and max_points > 0:
            data = self._downsample(data, max_points)

        return NodeHistoryResponse(
            node_id=node_id,
            hours=hours,
            count=len(data),
            data=data,
        )

    @staticmethod
    def _downsample(data: List[HistoryDataPoint], target: int) -> List[HistoryDataPoint]:
        """
        Reduce data points to target count by averaging buckets.
        Always keeps the first and last points.
        """
        if len(data) <= target or target < 2:
            return data

        result = [data[0]]  # always keep first
        bucket_size = (len(data) - 1) / (target - 1)

        for i in range(1, target - 1):
            start = int(i * bucket_size)
            end = int((i + 1) * bucket_size)
            end = min(end, len(data))
            bucket = data[start:end]

            if not bucket:
                continue

            n = len(bucket)
            avg_point = HistoryDataPoint(
                timestamp=sum(p.timestamp for p in bucket) / n,
                cpu_usage_pct=round(sum(p.cpu_usage_pct for p in bucket) / n, 2),
                mem_usage_pct=round(sum(p.mem_usage_pct for p in bucket) / n, 2),
                disk_usage_pct=round(sum(p.disk_usage_pct for p in bucket) / n, 2),
                net_rx_rate_kbps=round(sum(p.net_rx_rate_kbps for p in bucket) / n, 2),
                net_tx_rate_kbps=round(sum(p.net_tx_rate_kbps for p in bucket) / n, 2),
                uptime_sec=round(sum(p.uptime_sec for p in bucket) / n, 2),
            )
            result.append(avg_point)

        result.append(data[-1])  # always keep last
        return result

    def get_node_count(self) -> int:
        with self._lock:
            return len(self._cache)

    def get_uptime(self) -> float:
        return round(time.time() - self._started_at, 1)

    # ── Data Cleanup ──

    def _maybe_cleanup(self, now: float) -> None:
        if now - self._last_cleanup < CLEANUP_INTERVAL:
            return
        self._last_cleanup = now
        self._run_cleanup(now)

    def _run_cleanup(self, now: float) -> None:
        cutoff_delete = now - HISTORY_RETENTION_SECONDS
        cutoff_archive = now - 3600

        result = self._db.execute(
            "DELETE FROM report_history WHERE timestamp < ?",
            (cutoff_delete,),
        )
        if result.rowcount:
            logger.info(f"Cleanup: deleted {result.rowcount} rows older than {HISTORY_RETENTION_DAYS}d")

        self._db.execute(
            """
            UPDATE report_history SET is_archived = 1
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY node_id, CAST(timestamp / 3600 AS INTEGER)
                        ORDER BY timestamp DESC
                    ) as rn
                    FROM report_history
                    WHERE is_archived = 0 AND timestamp < ?
                ) sub WHERE rn = 1
            )
            """,
            (cutoff_archive,),
        )

        result = self._db.execute(
            "DELETE FROM report_history WHERE is_archived = 0 AND timestamp < ?",
            (cutoff_archive,),
        )
        if result.rowcount:
            logger.info(f"Cleanup: archived, removed {result.rowcount} redundant rows")

    def force_cleanup(self) -> None:
        self._run_cleanup(time.time())