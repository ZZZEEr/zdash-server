"""
Report adapter — converts various Agent JSON formats to the
canonical format expected by Server Pydantic models.

Handles:
  - Field name differences (report_ts → timestamp, networks → network, etc.)
  - Unit conversions (total_kb → total_mb)
  - Structural differences (load_1/5/15 → load_avg array)
  - disk_io extraction from disks array
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger("zdash.adapter")


def adapt_report(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert raw Agent JSON to canonical Server format.
    If already in canonical format, returns as-is.
    """
    # Quick check: if "node.node_id" exists and "timestamp" exists,
    # it's likely already canonical format
    node = raw.get("node", {})
    if "timestamp" in raw and "network" in raw and "total_mb" in raw.get("memory", {}):
        return raw  # Already canonical

    logger.debug("Adapting non-canonical report format")

    adapted = {}

    # ── node info ──
    adapted_node = {
        "node_id": node.get("node_id", ""),
        "hostname": node.get("hostname", ""),
        "os": node.get("os", ""),
        "arch": node.get("arch", ""),
    }
    adapted["node"] = adapted_node

    # ── timestamp ──
    # Agent sends "report_ts", canonical is "timestamp"
    adapted["timestamp"] = raw.get("report_ts", raw.get("timestamp", 0))

    # ── uptime ──
    # Agent puts uptime inside node, canonical is top-level
    adapted["uptime_sec"] = node.get("uptime_sec", raw.get("uptime_sec", 0))

    # ── CPU ──
    cpu_raw = raw.get("cpu", {})
    load_avg = cpu_raw.get("load_avg", None)
    if load_avg is None:
        # Agent sends load_1, load_5, load_15 separately
        load_avg = [
            cpu_raw.get("load_1", 0.0),
            cpu_raw.get("load_5", 0.0),
            cpu_raw.get("load_15", 0.0),
        ]
    adapted["cpu"] = {
        "count": cpu_raw.get("count", 0),
        "usage_pct": cpu_raw.get("usage_pct", 0.0),
        "load_avg": load_avg,
    }

    # ── Memory ──
    mem_raw = raw.get("memory", {})
    # Agent sends KB, canonical is MB
    total_mb = mem_raw.get("total_mb", 0)
    used_mb = mem_raw.get("used_mb", 0)

    if total_mb == 0 and "total_kb" in mem_raw:
        total_mb = round(mem_raw["total_kb"] / 1024, 1)
    if used_mb == 0 and "used_kb" in mem_raw:
        used_mb = round(mem_raw["used_kb"] / 1024, 1)

    # If still 0, try to compute from available_kb
    if used_mb == 0 and "available_kb" in mem_raw and "total_kb" in mem_raw:
        used_mb = round((mem_raw["total_kb"] - mem_raw["available_kb"]) / 1024, 1)

    adapted["memory"] = {
        "total_mb": total_mb,
        "used_mb": used_mb,
        "usage_pct": mem_raw.get("usage_pct", 0.0),
    }

    # ── Disks ──
    disks_raw = raw.get("disks", [])
    adapted_disks = []
    adapted_disk_io = []

    for d in disks_raw:
        # Disk partition info
        adapted_disks.append({
            "device": d.get("device", ""),
            "mount": d.get("mount", ""),
            "fs_type": d.get("fs_type", ""),
            "total_mb": d.get("total_mb", 0),
            "used_mb": d.get("used_mb", 0),
            "usage_pct": d.get("usage_pct", 0.0),
        })

        # Extract disk IO if embedded in disk entry
        # Agent sends io_read_kb / io_write_kb inside each disk
        io_read = d.get("io_read_kb", 0)
        io_write = d.get("io_write_kb", 0)
        read_bytes = d.get("read_bytes", io_read * 1024)
        write_bytes = d.get("write_bytes", io_write * 1024)

        if read_bytes > 0 or write_bytes > 0:
            adapted_disk_io.append({
                "device": d.get("device", ""),
                "read_bytes": int(read_bytes),
                "write_bytes": int(write_bytes),
            })

    adapted["disks"] = adapted_disks

    # Also check for standalone disk_io array (canonical format)
    if raw.get("disk_io"):
        adapted["disk_io"] = raw["disk_io"]
    else:
        adapted["disk_io"] = adapted_disk_io

    # ── Network ──
    # Agent sends "networks" (with s), canonical is "network" (no s)
    net_raw = raw.get("networks", raw.get("network", []))
    adapted_network = []

    for n in net_raw:
        adapted_network.append({
            "iface": n.get("iface", ""),
            "rx_bytes": n.get("rx_bytes", 0),
            "tx_bytes": n.get("tx_bytes", 0),
            "rx_packets": n.get("rx_packets", 0),
            "tx_packets": n.get("tx_packets", 0),
        })

    adapted["network"] = adapted_network

    # ── Extras ──
    # Preserve any extra fields Agent might send
    extras = raw.get("extras", {})

    # Also save Agent-specific fields that don't map to canonical
    if "agent_version" in raw:
        extras["agent_version"] = raw["agent_version"]
    if "kernel" in node:
        extras["kernel"] = node["kernel"]
    if "swap_total_kb" in mem_raw:
        extras["swap_total_kb"] = mem_raw["swap_total_kb"]
        extras["swap_used_kb"] = mem_raw.get("swap_used_kb", 0)
    if "buffers_kb" in mem_raw:
        extras["buffers_kb"] = mem_raw["buffers_kb"]
        extras["cached_kb"] = mem_raw.get("cached_kb", 0)

    adapted["extras"] = extras

    return adapted