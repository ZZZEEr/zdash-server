"""
Pydantic data models for ZDash — Phase 1 + Phase 2.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# Phase 1 Models

class CpuInfo(BaseModel):
    count: int = 0
    usage_pct: float = 0.0
    load_avg: List[float] = Field(default_factory=lambda: [0.0, 0.0, 0.0])


class MemoryInfo(BaseModel):
    total_mb: float = 0.0
    used_mb: float = 0.0
    usage_pct: float = 0.0


class DiskPartition(BaseModel):
    device: str = ""
    mount: str = ""
    fs_type: str = ""
    total_mb: float = 0.0
    used_mb: float = 0.0
    usage_pct: float = 0.0


class DiskIO(BaseModel):
    device: str = ""
    read_bytes: int = 0
    write_bytes: int = 0


class NetInterface(BaseModel):
    iface: str = ""
    rx_bytes: int = 0
    tx_bytes: int = 0
    rx_packets: int = 0
    tx_packets: int = 0


class NodeInfo(BaseModel):
    node_id: str
    hostname: str = ""
    os: str = ""
    arch: str = ""


class AgentReport(BaseModel):
    node: NodeInfo
    timestamp: float = Field(default_factory=time.time)
    uptime_sec: float = 0.0
    cpu: CpuInfo = Field(default_factory=CpuInfo)
    memory: MemoryInfo = Field(default_factory=MemoryInfo)
    disks: List[DiskPartition] = Field(default_factory=list)
    disk_io: List[DiskIO] = Field(default_factory=list)
    network: List[NetInterface] = Field(default_factory=list)
    extras: Dict[str, Any] = Field(default_factory=dict)


class NetRateDetail(BaseModel):
    iface: str
    rx_rate_kbps: float = 0.0
    tx_rate_kbps: float = 0.0


class DiskIORateDetail(BaseModel):
    device: str
    read_rate_kbps: float = 0.0
    write_rate_kbps: float = 0.0


class ComputedRates(BaseModel):
    net_rates: List[NetRateDetail] = Field(default_factory=list)
    disk_io_rates: List[DiskIORateDetail] = Field(default_factory=list)


class NodeSummary(BaseModel):
    node_id: str
    hostname: str = ""
    os: str = ""
    arch: str = ""
    status: str = "offline"
    last_seen: float = 0.0
    uptime_sec: float = 0.0
    cpu_usage_pct: float = 0.0
    mem_usage_pct: float = 0.0
    disk_usage_pct: float = 0.0
    net_rx_rate_kbps: float = 0.0
    net_tx_rate_kbps: float = 0.0
    tags: List[str] = Field(default_factory=list)


class NodeListResponse(BaseModel):
    count: int
    nodes: List[NodeSummary]


class NodeDetailResponse(BaseModel):
    node_id: str
    status: str = "offline"
    last_seen: float = 0.0
    last_report: Dict[str, Any] = Field(default_factory=dict)
    computed: ComputedRates = Field(default_factory=ComputedRates)
    tags: List[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str = "ok"
    uptime: float = 0.0
    node_count: int = 0


class ReportAccepted(BaseModel):
    status: str = "accepted"
    node_id: str


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class HistoryDataPoint(BaseModel):
    timestamp: float
    cpu_usage_pct: float = 0.0
    mem_usage_pct: float = 0.0
    disk_usage_pct: float = 0.0
    net_rx_rate_kbps: float = 0.0
    net_tx_rate_kbps: float = 0.0
    uptime_sec: float = 0.0


class NodeHistoryResponse(BaseModel):
    node_id: str
    hours: float
    count: int
    data: List[HistoryDataPoint]


# Phase 2 New Models

class BatchReportRequest(BaseModel):
    reports: List[AgentReport]


class BatchReportResult(BaseModel):
    node_id: str
    status: str = "accepted"
    error: Optional[str] = None


class BatchReportResponse(BaseModel):
    total: int
    accepted: int
    failed: int
    results: List[BatchReportResult]


class TagsUpdateRequest(BaseModel):
    tags: List[str]


class TagsResponse(BaseModel):
    node_id: str
    tags: List[str]


class AlertRuleCreate(BaseModel):
    name: str = ""
    metric: str
    operator: str = "gt"
    threshold: float = 0.0
    node_id: Optional[str] = None
    enabled: bool = True


class AlertRule(BaseModel):
    id: int
    name: str = ""
    metric: str
    operator: str = "gt"
    threshold: float = 0.0
    node_id: Optional[str] = None
    enabled: bool = True
    created_at: float = 0.0
    updated_at: float = 0.0


class AlertRuleListResponse(BaseModel):
    count: int
    rules: List[AlertRule]


class AlertEvent(BaseModel):
    id: int
    rule_id: int
    rule_name: str = ""
    node_id: str
    metric: str
    threshold: float = 0.0
    actual_value: float = 0.0
    message: str = ""
    triggered_at: float = 0.0
    acknowledged: bool = False
    acked_at: Optional[float] = None


class AlertHistoryResponse(BaseModel):
    count: int
    alerts: List[AlertEvent]


class AlertAckResponse(BaseModel):
    id: int
    acknowledged: bool = True
    acked_at: float = 0.0


class WSMessage(BaseModel):
    type: str
    data: Dict[str, Any] = Field(default_factory=dict)
