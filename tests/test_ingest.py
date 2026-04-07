"""
Unit tests for ZDash Server — Phase 1 功能测试（适配 Phase 2 架构）。
"""

import json
import os
import tempfile
import time

import pytest
from fastapi.testclient import TestClient

from server.models.database import Database
from server.services.node_store import NodeStore
from server.services.ws_manager import WSManager
from server.services.alert_engine import AlertEngine


@pytest.fixture
def app_client():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp_path = tmp.name
    tmp.close()

    db = None
    try:
        from fastapi import FastAPI
        from server.routers import ingest, nodes, health
        from server.routers import websocket as ws_router
        from server.routers import alerts as alerts_router
        from fastapi.exceptions import RequestValidationError
        from fastapi.responses import JSONResponse
        from server.models.node import ErrorResponse, ErrorDetail

        app = FastAPI()
        app.include_router(ingest.router)
        app.include_router(nodes.router)
        app.include_router(health.router)
        app.include_router(ws_router.router)
        app.include_router(alerts_router.router)

        @app.exception_handler(RequestValidationError)
        async def val_handler(request, exc):
            errors = exc.errors()
            if errors:
                first = errors[0]
                loc = " -> ".join(str(l) for l in first.get("loc", []))
                msg = first.get("msg", "Validation error")
                message = f"{msg} at {loc}" if loc else msg
            else:
                message = "Invalid request"
            return JSONResponse(
                status_code=400,
                content=ErrorResponse(
                    error=ErrorDetail(code="INVALID_PAYLOAD", message=message)
                ).model_dump(),
            )

        db = Database(tmp_path)
        store = NodeStore(database=db)
        store.initialize()
        ws_mgr = WSManager()
        alert_eng = AlertEngine(database=db)

        app.state.node_store = store
        app.state.ws_manager = ws_mgr
        app.state.alert_engine = alert_eng

        with TestClient(app) as client:
            yield client

    finally:
        if db is not None:
            db.close()
        try:
            os.unlink(tmp_path)
        except PermissionError:
            pass
        for suffix in ("-wal", "-shm"):
            try:
                p = tmp_path + suffix
                if os.path.exists(p):
                    os.unlink(p)
            except PermissionError:
                pass


def _make_report(
    node_id="test-node-01",
    hostname="TestHost",
    timestamp=None,
    cpu_pct=25.0,
    mem_pct=60.0,
    net_rx=1000000,
    net_tx=500000,
    disk_read=2000000,
    disk_write=1000000,
):
    return {
        "node": {
            "node_id": node_id,
            "hostname": hostname,
            "os": "Linux 6.1",
            "arch": "x86_64",
        },
        "timestamp": timestamp or time.time(),
        "uptime_sec": 86400,
        "cpu": {
            "count": 4,
            "usage_pct": cpu_pct,
            "load_avg": [1.0, 0.8, 0.5],
        },
        "memory": {
            "total_mb": 4096,
            "used_mb": 4096 * mem_pct / 100,
            "usage_pct": mem_pct,
        },
        "disks": [
            {
                "device": "/dev/sda1",
                "mount": "/",
                "fs_type": "ext4",
                "total_mb": 50000,
                "used_mb": 20000,
                "usage_pct": 40.0,
            }
        ],
        "disk_io": [
            {
                "device": "/dev/sda1",
                "read_bytes": disk_read,
                "write_bytes": disk_write,
            }
        ],
        "network": [
            {
                "iface": "eth0",
                "rx_bytes": net_rx,
                "tx_bytes": net_tx,
                "rx_packets": 10000,
                "tx_packets": 5000,
            }
        ],
        "extras": {"custom_key": "custom_value"},
    }


class TestHealthCheck:
    def test_health_returns_ok(self, app_client):
        resp = app_client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "uptime" in data
        assert "node_count" in data
        assert data["node_count"] == 0

    def test_health_node_count_updates(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.get("/api/v1/health")
        assert resp.json()["node_count"] == 1


class TestReportIngest:
    def test_basic_ingest(self, app_client):
        report = _make_report()
        resp = app_client.post("/api/v1/report", json=report)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["node_id"] == "test-node-01"

    def test_ingest_missing_node_id(self, app_client):
        bad_report = _make_report()
        del bad_report["node"]["node_id"]
        resp = app_client.post("/api/v1/report", json=bad_report)
        assert resp.status_code == 400
        data = resp.json()
        assert data["error"]["code"] == "INVALID_PAYLOAD"

    def test_ingest_empty_body(self, app_client):
        resp = app_client.post("/api/v1/report", json={})
        assert resp.status_code == 400

    def test_ingest_multiple_nodes(self, app_client):
        app_client.post("/api/v1/report", json=_make_report(node_id="node-a"))
        app_client.post("/api/v1/report", json=_make_report(node_id="node-b"))
        app_client.post("/api/v1/report", json=_make_report(node_id="node-c"))

        resp = app_client.get("/api/v1/nodes")
        data = resp.json()
        assert data["count"] == 3
        node_ids = {n["node_id"] for n in data["nodes"]}
        assert node_ids == {"node-a", "node-b", "node-c"}

    def test_ingest_updates_existing_node(self, app_client):
        t1 = time.time()
        app_client.post(
            "/api/v1/report",
            json=_make_report(node_id="node-x", cpu_pct=10.0, timestamp=t1),
        )

        t2 = t1 + 30
        app_client.post(
            "/api/v1/report",
            json=_make_report(node_id="node-x", cpu_pct=50.0, timestamp=t2),
        )

        resp = app_client.get("/api/v1/nodes")
        assert resp.json()["count"] == 1

        resp = app_client.get("/api/v1/nodes/node-x")
        assert resp.status_code == 200
        detail = resp.json()
        assert detail["last_report"]["cpu"]["usage_pct"] == 50.0


class TestNodeList:
    def test_empty_list(self, app_client):
        resp = app_client.get("/api/v1/nodes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["nodes"] == []

    def test_node_summary_fields(self, app_client):
        app_client.post("/api/v1/report", json=_make_report(
            cpu_pct=33.3, mem_pct=77.7
        ))
        resp = app_client.get("/api/v1/nodes")
        node = resp.json()["nodes"][0]

        assert node["node_id"] == "test-node-01"
        assert node["hostname"] == "TestHost"
        assert node["os"] == "Linux 6.1"
        assert node["arch"] == "x86_64"
        assert node["status"] == "online"
        assert node["cpu_usage_pct"] == 33.3
        assert node["mem_usage_pct"] == 77.7
        assert node["disk_usage_pct"] == 40.0
        assert "last_seen" in node
        assert "uptime_sec" in node


class TestNodeDetail:
    def test_detail_existing_node(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.get("/api/v1/nodes/test-node-01")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "test-node-01"
        assert data["status"] == "online"
        assert "last_report" in data
        assert "computed" in data
        assert "net_rates" in data["computed"]
        assert "disk_io_rates" in data["computed"]

    def test_detail_nonexistent_node(self, app_client):
        resp = app_client.get("/api/v1/nodes/does-not-exist")
        assert resp.status_code == 404
        data = resp.json()
        assert data["error"]["code"] == "NODE_NOT_FOUND"

    def test_detail_has_full_report(self, app_client):
        report = _make_report()
        app_client.post("/api/v1/report", json=report)
        resp = app_client.get("/api/v1/nodes/test-node-01")
        data = resp.json()

        lr = data["last_report"]
        assert lr["node"]["node_id"] == "test-node-01"
        assert lr["cpu"]["count"] == 4
        assert len(lr["network"]) == 1
        assert lr["extras"]["custom_key"] == "custom_value"


class TestRateComputation:
    def test_net_rates_computed_on_second_report(self, app_client):
        t1 = time.time()
        t2 = t1 + 10

        app_client.post("/api/v1/report", json=_make_report(
            timestamp=t1, net_rx=1000000, net_tx=500000
        ))
        app_client.post("/api/v1/report", json=_make_report(
            timestamp=t2, net_rx=1102400, net_tx=551200
        ))

        resp = app_client.get("/api/v1/nodes/test-node-01")
        computed = resp.json()["computed"]

        assert len(computed["net_rates"]) == 1
        eth0_rate = computed["net_rates"][0]
        assert eth0_rate["iface"] == "eth0"
        assert eth0_rate["rx_rate_kbps"] == 10.0
        assert eth0_rate["tx_rate_kbps"] == 5.0

    def test_disk_io_rates(self, app_client):
        t1 = time.time()
        t2 = t1 + 10

        app_client.post("/api/v1/report", json=_make_report(
            timestamp=t1, disk_read=2000000, disk_write=1000000
        ))
        app_client.post("/api/v1/report", json=_make_report(
            timestamp=t2, disk_read=2102400, disk_write=1051200
        ))

        resp = app_client.get("/api/v1/nodes/test-node-01")
        computed = resp.json()["computed"]

        assert len(computed["disk_io_rates"]) == 1
        sda = computed["disk_io_rates"][0]
        assert sda["device"] == "/dev/sda1"
        assert sda["read_rate_kbps"] == 10.0
        assert sda["write_rate_kbps"] == 5.0

    def test_no_rates_on_first_report(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.get("/api/v1/nodes/test-node-01")
        computed = resp.json()["computed"]
        assert computed["net_rates"] == []
        assert computed["disk_io_rates"] == []

    def test_summary_net_rates(self, app_client):
        t1 = time.time()
        t2 = t1 + 10

        app_client.post("/api/v1/report", json=_make_report(
            timestamp=t1, net_rx=0, net_tx=0
        ))
        app_client.post("/api/v1/report", json=_make_report(
            timestamp=t2, net_rx=102400, net_tx=51200
        ))

        resp = app_client.get("/api/v1/nodes")
        node = resp.json()["nodes"][0]
        assert node["net_rx_rate_kbps"] == 10.0
        assert node["net_tx_rate_kbps"] == 5.0


class TestHistory:
    def test_history_basic(self, app_client):
        base_time = time.time()

        for i in range(5):
            app_client.post("/api/v1/report", json=_make_report(
                timestamp=base_time + i * 60,
                cpu_pct=20.0 + i * 5,
            ))

        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "test-node-01"
        assert data["hours"] == 1
        assert data["count"] == 5
        assert len(data["data"]) == 5

        timestamps = [d["timestamp"] for d in data["data"]]
        assert timestamps == sorted(timestamps)

        cpu_values = [d["cpu_usage_pct"] for d in data["data"]]
        assert cpu_values == [20.0, 25.0, 30.0, 35.0, 40.0]

    def test_history_nonexistent_node(self, app_client):
        resp = app_client.get("/api/v1/nodes/ghost-node/history")
        assert resp.status_code == 404

    def test_history_hours_filter(self, app_client):
        now = time.time()

        app_client.post("/api/v1/report", json=_make_report(
            timestamp=now - 7200, cpu_pct=10.0
        ))
        app_client.post("/api/v1/report", json=_make_report(
            timestamp=now, cpu_pct=50.0
        ))

        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1")
        data = resp.json()
        assert data["count"] == 1
        assert data["data"][0]["cpu_usage_pct"] == 50.0

        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=3")
        data = resp.json()
        assert data["count"] == 2

    def test_history_default_hours(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.get("/api/v1/nodes/test-node-01/history")
        data = resp.json()
        assert data["hours"] == 24

    def test_history_data_point_fields(self, app_client):
        app_client.post("/api/v1/report", json=_make_report(
            cpu_pct=45.5, mem_pct=72.3
        ))
        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1")
        point = resp.json()["data"][0]

        required_fields = [
            "timestamp", "cpu_usage_pct", "mem_usage_pct",
            "disk_usage_pct", "net_rx_rate_kbps", "net_tx_rate_kbps", "uptime_sec"
        ]
        for field in required_fields:
            assert field in point, f"Missing field: {field}"

        assert point["cpu_usage_pct"] == 45.5
        assert point["mem_usage_pct"] == 72.3


class TestCleanup:
    def test_force_cleanup_runs_without_error(self, app_client):
        store = app_client.app.state.node_store
        store.force_cleanup()

    def test_cleanup_preserves_recent_data(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        store = app_client.app.state.node_store
        store.force_cleanup()

        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1")
        assert resp.json()["count"] >= 1


class TestEdgeCases:
    def test_concurrent_node_ingest(self, app_client):
        for i in range(10):
            resp = app_client.post(
                "/api/v1/report",
                json=_make_report(node_id=f"node-{i:03d}", hostname=f"Host-{i}"),
            )
            assert resp.status_code == 200

        resp = app_client.get("/api/v1/nodes")
        assert resp.json()["count"] == 10

    def test_report_with_minimal_fields(self, app_client):
        minimal = {
            "node": {"node_id": "minimal-node"},
        }
        resp = app_client.post("/api/v1/report", json=minimal)
        assert resp.status_code == 200
        assert resp.json()["node_id"] == "minimal-node"

        resp = app_client.get("/api/v1/nodes")
        assert resp.json()["count"] == 1

    def test_large_extras_field(self, app_client):
        report = _make_report()
        report["extras"] = {
            "docker_containers": ["nginx", "redis", "postgres"],
            "custom_metrics": {"temp_celsius": 42.5},
        }
        resp = app_client.post("/api/v1/report", json=report)
        assert resp.status_code == 200

        detail = app_client.get("/api/v1/nodes/test-node-01").json()
        assert detail["last_report"]["extras"]["docker_containers"] == [
            "nginx", "redis", "postgres"
        ]

    def test_invalid_json_body(self, app_client):
        resp = app_client.post(
            "/api/v1/report",
            content="not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code in (400, 422)