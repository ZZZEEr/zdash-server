"""
Unit tests for ZDash Server Phase 2 features.
- WebSocket push
- Alert rules and history
- Tags
- Batch ingest
- History downsampling
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
    disk_pct=40.0,
    net_rx=1000000,
    net_tx=500000,
    disk_read=2000000,
    disk_write=1000000,
):
    return {
        "node": {"node_id": node_id, "hostname": hostname, "os": "Linux 6.1", "arch": "x86_64"},
        "timestamp": timestamp or time.time(),
        "uptime_sec": 86400,
        "cpu": {"count": 4, "usage_pct": cpu_pct, "load_avg": [1.0, 0.8, 0.5]},
        "memory": {"total_mb": 4096, "used_mb": 4096 * mem_pct / 100, "usage_pct": mem_pct},
        "disks": [{"device": "/dev/sda1", "mount": "/", "fs_type": "ext4",
                    "total_mb": 50000, "used_mb": 50000 * disk_pct / 100, "usage_pct": disk_pct}],
        "disk_io": [{"device": "/dev/sda1", "read_bytes": disk_read, "write_bytes": disk_write}],
        "network": [{"iface": "eth0", "rx_bytes": net_rx, "tx_bytes": net_tx,
                      "rx_packets": 10000, "tx_packets": 5000}],
        "extras": {},
    }


# ══════════════════════════════════════
# Tags Tests
# ══════════════════════════════════════

class TestTags:
    def test_set_and_get_tags(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.put(
            "/api/v1/nodes/test-node-01/tags",
            json={"tags": ["router", "production"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "test-node-01"
        assert sorted(data["tags"]) == ["production", "router"]

    def test_tags_in_node_list(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        app_client.put("/api/v1/nodes/test-node-01/tags", json={"tags": ["nas"]})

        resp = app_client.get("/api/v1/nodes")
        node = resp.json()["nodes"][0]
        assert "nas" in node["tags"]

    def test_tags_in_node_detail(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        app_client.put("/api/v1/nodes/test-node-01/tags", json={"tags": ["vm", "dev"]})

        resp = app_client.get("/api/v1/nodes/test-node-01")
        assert sorted(resp.json()["tags"]) == ["dev", "vm"]

    def test_filter_by_tag(self, app_client):
        app_client.post("/api/v1/report", json=_make_report(node_id="router-1"))
        app_client.post("/api/v1/report", json=_make_report(node_id="nas-1"))
        app_client.post("/api/v1/report", json=_make_report(node_id="vm-1"))

        app_client.put("/api/v1/nodes/router-1/tags", json={"tags": ["router"]})
        app_client.put("/api/v1/nodes/nas-1/tags", json={"tags": ["nas"]})
        app_client.put("/api/v1/nodes/vm-1/tags", json={"tags": ["vm"]})

        resp = app_client.get("/api/v1/nodes?tag=router")
        data = resp.json()
        assert data["count"] == 1
        assert data["nodes"][0]["node_id"] == "router-1"

        resp = app_client.get("/api/v1/nodes")
        assert resp.json()["count"] == 3

    def test_tags_nonexistent_node(self, app_client):
        resp = app_client.put("/api/v1/nodes/ghost/tags", json={"tags": ["x"]})
        assert resp.status_code == 404

    def test_tags_normalized(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.put(
            "/api/v1/nodes/test-node-01/tags",
            json={"tags": [" Router ", "ROUTER", "nas", ""]},
        )
        tags = resp.json()["tags"]
        assert tags == ["nas", "router"]  # deduplicated, lowercase, sorted

    def test_clear_tags(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        app_client.put("/api/v1/nodes/test-node-01/tags", json={"tags": ["a", "b"]})
        resp = app_client.put("/api/v1/nodes/test-node-01/tags", json={"tags": []})
        assert resp.json()["tags"] == []


# ══════════════════════════════════════
# Batch Ingest Tests
# ══════════════════════════════════════

class TestBatchIngest:
    def test_batch_basic(self, app_client):
        reports = [
            _make_report(node_id=f"batch-{i}") for i in range(5)
        ]
        resp = app_client.post("/api/v1/report/batch", json={"reports": reports})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 5
        assert data["accepted"] == 5
        assert data["failed"] == 0

        resp = app_client.get("/api/v1/nodes")
        assert resp.json()["count"] == 5

    def test_batch_empty(self, app_client):
        resp = app_client.post("/api/v1/report/batch", json={"reports": []})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0

    def test_batch_results_per_node(self, app_client):
        reports = [_make_report(node_id=f"n-{i}") for i in range(3)]
        resp = app_client.post("/api/v1/report/batch", json={"reports": reports})
        results = resp.json()["results"]
        assert len(results) == 3
        for r in results:
            assert r["status"] == "accepted"


# ══════════════════════════════════════
# Alert Rules Tests
# ══════════════════════════════════════

class TestAlertRules:
    def test_create_rule(self, app_client):
        resp = app_client.post("/api/v1/alerts/rules", json={
            "name": "High CPU",
            "metric": "cpu_usage_pct",
            "operator": "gt",
            "threshold": 90.0,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] >= 1
        assert data["name"] == "High CPU"
        assert data["metric"] == "cpu_usage_pct"
        assert data["threshold"] == 90.0

    def test_list_rules(self, app_client):
        app_client.post("/api/v1/alerts/rules", json={
            "name": "R1", "metric": "cpu_usage_pct", "threshold": 80,
        })
        app_client.post("/api/v1/alerts/rules", json={
            "name": "R2", "metric": "mem_usage_pct", "threshold": 90,
        })
        resp = app_client.get("/api/v1/alerts/rules")
        data = resp.json()
        assert data["count"] == 2

    def test_delete_rule(self, app_client):
        resp = app_client.post("/api/v1/alerts/rules", json={
            "name": "Temp", "metric": "cpu_usage_pct", "threshold": 50,
        })
        rule_id = resp.json()["id"]

        resp = app_client.delete(f"/api/v1/alerts/rules/{rule_id}")
        assert resp.status_code == 200

        resp = app_client.get("/api/v1/alerts/rules")
        assert resp.json()["count"] == 0

    def test_delete_nonexistent_rule(self, app_client):
        resp = app_client.delete("/api/v1/alerts/rules/9999")
        assert resp.status_code == 404

    def test_invalid_metric(self, app_client):
        resp = app_client.post("/api/v1/alerts/rules", json={
            "name": "Bad", "metric": "invalid_metric", "threshold": 50,
        })
        assert resp.status_code == 400

    def test_invalid_operator(self, app_client):
        resp = app_client.post("/api/v1/alerts/rules", json={
            "name": "Bad", "metric": "cpu_usage_pct", "operator": "xxx", "threshold": 50,
        })
        assert resp.status_code == 400

    def test_rule_with_specific_node(self, app_client):
        resp = app_client.post("/api/v1/alerts/rules", json={
            "name": "Node Specific",
            "metric": "mem_usage_pct",
            "threshold": 80,
            "node_id": "server-01",
        })
        assert resp.status_code == 200
        assert resp.json()["node_id"] == "server-01"


# ══════════════════════════════════════
# Alert Triggering Tests
# ══════════════════════════════════════

class TestAlertTriggering:
    def test_alert_triggers_on_threshold(self, app_client):
        # Create rule: CPU > 80%
        app_client.post("/api/v1/alerts/rules", json={
            "name": "High CPU",
            "metric": "cpu_usage_pct",
            "operator": "gt",
            "threshold": 80.0,
        })

        # Report with CPU = 95%
        app_client.post("/api/v1/report", json=_make_report(cpu_pct=95.0))

        # Check alert history
        resp = app_client.get("/api/v1/alerts/history")
        data = resp.json()
        assert data["count"] >= 1
        alert = data["alerts"][0]
        assert alert["metric"] == "cpu_usage_pct"
        assert alert["actual_value"] == 95.0
        assert alert["node_id"] == "test-node-01"

    def test_no_alert_below_threshold(self, app_client):
        app_client.post("/api/v1/alerts/rules", json={
            "name": "High CPU", "metric": "cpu_usage_pct", "threshold": 80.0,
        })
        app_client.post("/api/v1/report", json=_make_report(cpu_pct=50.0))

        resp = app_client.get("/api/v1/alerts/history")
        assert resp.json()["count"] == 0

    def test_acknowledge_alert(self, app_client):
        app_client.post("/api/v1/alerts/rules", json={
            "name": "X", "metric": "cpu_usage_pct", "threshold": 10.0,
        })
        app_client.post("/api/v1/report", json=_make_report(cpu_pct=50.0))

        history = app_client.get("/api/v1/alerts/history").json()
        alert_id = history["alerts"][0]["id"]

        resp = app_client.post(f"/api/v1/alerts/history/{alert_id}/ack")
        assert resp.status_code == 200
        data = resp.json()
        assert data["acknowledged"] is True
        assert data["acked_at"] > 0

    def test_ack_nonexistent_alert(self, app_client):
        resp = app_client.post("/api/v1/alerts/history/9999/ack")
        assert resp.status_code == 404

    def test_alert_filter_by_node(self, app_client):
        app_client.post("/api/v1/alerts/rules", json={
            "name": "X", "metric": "cpu_usage_pct", "threshold": 10.0,
        })
        app_client.post("/api/v1/report", json=_make_report(node_id="a", cpu_pct=50))
        app_client.post("/api/v1/report", json=_make_report(node_id="b", cpu_pct=50))

        resp = app_client.get("/api/v1/alerts/history?node_id=a")
        assert resp.json()["count"] == 1
        assert resp.json()["alerts"][0]["node_id"] == "a"

    def test_rule_specific_node_only(self, app_client):
        # Rule only for node "target"
        app_client.post("/api/v1/alerts/rules", json={
            "name": "Targeted",
            "metric": "cpu_usage_pct",
            "threshold": 10.0,
            "node_id": "target",
        })
        # Report from different node
        app_client.post("/api/v1/report", json=_make_report(node_id="other", cpu_pct=99))
        # Report from target node
        app_client.post("/api/v1/report", json=_make_report(node_id="target", cpu_pct=99))

        resp = app_client.get("/api/v1/alerts/history")
        alerts = resp.json()["alerts"]
        assert len(alerts) == 1
        assert alerts[0]["node_id"] == "target"


# ══════════════════════════════════════
# History Downsampling Tests
# ══════════════════════════════════════

class TestDownsampling:
    def test_downsample_reduces_points(self, app_client):
        base = time.time()
        for i in range(100):
            app_client.post("/api/v1/report", json=_make_report(
                timestamp=base + i * 10,
                cpu_pct=20 + (i % 30),
            ))

        # Without limit
        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1")
        full_count = resp.json()["count"]
        assert full_count == 100

        # With limit
        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1&max_points=20")
        limited = resp.json()
        assert limited["count"] == 20

    def test_downsample_preserves_first_last(self, app_client):
        base = time.time()
        for i in range(50):
            app_client.post("/api/v1/report", json=_make_report(
                timestamp=base + i * 10,
                cpu_pct=float(i),
            ))

        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1&max_points=10")
        data = resp.json()["data"]

        # First point should be close to original first
        assert data[0]["cpu_usage_pct"] == 0.0
        # Last point should be close to original last
        assert data[-1]["cpu_usage_pct"] == 49.0

    def test_no_downsample_when_under_limit(self, app_client):
        base = time.time()
        for i in range(5):
            app_client.post("/api/v1/report", json=_make_report(
                timestamp=base + i * 10,
            ))

        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1&max_points=100")
        assert resp.json()["count"] == 5


# ══════════════════════════════════════
# WebSocket Tests
# ══════════════════════════════════════

class TestWebSocket:
    def test_ws_connect_and_receive(self, app_client):
        with app_client.websocket_connect("/api/v1/ws") as ws:
            # Send a report, which should trigger a WS broadcast
            app_client.post("/api/v1/report", json=_make_report())

            # We should receive a node_update message
            data = ws.receive_json()
            assert data["type"] == "node_update"
            assert "data" in data
            assert data["data"]["count"] >= 1

    def test_ws_ping_pong(self, app_client):
        with app_client.websocket_connect("/api/v1/ws") as ws:
            ws.send_text("ping")
            resp = ws.receive_json()
            assert resp["type"] == "pong"

    def test_ws_alert_broadcast(self, app_client):
        # Create rule
        app_client.post("/api/v1/alerts/rules", json={
            "name": "WS Alert",
            "metric": "cpu_usage_pct",
            "threshold": 50.0,
        })

        with app_client.websocket_connect("/api/v1/ws") as ws:
            # Report that triggers the alert
            app_client.post("/api/v1/report", json=_make_report(cpu_pct=80))

            # Should get node_update first
            msg1 = ws.receive_json()
            assert msg1["type"] == "node_update"

            # Then alert
            msg2 = ws.receive_json()
            assert msg2["type"] == "alert"
            assert msg2["data"]["metric"] == "cpu_usage_pct"


# ══════════════════════════════════════
# Phase 1 Backward Compatibility
# ══════════════════════════════════════

class TestBackwardCompat:
    """Ensure Phase 1 behavior is preserved."""

    def test_basic_ingest_still_works(self, app_client):
        resp = app_client.post("/api/v1/report", json=_make_report())
        assert resp.status_code == 200
        assert resp.json()["status"] == "accepted"

    def test_node_list_still_works(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.get("/api/v1/nodes")
        data = resp.json()
        assert data["count"] == 1
        node = data["nodes"][0]
        # All Phase 1 fields still present
        for field in ["node_id", "hostname", "os", "arch", "status",
                       "last_seen", "uptime_sec", "cpu_usage_pct",
                       "mem_usage_pct", "disk_usage_pct",
                       "net_rx_rate_kbps", "net_tx_rate_kbps"]:
            assert field in node

    def test_node_detail_still_works(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.get("/api/v1/nodes/test-node-01")
        data = resp.json()
        assert "last_report" in data
        assert "computed" in data

    def test_history_still_works(self, app_client):
        app_client.post("/api/v1/report", json=_make_report())
        resp = app_client.get("/api/v1/nodes/test-node-01/history?hours=1")
        data = resp.json()
        assert data["count"] >= 1

    def test_health_still_works(self, app_client):
        resp = app_client.get("/api/v1/health")
        assert resp.json()["status"] == "ok"

    def test_404_still_works(self, app_client):
        resp = app_client.get("/api/v1/nodes/nonexistent")
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "NODE_NOT_FOUND"

    def test_validation_error_still_works(self, app_client):
        resp = app_client.post("/api/v1/report", json={})
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "INVALID_PAYLOAD"