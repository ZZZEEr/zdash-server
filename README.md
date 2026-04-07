# ZDash Server

Personal unified ops dashboard — server component.

Receives telemetry from agents deployed on your devices (routers, NAS, VMs, etc.)
and provides a REST API + WebSocket for real-time dashboard display.

## Features

- **Agent Ingest** — Receive system metrics via `POST /api/v1/report`
- **Node Overview** — `GET /api/v1/nodes` with status (online/stale/offline)
- **Rate Calculation** — Network KB/s and Disk I/O KB/s computed server-side
- **History** — Time-series storage with auto-archival and downsampling
- **WebSocket** — Real-time push to connected frontends
- **Alerts** — Configurable threshold rules with cooldown and history
- **Tags** — Label nodes for filtering (router, nas, vm, etc.)
- **Batch Ingest** — Submit multiple node reports in a single request
- **SQLite** — Zero-config persistence with WAL mode
- **Docker** — One-command deployment

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Clone and start
git clone https://github.com/yourname/zdash.git
cd zdash
docker-compose up -d

# Check health
curl http://localhost:8850/api/v1/health
```

### Option 2: Run Directly

```bash
cd zdash
pip install -r server/requirements.txt
python -m uvicorn server.main:app --host 0.0.0.0 --port 8850
```

### Run Tests

```bash
pip install -r server/requirements.txt
pytest server/tests/ -v
```

## API Reference

### Core Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/health` | Server health check |
| `POST` | `/api/v1/report` | Agent data ingest |
| `POST` | `/api/v1/report/batch` | Batch ingest (multiple nodes) |
| `GET` | `/api/v1/nodes` | Node list (`?tag=xxx` to filter) |
| `GET` | `/api/v1/nodes/{id}` | Node detail + computed rates |
| `GET` | `/api/v1/nodes/{id}/history` | History (`?hours=24&max_points=500`) |
| `PUT` | `/api/v1/nodes/{id}/tags` | Set node tags |
| `WS` | `/api/v1/ws` | Real-time WebSocket updates |

### Alert Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/alerts/rules` | List alert rules |
| `POST` | `/api/v1/alerts/rules` | Create alert rule |
| `DELETE` | `/api/v1/alerts/rules/{id}` | Delete alert rule |
| `GET` | `/api/v1/alerts/history` | Alert event history |
| `POST` | `/api/v1/alerts/history/{id}/ack` | Acknowledge alert |

### Interactive Docs

Open `http://localhost:8850/docs` for Swagger UI.

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `ZDASH_DB_PATH` | `zdash.db` | SQLite database file path |
| `ZDASH_PORT` | `8850` | Server port (docker-compose) |
| `TZ` | `Asia/Shanghai` | Timezone |

## Architecture

```
Agents (多台设备)           Server                    Frontend
┌──────────┐          ┌─────────────────┐          ┌──────────┐
│ OpenWrt  │─POST────▶│  FastAPI        │◀─GET/WS──│ Browser  │
│ NAS      │─────────▶│  ┌───────────┐  │          │          │
│ VM       │─────────▶│  │ Memory    │  │          └──────────┘
└──────────┘          │  │ Cache     │  │
                      │  └─────┬─────┘  │
                      │  ┌─────▼─────┐  │
                      │  │  SQLite   │  │
                      │  │  (WAL)    │  │
                      │  └───────────┘  │
                      │  Alert Engine   │
                      │  WS Manager     │
                      └─────────────────┘
```

## License

MIT