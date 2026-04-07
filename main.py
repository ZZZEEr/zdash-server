"""ZDash Server — Main application entry point (Phase 2)."""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from server.models.database import Database
from server.models.node import ErrorResponse, ErrorDetail
from server.services.node_store import NodeStore
from server.services.ws_manager import WSManager
from server.services.alert_engine import AlertEngine
from server.routers import ingest, nodes, health
from server.routers import websocket as ws_router
from server.routers import alerts as alerts_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("zdash.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = os.getenv("ZDASH_DB_PATH", "zdash.db")
    db = Database(db_path)

    store = NodeStore(database=db)
    store.initialize()

    ws_mgr = WSManager()
    alert_eng = AlertEngine(database=db)

    app.state.node_store = store
    app.state.ws_manager = ws_mgr
    app.state.alert_engine = alert_eng
    app.state.database = db

    logger.info(f"ZDash Server started (db={db_path})")
    yield
    db.close()
    logger.info("ZDash Server stopped")


app = FastAPI(title="ZDash Server", version="0.3.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

app.include_router(ingest.router, tags=["Ingest"])
app.include_router(nodes.router, tags=["Nodes"])
app.include_router(health.router, tags=["Health"])
app.include_router(ws_router.router, tags=["WebSocket"])
app.include_router(alerts_router.router, tags=["Alerts"])


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    if errors:
        first = errors[0]
        loc = " -> ".join(str(l) for l in first.get("loc", []))
        msg = first.get("msg", "Validation error")
        message = f"{msg} at {loc}" if loc else msg
    else:
        message = "Invalid request payload"
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(error=ErrorDetail(code="INVALID_PAYLOAD", message=message)).model_dump(),
    )
