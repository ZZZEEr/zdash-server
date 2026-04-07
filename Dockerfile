"""
ZDash Server — Main application with enhanced error handling and graceful shutdown.
"""

import logging
import os
import signal
import sys
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

# Configure logging
log_level = os.getenv("ZDASH_LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("zdash.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle with graceful handling."""
    db_path = os.getenv("ZDASH_DB_PATH", "zdash.db")
    db = Database(db_path)

    try:
        store = NodeStore(database=db)
        store.initialize()

        ws_mgr = WSManager()
        alert_eng = AlertEngine(database=db)

        app.state.node_store = store
        app.state.ws_manager = ws_mgr
        app.state.alert_engine = alert_eng
        app.state.database = db

        logger.info(f"ZDash Server started (db={db_path}, log_level={log_level})")
        logger.info(f"WebSocket manager: {ws_mgr.connection_count} connections")

        yield

    except Exception as e:
        logger.error(f"Startup error: {e}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down ZDash Server...")
        try:
            db.close()
            logger.info("Database closed")
        except Exception as e:
            logger.error(f"Error closing database: {e}")


app = FastAPI(
    title="ZDash Server",
    description="Personal unified ops dashboard — server component",
    version="0.3.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ZDASH_CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(ingest.router, tags=["Ingest"])
app.include_router(nodes.router, tags=["Nodes"])
app.include_router(health.router, tags=["Health"])
app.include_router(ws_router.router, tags=["WebSocket"])
app.include_router(alerts_router.router, tags=["Alerts"])


# Global exception handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle Pydantic validation errors."""
    errors = exc.errors()
    if errors:
        first = errors[0]
        loc = " -> ".join(str(l) for l in first.get("loc", []))
        msg = first.get("msg", "Validation error")
        message = f"{msg} at {loc}" if loc else msg
    else:
        message = "Invalid request payload"

    logger.warning(f"Validation error: {message}")
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            error=ErrorDetail(code="INVALID_PAYLOAD", message=message)
        ).model_dump(),
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error=ErrorDetail(code="INTERNAL_ERROR", message="Internal server error")
        ).model_dump(),
    )


# Graceful shutdown signal handlers
def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)