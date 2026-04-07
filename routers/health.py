"""GET /api/v1/health"""

from fastapi import APIRouter, Request
from server.models.node import HealthResponse

router = APIRouter()


@router.get("/api/v1/health", response_model=HealthResponse)
async def health_check(request: Request):
    store = request.app.state.node_store
    return HealthResponse(status="ok", uptime=store.get_uptime(), node_count=store.get_node_count())
