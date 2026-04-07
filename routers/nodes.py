"""Node query endpoints."""

from typing import Optional

from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse

from server.models.node import (
    NodeListResponse, NodeDetailResponse, NodeHistoryResponse,
    TagsUpdateRequest, TagsResponse, ErrorResponse, ErrorDetail,
)

router = APIRouter()


@router.get("/api/v1/nodes", response_model=NodeListResponse)
async def get_nodes(request: Request, tag: Optional[str] = Query(default=None)):
    store = request.app.state.node_store
    return store.get_node_list(tag=tag)


@router.get("/api/v1/nodes/{node_id}", response_model=NodeDetailResponse, responses={404: {"model": ErrorResponse}})
async def get_node_detail(node_id: str, request: Request):
    store = request.app.state.node_store
    detail = store.get_node_detail(node_id)
    if detail is None:
        return JSONResponse(
            status_code=404,
            content=ErrorResponse(error=ErrorDetail(code="NODE_NOT_FOUND", message=f"Node \'{node_id}\' not found")).model_dump(),
        )
    return detail


@router.get("/api/v1/nodes/{node_id}/history", response_model=NodeHistoryResponse, responses={404: {"model": ErrorResponse}})
async def get_node_history(
    node_id: str, request: Request,
    hours: float = Query(default=24, ge=0.1, le=168),
    max_points: Optional[int] = Query(default=None, ge=10, le=10000),
):
    store = request.app.state.node_store
    history = store.get_node_history(node_id, hours=hours, max_points=max_points)
    if history is None:
        return JSONResponse(
            status_code=404,
            content=ErrorResponse(error=ErrorDetail(code="NODE_NOT_FOUND", message=f"Node \'{node_id}\' not found")).model_dump(),
        )
    return history


@router.put("/api/v1/nodes/{node_id}/tags", response_model=TagsResponse, responses={404: {"model": ErrorResponse}})
async def update_node_tags(node_id: str, body: TagsUpdateRequest, request: Request):
    store = request.app.state.node_store
    success = store.set_tags(node_id, body.tags)
    if not success:
        return JSONResponse(
            status_code=404,
            content=ErrorResponse(error=ErrorDetail(code="NODE_NOT_FOUND", message=f"Node \'{node_id}\' not found")).model_dump(),
        )
    tags = store.get_tags(node_id) or []
    return TagsResponse(node_id=node_id, tags=tags)
