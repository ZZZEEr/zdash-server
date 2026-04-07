"""Alert management endpoints."""

from typing import Optional

from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse

from server.models.node import (
    AlertRuleCreate, AlertRule, AlertRuleListResponse,
    AlertHistoryResponse, AlertEvent, AlertAckResponse,
    ErrorResponse, ErrorDetail,
)

router = APIRouter()


@router.get("/api/v1/alerts/rules", response_model=AlertRuleListResponse)
async def list_alert_rules(request: Request):
    engine = request.app.state.alert_engine
    rules = engine.get_rules()
    return AlertRuleListResponse(count=len(rules), rules=[AlertRule(**r) for r in rules])


@router.post("/api/v1/alerts/rules", response_model=AlertRule, responses={400: {"model": ErrorResponse}})
async def create_alert_rule(body: AlertRuleCreate, request: Request):
    engine = request.app.state.alert_engine
    try:
        rule = engine.create_rule(
            name=body.name, metric=body.metric, operator=body.operator,
            threshold=body.threshold, node_id=body.node_id, enabled=body.enabled,
        )
        return AlertRule(**rule)
    except ValueError as e:
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(error=ErrorDetail(code="INVALID_RULE", message=str(e))).model_dump(),
        )


@router.delete("/api/v1/alerts/rules/{rule_id}", responses={404: {"model": ErrorResponse}})
async def delete_alert_rule(rule_id: int, request: Request):
    engine = request.app.state.alert_engine
    deleted = engine.delete_rule(rule_id)
    if not deleted:
        return JSONResponse(
            status_code=404,
            content=ErrorResponse(error=ErrorDetail(code="RULE_NOT_FOUND", message=f"Rule #{rule_id} not found")).model_dump(),
        )
    return {"status": "deleted", "rule_id": rule_id}


@router.get("/api/v1/alerts/history", response_model=AlertHistoryResponse)
async def get_alert_history(
    request: Request,
    node_id: Optional[str] = Query(default=None),
    hours: float = Query(default=24, ge=0.1, le=720),
    limit: int = Query(default=100, ge=1, le=1000),
):
    engine = request.app.state.alert_engine
    alerts = engine.get_history(node_id=node_id, hours=hours, limit=limit)
    return AlertHistoryResponse(count=len(alerts), alerts=[AlertEvent(**a) for a in alerts])


@router.post("/api/v1/alerts/history/{alert_id}/ack", response_model=AlertAckResponse, responses={404: {"model": ErrorResponse}})
async def acknowledge_alert(alert_id: int, request: Request):
    engine = request.app.state.alert_engine
    result = engine.acknowledge(alert_id)
    if result is None:
        return JSONResponse(
            status_code=404,
            content=ErrorResponse(error=ErrorDetail(code="ALERT_NOT_FOUND", message=f"Alert #{alert_id} not found")).model_dump(),
        )
    return AlertAckResponse(**result)
