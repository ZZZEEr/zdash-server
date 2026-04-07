"""POST /api/v1/report and POST /api/v1/report/batch"""

import asyncio
from typing import List

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from server.models.node import (
    AgentReport, BatchReportRequest, BatchReportResponse, BatchReportResult,
    ReportAccepted, ErrorResponse, ErrorDetail,
)

router = APIRouter()


async def _process_single_report(report: AgentReport, request: Request) -> dict:
    store = request.app.state.node_store
    alert_engine = request.app.state.alert_engine
    ws = request.app.state.ws_manager

    node_data = store.ingest(report)

    alerts = alert_engine.evaluate(
        node_id=report.node.node_id,
        node_data=node_data,
        node_status=node_data.get("status", "online"),
    )

    node_list = store.get_node_list()
    asyncio.create_task(ws.broadcast_node_update(node_list.model_dump()))

    for alert in alerts:
        asyncio.create_task(ws.broadcast_alert(alert))

    return node_data


@router.post("/api/v1/report", response_model=ReportAccepted, responses={400: {"model": ErrorResponse}})
async def report_ingest(report: AgentReport, request: Request):
    try:
        await _process_single_report(report, request)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(error=ErrorDetail(code="INGEST_ERROR", message=str(e))).model_dump(),
        )
    return ReportAccepted(status="accepted", node_id=report.node.node_id)


@router.post("/api/v1/report/batch", response_model=BatchReportResponse, responses={400: {"model": ErrorResponse}})
async def report_batch_ingest(batch: BatchReportRequest, request: Request):
    results: List[BatchReportResult] = []
    accepted = 0
    failed = 0

    for report in batch.reports:
        try:
            await _process_single_report(report, request)
            results.append(BatchReportResult(node_id=report.node.node_id, status="accepted"))
            accepted += 1
        except Exception as e:
            results.append(BatchReportResult(node_id=report.node.node_id, status="failed", error=str(e)))
            failed += 1

    return BatchReportResponse(total=len(batch.reports), accepted=accepted, failed=failed, results=results)
