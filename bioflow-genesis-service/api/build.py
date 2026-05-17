"""POST /api/genesis/build — render workflow JSON and persist to Texera (no agent)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from core import upload_cache
from core.classifier import infer_task_from_free_text, is_error_suggestion
from core.texera_client import TexeraAuthError, TexeraClient, TexeraClientError
from core.workflow_builder import build_workflow_json, default_feature_cols

logger = logging.getLogger(__name__)

router = APIRouter()


class BuildRequest(BaseModel):
    upload_id: str
    card_index: int = Field(ge=0, le=3)
    free_text: str | None = None
    wid: int | None = None
    jwt_token: str = Field(..., min_length=1)


def _genesis_workflow_name(card_title: str) -> str:
    t = (card_title or "Genesis workflow").strip()
    if t.lower().startswith("[genesis]"):
        return t
    return f"[Genesis] {t}"


@router.post("/build")
async def build_workflow(req: BuildRequest):
    cached = upload_cache.get(req.upload_id)
    if not cached:
        return JSONResponse(
            status_code=404,
            content={"error": f"upload_id not found: {req.upload_id}"},
        )

    columns: list[str] = list(cached.get("columns", []))
    file_path = str(cached["file_path"])
    sample_rows_matrix: list[list[Any]] = list(cached.get("sample_rows", []))
    sample_dicts = [dict(zip(columns, row)) for row in sample_rows_matrix]
    row_count = int(cached.get("row_count", len(sample_rows_matrix)))
    suggestions: list[dict[str, Any]] = list(cached.get("suggestions") or [])
    dataset_summary = str(cached.get("dataset_summary", ""))
    scenario_label = str(cached.get("scenario_label", ""))

    free = (req.free_text or "").strip()
    algo: str | None = None
    feature_cols: list[str] = []
    chart_type = "auto"

    if free:
        try:
            inferred = infer_task_from_free_text(
                free,
                columns=columns,
                sample_rows=sample_dicts,
                row_count=row_count,
                dataset_summary=dataset_summary,
                scenario_label=scenario_label,
            )
        except Exception as e:
            logger.warning("free-text infer failed: %s", e)
            return JSONResponse(
                status_code=502,
                content={"error": f"could not interpret custom goal: {e}"},
            )
        task_type = str(inferred.get("task_type") or "exploration").lower()
        target_col = inferred.get("target_column")
        target_s = str(target_col).strip() if target_col is not None else ""
        algo = inferred.get("algorithm")
        if isinstance(algo, str):
            algo = algo.strip() or None
        fc = inferred.get("feature_cols")
        if isinstance(fc, list) and fc:
            feature_cols = [str(x) for x in fc if x is not None and str(x).strip()]
        else:
            feature_cols = default_feature_cols(columns, target_s or None)
        raw_ct = inferred.get("chart_type")
        if isinstance(raw_ct, str) and raw_ct.strip():
            chart_type = raw_ct.strip().lower()
        card_title = "Custom analysis"
        if task_type == "visualization":
            if chart_type == "pie":
                card_title = f"Pie chart — {target_s or 'target'}"
            elif chart_type == "bar":
                card_title = f"Bar chart — {target_s or 'target'}"
            elif chart_type == "histogram":
                card_title = f"Histogram — {target_s or 'feature'}"
            elif chart_type == "scatter":
                card_title = f"Scatter plot — {target_s or 'data'}"
            else:
                card_title = f"Chart — {target_s or 'data'}"
        workflow_name = _genesis_workflow_name(card_title)
    else:
        if req.card_index < 0 or req.card_index >= len(suggestions):
            return JSONResponse(
                status_code=400,
                content={"error": f"card_index out of range: {req.card_index}"},
            )
        picked = suggestions[req.card_index]
        if is_error_suggestion(picked):
            return JSONResponse(
                status_code=400,
                content={"error": "analysis suggestions unavailable — retry upload"},
            )
        task_type = str(picked.get("task_type") or "classification").lower()
        tc = picked.get("target_column") or cached.get("target_column") or ""
        target_s = str(tc).strip() if tc is not None else ""
        algo = picked.get("algorithm")
        if isinstance(algo, str):
            algo = algo.strip() or None
        fc = picked.get("feature_cols")
        if isinstance(fc, list) and fc:
            feature_cols = [str(x) for x in fc if x is not None and str(x).strip()]
        else:
            feature_cols = default_feature_cols(columns, target_s or None)
        card_title = str(picked.get("title") or "Genesis workflow")
        workflow_name = _genesis_workflow_name(card_title)
        raw_ct = picked.get("chart_type")
        if isinstance(raw_ct, str) and raw_ct.strip():
            chart_type = raw_ct.strip().lower()

    if task_type in ("classification", "regression", "automl") and not target_s:
        return JSONResponse(
            status_code=400,
            content={"error": "missing target column for supervised workflow"},
        )
    if task_type in ("exploration", "visualization") and not target_s:
        # Pearson / charts need a label column — fall back to last column or first.
        target_s = columns[-1] if columns else ""

    try:
        content = build_workflow_json(
            task_type=task_type,
            target_col=target_s,
            feature_cols=feature_cols,
            dataset_path=file_path,
            workflow_name=workflow_name,
            algorithm=algo,
            chart_type=chart_type,
        )
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    client = TexeraClient()
    try:
        if req.wid is not None:
            client.persist_workflow(
                req.jwt_token,
                int(req.wid),
                workflow_name,
                content,
            )
            return {"wid": int(req.wid), "workflow_name": workflow_name}
        wid = client.create_workflow_from_dict(
            req.jwt_token,
            workflow_name,
            content,
        )
        return {"wid": wid, "workflow_name": workflow_name}
    except TexeraAuthError as e:
        return JSONResponse(status_code=401, content={"error": str(e)})
    except TexeraClientError as e:
        logger.exception("texera persist/create failed")
        return JSONResponse(status_code=502, content={"error": str(e)})
