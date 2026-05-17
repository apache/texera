from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from core import upload_cache
from core.classifier import classify_dataset

router = APIRouter()


class AnalyzeRequest(BaseModel):
    upload_id: str | None = None
    dataset_id: int | None = None
    dataset_name: str | None = None
    file_path: str | None = None
    columns: list[str] = Field(default_factory=list)
    sample_rows: list[list[Any]] = Field(default_factory=list)
    row_count: int | None = None


def _rows_to_dicts(columns: list[str], rows: list[list[Any]]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row)) for row in rows]


@router.post("/analyze")
async def analyze(req: AnalyzeRequest):
    if req.upload_id:
        cached = upload_cache.get(req.upload_id)
        if not cached:
            return JSONResponse(
                status_code=404,
                content={"error": f"upload_id not found: {req.upload_id}"},
            )
        columns = list(cached["columns"])
        sample_matrix: list[list[Any]] = list(cached.get("sample_rows", []))
        row_count = int(cached.get("row_count", len(sample_matrix)))
        file_path = str(cached["file_path"])
        dataset_id = int(cached["dataset_id"])
        dataset_name = cached.get("dataset_name")
    else:
        if not req.columns:
            return JSONResponse(
                status_code=400,
                content={"error": "missing required field: columns (or upload_id)"},
            )
        columns = req.columns
        sample_matrix = req.sample_rows
        row_count = int(req.row_count) if req.row_count is not None else len(sample_matrix)
        file_path = req.file_path or ""
        dataset_id = int(req.dataset_id or 0)
        dataset_name = req.dataset_name

    sample_dicts = _rows_to_dicts(columns, sample_matrix)
    llm = classify_dataset(columns, sample_dicts, row_count)

    detected = llm.get("scenario_label", "generic")
    target_top = llm.get("target_column")
    if target_top is None:
        target_top = ""

    suggestions_out = []
    for s in llm.get("suggestions", []):
        one = dict(s)
        tc = one.get("target_column")
        suggestions_out.append(
            {
                "id": str(one.get("id", "suggestion")),
                "title": one.get("title", ""),
                "description": one.get("description", ""),
                "goal_for_agent": one.get("goal_for_agent", ""),
                "analysis_type": one.get("analysis_type", ""),
                "task_type": one.get("task_type", ""),
                "target_column": tc,
                "algorithm": one.get("algorithm"),
                "feature_cols": one.get("feature_cols") if isinstance(one.get("feature_cols"), list) else [],
                "estimated_runtime_seconds": one.get("estimated_runtime_seconds", 12),
                "error": bool(one.get("error")),
            }
        )

    body: dict[str, Any] = {
        "detected_scenario": detected,
        "scenario_label": detected,
        "confidence": llm.get("confidence", 0.5),
        "target_column": str(target_top) if target_top is not None else "",
        "dataset_summary": llm.get("dataset_summary", ""),
        "suggestions": suggestions_out,
        "llm_error": bool(llm.get("llm_error")),
    }

    if req.upload_id:
        upload_cache.merge(
            req.upload_id,
            {
                "dataset_summary": body["dataset_summary"],
                "scenario_label": body["scenario_label"],
                "target_column": body["target_column"],
                "confidence": body["confidence"],
                "suggestions": suggestions_out,
                "columns": columns,
                "llm_error": body["llm_error"],
            },
        )

    # Echo identifiers helpful for clients that do not round-trip the full upload payload.
    if req.upload_id:
        body["upload_id"] = req.upload_id
    if file_path:
        body["file_path"] = file_path
    if dataset_id:
        body["dataset_id"] = dataset_id
    if dataset_name:
        body["dataset_name"] = dataset_name
    body["row_count"] = row_count

    return body
