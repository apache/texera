from __future__ import annotations

from typing import Literal

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from core.classifier import is_error_suggestion
from core.instantiator import TemplateNotFoundError, default_workflow_name, has_static_template, render
from core import upload_cache

router = APIRouter()

_AGENT_DEPRECATED = (
    "Agent mode is deprecated. Call POST /api/genesis/build with jwt_token, upload_id, "
    "and card_index (0–3), or free_text for a custom instruction."
)


class InstantiateRequest(BaseModel):
    suggestion_id: str
    upload_id: str | None = None
    dataset_id: int | None = None
    file_path: str | None = None
    target_column: str | None = None
    columns: list[str] = Field(default_factory=list)
    mode: Literal["template", "agent"] = "template"
    custom_goal: str | None = None


def _resolve_template_target(suggestion: dict | None, req: InstantiateRequest) -> str:
    if req.target_column:
        return req.target_column
    if suggestion and suggestion.get("target_column"):
        return str(suggestion["target_column"])
    return ""


@router.post("/instantiate")
async def instantiate(req: InstantiateRequest):
    cached: dict | None = None
    if req.upload_id:
        cached = upload_cache.get(req.upload_id)
        if not cached:
            return JSONResponse(
                status_code=404,
                content={"error": f"upload_id not found: {req.upload_id}"},
            )
        dyn_suggestions = cached.get("suggestions") or []

        if req.custom_goal and str(req.custom_goal).strip():
            if req.mode != "agent":
                return JSONResponse(
                    status_code=400,
                    content={"error": "custom_goal requires mode=agent (deprecated — use /build)"},
                )
            return JSONResponse(status_code=400, content={"error": _AGENT_DEPRECATED})

        suggestion_obj = next(
            (s for s in dyn_suggestions if s.get("id") == req.suggestion_id),
            None,
        )
        if suggestion_obj is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"suggestion_id not found for upload: {req.suggestion_id}"},
            )
        if is_error_suggestion(suggestion_obj):
            return JSONResponse(
                status_code=400,
                content={"error": "analysis suggestions unavailable — retry upload"},
            )
        dataset_id = int(cached["dataset_id"])
        file_path = str(cached["file_path"])
        columns = list(cached.get("columns", []))
        dataset_summary = str(cached.get("dataset_summary", ""))
        scenario_label = str(cached.get("scenario_label", ""))

        if req.mode == "agent":
            _ = (dataset_id, file_path, columns, dataset_summary, scenario_label)
            return JSONResponse(status_code=400, content={"error": _AGENT_DEPRECATED})

        tmpl_target = _resolve_template_target(suggestion_obj, req)
        try:
            content = render(req.suggestion_id, dataset_id, file_path, tmpl_target)
        except TemplateNotFoundError:
            return JSONResponse(
                status_code=404,
                content={"error": f"no template for suggestion_id: {req.suggestion_id}"},
            )
        return {
            "mode": "template",
            "workflow_name": default_workflow_name(req.suggestion_id),
            "workflow_content": content,
        }

    # Legacy path (no upload_id) — static JSON templates only
    if not has_static_template(req.suggestion_id):
        return JSONResponse(
            status_code=404,
            content={"error": f"suggestion_id not found: {req.suggestion_id}"},
        )

    if req.dataset_id is None or req.file_path is None:
        return JSONResponse(
            status_code=400,
            content={"error": "dataset_id and file_path are required without upload_id"},
        )

    if req.mode == "agent":
        return JSONResponse(status_code=400, content={"error": _AGENT_DEPRECATED})

    tmpl_target = req.target_column or ""
    try:
        content = render(
            req.suggestion_id, req.dataset_id, req.file_path, tmpl_target
        )
    except TemplateNotFoundError:
        return JSONResponse(
            status_code=404,
            content={"error": f"suggestion_id not found: {req.suggestion_id}"},
        )
    return {
        "mode": "template",
        "workflow_name": default_workflow_name(req.suggestion_id),
        "workflow_content": content,
    }
