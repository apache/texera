from __future__ import annotations

import json
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


class TemplateNotFoundError(Exception):
    pass


def _template_path(suggestion_id: str) -> Path:
    # Defend against path traversal: only allow simple filenames.
    if "/" in suggestion_id or "\\" in suggestion_id or ".." in suggestion_id:
        raise TemplateNotFoundError(suggestion_id)
    return TEMPLATES_DIR / f"{suggestion_id}.json"


def has_static_template(suggestion_id: str) -> bool:
    try:
        return _template_path(suggestion_id).is_file()
    except TemplateNotFoundError:
        return False


def render(suggestion_id: str, dataset_id: int, file_path: str, target_column: str) -> str:
    path = _template_path(suggestion_id)
    if not path.is_file():
        raise TemplateNotFoundError(suggestion_id)
    raw = path.read_text(encoding="utf-8")
    rendered = (
        raw.replace("{{DATASET_PATH}}", file_path)
        .replace("{{TARGET_COLUMN}}", target_column)
        .replace("{{DATASET_ID}}", str(dataset_id))
    )
    # Re-serialize to a compact JSON string so callers get a single line of JSON,
    # matching what Texera /api/workflow/create expects as `content`.
    obj = json.loads(rendered)
    return json.dumps(obj, ensure_ascii=False)


def default_workflow_name(suggestion_id: str) -> str:
    try:
        raw = _template_path(suggestion_id).read_text(encoding="utf-8")
        obj = json.loads(raw)
        name = obj.get("_WORKFLOW_NAME")
        if isinstance(name, str) and name.strip():
            return name
    except (FileNotFoundError, TemplateNotFoundError, json.JSONDecodeError):
        pass
    return f"[Genesis] {suggestion_id}"
