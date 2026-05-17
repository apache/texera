from __future__ import annotations

import json
import logging
from typing import Any

from core.llm_client import chat_completion

logger = logging.getLogger(__name__)

CLASSIFIER_SYSTEM = """You are a senior data scientist. Look at this CSV's column names and sample rows.

LANGUAGE REQUIREMENT (CRITICAL):
All user-facing strings MUST be in **English** (titles, descriptions, scenario_label, dataset_summary).

Output ONLY valid JSON. No markdown fences (no ```), no explanation text. The reply must start with { and end with }.

--- task_type definitions (choose ONE per suggestion; follow intent, not only techniques) ---

- **visualization**: The user wants a **CHART or PLOT** to **see** the data (pie, bar, scatter, histogram, distribution).
  Keywords / intents: chart, plot, pie chart, bar chart, scatter plot, histogram, visualize, show distribution, "show me X by Y" **when the goal is viewing data**, not training a model or correlation tables.
  Use **visualization** when the primary deliverable is a figure — **not** classification, regression, exploration correlations, or AutoML.

- **classification**: The user wants to **PREDICT** a **categorical** label. Train a supervised classifier and report performance on a holdout split.
  Keywords / intents: predict class, diagnose category, classify, which group, forecast **discrete** outcome.

- **regression**: The user wants to **PREDICT** a **numeric** label. Train a regressor.
  Keywords: predict price/amount, estimate continuous value, forecast **number**.

- **exploration**: The user wants to **UNDERSTAND** the data or **RANK** drivers / risk factors **without** the primary goal of deploying a prediction pipeline.
  Keywords: which features matter, find drivers, **risk factors**, correlation, association, importance / ranking, what predicts (interpretive), top predictors, "what is associated with".
  **IMPORTANT**: If the goal is to **RANK or EXPLAIN** features (even if a tree model could be used internally in other tools), use **exploration** — the workflow should show rankings and associations, not a full train→predict product.
  Use exploration when the card title/description is about understanding, screening factors, or correlation — not about shipping a classifier.

- **automl**: The user wants to **COMPARE multiple algorithms** on the same task.
  Keywords: best model, compare classifiers, AutoML, which algorithm wins.
  The optional fourth card with id containing "automl" MUST use task_type "automl".

--- coverage requirement ---

Generate **3** specific suggestions **plus** a **4th** AutoML card **if and only if** there is a clear supervised classification/regression target column. If the data is **only** for unsupervised exploration, return **exactly 3** suggestions (omit the AutoML card).

**Generate 4 SUGGESTIONS (when supervised) covering distinct USER INTENTS:**
1. A **predictive** card: **classification** OR **regression** (train a model to predict the target).
2. An **exploration** card: understand drivers, **risk factors**, correlations, ranking — task_type **exploration**.
3. **Either** a **visualization** card (task_type **visualization**) when the dataset has **both** numeric features and a **categorical / discrete** target (e.g. pie/bar of Outcome), **or** a distinct alternative analysis that is still **not** automl.
4. A **model comparison** card: task_type **automl** (the card_d_automl slot).

**HARD RULE when a supervised target exists:** among the non-automl suggestions you MUST include **at least one** with task_type **exploration** AND **at least one** with task_type **classification** OR **regression** (pick which matches the target: categorical → classification, numeric → regression). Do **not** return four suggestions that are all classification. A **visualization** card satisfies the "distinct intent" slot when it replaces a second chart-oriented suggestion; still keep exploration + one predictive card as above.

Each suggestion MUST include:
- "id": "card_a", "card_b", "card_c", and optionally "card_d_automl"
- "title": short English title
- "description": 1–2 English sentences
- "task_type": one of "classification" | "regression" | "exploration" | "automl" | "visualization"
    * Supervised binary/multi-class categorical target -> "classification" **only** when the card is about **prediction**
    * Continuous numeric target -> "regression" **only** when the card is about **prediction**
    * Understanding / risk factors / correlations / ranking without deployable predictor focus -> "exploration"
    * Chart / plot / distribution as the main deliverable -> "visualization" (set "chart_type": "pie" | "bar" | "scatter" | "histogram" | "auto" when helpful)
    * card_d_automl MUST use "automl"
- "target_column": string name of the label column, or null when not applicable (pure exploration without a label)
- "algorithm": optional native Texera sklearn operator hint, e.g. "SklearnLogisticRegression", "SklearnRandomForest", "SklearnLinearRegression", or null to use defaults
  * For **exploration** cards, prefer null or algorithms only if truly needed; ranking cards should not default everything to RandomForest for prediction.
- "feature_cols": optional list of column names to use as features; if null or empty, downstream code will use **all columns except target**

Optional compatibility (may be empty string): "goal_for_agent" — legacy field, not used by the app anymore.

Tailor everything to **this** dataset. For Pima / diabetes with Outcome 0/1, include **both** a classification **prediction** card and an **exploration** card (risk factors / correlations). When there is **no** target, use "exploration" — and **do not** emit card_d_automl.

Return STRICTLY this JSON shape:
{
  "scenario_label": "short English label",
  "dataset_summary": "one English sentence",
  "target_column": "best guess label column or null",
  "confidence": 0.0-1.0,
  "suggestions": [ /* 3 or 4 objects */ ]
}
"""


FREE_TEXT_INFER_SYSTEM = """You infer how to build a Texera ML workflow from a short user instruction and CSV context.
Output ONLY JSON (no markdown) with keys:
- "task_type": "classification" | "regression" | "exploration" | "automl" | "visualization"
- "target_column": string or null
- "algorithm": string or null (e.g. SklearnLogisticRegression, SklearnLinearRegression)
- "chart_type": "auto" | "pie" | "bar" | "scatter" | "histogram" — include when task_type is visualization (otherwise null or omit)
- "feature_cols": array of strings or empty (means: use all non-target columns)

Task routing (same semantics as batch classifier):
- **visualization** if the user wants a chart or plot (pie / bar / scatter / histogram / visualize / distribution), **without** training a model as the main goal.
- **exploration** if the user wants to understand drivers, risk factors, correlations, which features matter, ranking, "what is associated with" — even when a standard label column exists.
- **classification** / **regression** only when the user clearly wants to **train a predictor** (predict, classify, forecast a specific outcome as the product).

For **visualization**, set chart_type from wording: "pie" → pie, "histogram" → histogram, "scatter" → scatter, "bar" → bar, otherwise auto.

Be conservative on target_column: if the user did not name a target but columns suggest a standard label (e.g. Outcome), you may use it for **classification/regression/visualization** flows; for **exploration** requests about risk factors you should still set task_type to **exploration** and may keep that column as target_column for correlation-style workflows."""


AUTOML_FALLBACK_GOAL_EN = (
    "AutoML: parallel SklearnLogisticRegression, SklearnDecisionTree, SklearnRandomForest; compare accuracy."
)


def infer_task_from_free_text(
    free_text: str,
    *,
    columns: list[str],
    sample_rows: list[dict[str, Any]],
    row_count: int,
    dataset_summary: str,
    scenario_label: str,
) -> dict[str, Any]:
    """Short LLM call when the user types a custom goal instead of picking a card."""
    cols = _normalize_columns(columns)
    user_msg = (
        f"Scenario: {scenario_label}\nSummary: {dataset_summary}\n"
        f"Rows (approx): {row_count}\nColumns: {json.dumps(cols)}\n"
        f"Sample: {json.dumps(sample_rows[:5], default=str)}\n"
        f"User instruction:\n{free_text.strip()}"
    )
    out = chat_completion(FREE_TEXT_INFER_SYSTEM, user_msg, json_mode=True, timeout=20.0)
    if not isinstance(out, dict):
        raise ValueError("LLM returned non-object JSON for free-text inference")
    if out.get("target_column") is None and out.get("target_col") is not None:
        out["target_column"] = out.get("target_col")
    snippet = free_text.strip().replace("\n", " ")[:100]
    logger.info(
        "[FREE_TEXT] User said: %r. LLM inferred: task=%s target=%s algo=%s feature_cols=%s chart=%s",
        snippet,
        out.get("task_type"),
        out.get("target_column"),
        out.get("algorithm"),
        out.get("feature_cols"),
        out.get("chart_type"),
    )
    _force_visualization_from_keywords(free_text, out)
    return out


def _force_visualization_from_keywords(free_text: str, out: dict[str, Any]) -> None:
    """Hard route chart/plot language to visualization (overrides LLM over-classification)."""
    low = free_text.strip().lower()
    if not low:
        return
    markers = (
        "pie chart",
        "pie-chart",
        "piechart",
        "bar chart",
        "bar-chart",
        "histogram",
        "scatter plot",
        "scatterplot",
        "visualize",
        "visualise",
        "distribution",
    )
    hits = any(m in low for m in markers)
    hits = hits or f" {low} ".find(" chart ") >= 0
    hits = hits or f" {low} ".find(" plot ") >= 0
    hits = hits or f" {low} ".find(" plots ") >= 0
    if not hits:
        return
    out["task_type"] = "visualization"
    ct = "auto"
    if "histogram" in low:
        ct = "histogram"
    elif "scatter" in low:
        ct = "scatter"
    elif "pie" in low:
        ct = "pie"
    elif "bar" in low:
        ct = "bar"
    out["chart_type"] = ct


def _normalize_columns(raw: list[str]) -> list[str]:
    return [str(c) for c in raw if c is not None and str(c).strip() != ""]


def _looks_supervised(target_column: str | None) -> bool:
    return bool(target_column and str(target_column).strip())


def _normalize_target_val(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("null", "none"):
        return None
    return s


_TASK_TYPES = frozenset({"classification", "regression", "exploration", "automl", "visualization"})


def _coerce_task_type(val: Any, *, fallback: str) -> str:
    s = str(val or "").strip().lower()
    if s in _TASK_TYPES:
        return s
    if s in ("clustering", "unsupervised"):
        return "exploration"
    return fallback


def _parse_feature_cols(val: Any, all_columns: list[str], target: str | None) -> list[str]:
    if val is None:
        return _default_feature_cols(all_columns, target)
    if isinstance(val, list):
        out = [str(x) for x in val if x is not None and str(x).strip()]
        if not out:
            return _default_feature_cols(all_columns, target)
        return out
    return _default_feature_cols(all_columns, target)


def _default_feature_cols(all_columns: list[str], target: str | None) -> list[str]:
    if not target:
        return list(all_columns)
    return [c for c in all_columns if c != target]


def _ensure_suggestion_task_mix(suggestions: list[dict[str, Any]], global_target: str | None) -> None:
    """Ensure supervised decks include both exploration and a predictive task (LLM may over-classify)."""
    if not _looks_supervised(global_target):
        return
    non_auto_idx = [i for i, s in enumerate(suggestions) if s.get("task_type") != "automl"]
    if len(non_auto_idx) < 2:
        return

    def _non_auto_has(task: str) -> bool:
        return any(suggestions[i].get("task_type") == task for i in non_auto_idx)

    if not _non_auto_has("exploration"):
        for i in non_auto_idx:
            if suggestions[i].get("task_type") == "visualization":
                continue
            suggestions[i]["task_type"] = "exploration"
            break
        non_auto_idx = [i for i, s in enumerate(suggestions) if s.get("task_type") != "automl"]

    if not _non_auto_has("classification") and not _non_auto_has("regression"):
        for i in non_auto_idx:
            t = suggestions[i].get("task_type")
            if t in ("exploration", "visualization"):
                continue
            suggestions[i]["task_type"] = "classification"
            break
        else:
            for i in non_auto_idx:
                if suggestions[i].get("task_type") == "exploration":
                    suggestions[i]["task_type"] = "classification"
                    break


def _validate_suggestions(
    raw: list[Any], *, allow_automl: bool, global_target: str | None, columns: list[str]
) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError("suggestions must be a list")
    out: list[dict[str, Any]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError("invalid suggestion entry")
        s = dict(item)
        s.setdefault("id", f"card_{chr(ord('a') + i)}")
        s.setdefault("title", f"Analysis {i + 1}")
        s.setdefault("description", "")
        tid = str(s.get("id", ""))
        is_automl = "automl" in tid.lower()
        default_task = "automl" if is_automl else ("classification" if global_target else "exploration")
        s["task_type"] = _coerce_task_type(s.get("task_type"), fallback=default_task)
        tgt = _normalize_target_val(s.get("target_column"))
        if tgt is None and global_target:
            tgt = global_target
        s["target_column"] = tgt
        s["feature_cols"] = _parse_feature_cols(s.get("feature_cols"), columns, tgt)
        if s.get("algorithm") is not None:
            s["algorithm"] = str(s["algorithm"]).strip() or None
        else:
            s["algorithm"] = None
        s.setdefault("goal_for_agent", "")
        s.setdefault("estimated_runtime_seconds", 12)
        raw_ct = s.get("chart_type")
        if s.get("task_type") == "visualization":
            if isinstance(raw_ct, str) and raw_ct.strip():
                s["chart_type"] = raw_ct.strip().lower()
            else:
                s["chart_type"] = "auto"
        else:
            s["chart_type"] = None
        out.append(s)
    if len(out) < 3:
        raise ValueError("need at least 3 suggestions")
    if len(out) > 4:
        out = out[:4]
    if allow_automl and len(out) == 3:
        tgt = global_target
        for c in out:
            tc = _normalize_target_val(c.get("target_column"))
            if tc:
                tgt = tc
                break
        out.append(
            {
                "id": "card_d_automl",
                "title": "✨ Find Best Model Automatically",
                "description": (
                    "Train several classifiers in parallel on the same split and compare accuracy; "
                    "the workflow picks the best-performing model."
                ),
                "task_type": "automl",
                "target_column": tgt,
                "algorithm": None,
                "feature_cols": _parse_feature_cols(None, columns, tgt),
                "goal_for_agent": AUTOML_FALLBACK_GOAL_EN,
                "estimated_runtime_seconds": 20,
            }
        )
    if not allow_automl and len(out) > 3:
        out = out[:3]
    return out


def _hydrate_targets(suggestions: list[dict[str, Any]], global_target: str | None) -> None:
    for s in suggestions:
        tc = s.get("target_column")
        if tc is None or str(tc).strip() == "":
            if global_target:
                s["target_column"] = global_target
            else:
                s["target_column"] = None


def classify_dataset(
    columns: list[str],
    sample_rows: list[dict[str, Any]],
    row_count: int,
) -> dict[str, Any]:
    """LLM-driven scenario + 3–4 suggestions with task metadata for workflow_builder."""
    cols = _normalize_columns(columns)
    if not cols:
        return _error_response("No columns in CSV header.")

    user_msg = f"""Dataset has {row_count} rows.
Columns: {json.dumps(cols)}
First 5 rows as records: {json.dumps(sample_rows[:5], default=str)}

Generate suggestions (3 + optional AutoML)."""

    try:
        result = chat_completion(CLASSIFIER_SYSTEM, user_msg, json_mode=True, timeout=20.0)
        if not isinstance(result, dict):
            raise ValueError("LLM returned non-object JSON")

        target_top = _normalize_target_val(result.get("target_column"))

        allow_automl = _looks_supervised(target_top)
        suggestions = _validate_suggestions(
            result.get("suggestions") or [],
            allow_automl=allow_automl,
            global_target=target_top,
            columns=cols,
        )

        _ensure_suggestion_task_mix(suggestions, target_top)

        _hydrate_targets(suggestions, target_top)
        for s in suggestions:
            if s.get("target_column"):
                s["feature_cols"] = _parse_feature_cols(
                    s.get("feature_cols"), cols, str(s["target_column"])
                )

        out = {
            "scenario_label": str(result.get("scenario_label") or "dataset"),
            "dataset_summary": str(result.get("dataset_summary") or "A tabular dataset."),
            "target_column": target_top or "",
            "confidence": float(result.get("confidence", 0.7)),
            "suggestions": suggestions,
        }
        if not out["target_column"]:
            for s in suggestions:
                tc = s.get("target_column")
                if tc is not None and str(tc).strip():
                    out["target_column"] = str(tc)
                    break
        return out
    except Exception as e:
        logger.warning("classify_dataset LLM failed: %s", e)
        return _error_response(str(e))


def _error_response(detail: str) -> dict[str, Any]:
    return {
        "scenario_label": "error",
        "dataset_summary": (
            "We couldn't generate suggestions for this dataset. "
            "Please retry or check the CSV."
        ),
        "target_column": "",
        "confidence": 0.0,
        "suggestions": [
            {
                "id": "genesis_error_retry",
                "title": "Analysis failed — please retry",
                "description": (
                    "AI suggestions unavailable. Please re-upload the file or retry later."
                ),
                "task_type": "exploration",
                "goal_for_agent": "",
                "target_column": None,
                "algorithm": None,
                "feature_cols": [],
                "estimated_runtime_seconds": 0,
                "error": True,
                "error_detail": detail[:500],
            }
        ],
        "llm_error": True,
    }


def classify(columns: list[str]) -> dict[str, Any]:
    """Backward-compatible entry — weak without sample rows."""
    if not columns:
        return _analyze_response_from_llm(_error_response("empty columns"))
    body = classify_dataset(columns, [], row_count=0)
    return _analyze_response_from_llm(body)


def _analyze_response_from_llm(llm: dict[str, Any]) -> dict[str, Any]:
    label = llm.get("scenario_label", "generic")
    tc = llm.get("target_column") or ""
    if tc is None:
        tc = ""
    return {
        "detected_scenario": label,
        "scenario_label": label,
        "confidence": llm.get("confidence", 0.5),
        "target_column": tc,
        "dataset_summary": llm.get("dataset_summary", ""),
        "suggestions": llm["suggestions"],
        "llm_error": bool(llm.get("llm_error")),
    }


def list_scenarios() -> list[dict[str, Any]]:
    """Genesis now uses per-upload LLM suggestions — no static template scenarios."""
    return []


def get_suggestion(suggestion_id: str) -> dict[str, Any] | None:
    return None


def is_error_suggestion(suggestion: dict[str, Any]) -> bool:
    return bool(suggestion.get("error")) or suggestion.get("id") == "genesis_error_retry"
