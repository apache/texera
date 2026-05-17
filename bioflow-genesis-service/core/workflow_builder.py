"""Deterministic Texera workflow JSON builders (Iris example shape).

Sklearn trainers and SklearnPrediction use string port IDs ``input-0`` / ``input-1``
with dual inputs (training+testing, model+data) wired explicitly.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from pathlib import Path
from typing import Any

TASK_TYPES = frozenset({"classification", "regression", "exploration", "automl", "visualization"})

DEFAULT_CHART_OPERATORS = frozenset({"PieChart", "BarChart", "Scatterplot", "Histogram"})


def _new_op_id(prefix: str) -> str:
    return f"{prefix}-operator-{uuid.uuid4()}"


def _new_link_id() -> str:
    return f"link-{uuid.uuid4()}"


def _sklearn_train_ports() -> list[dict[str, Any]]:
    return [
        {
            "portID": "input-0",
            "displayName": "training",
            "allowMultiInputs": False,
            "isDynamicPort": False,
            "dependencies": [],
        },
        {
            "portID": "input-1",
            "displayName": "testing",
            "allowMultiInputs": False,
            "isDynamicPort": False,
            "dependencies": [{"id": 0, "internal": False}],
        },
    ]


def _sklearn_prediction_ports() -> list[dict[str, Any]]:
    return [
        {
            "portID": "input-0",
            "displayName": "model",
            "allowMultiInputs": False,
            "isDynamicPort": False,
            "dependencies": [],
        },
        {
            "portID": "input-1",
            "displayName": "",
            "allowMultiInputs": False,
            "isDynamicPort": False,
            "dependencies": [{"id": 0, "internal": False}],
        },
    ]


def _single_input_port(
    *,
    allow_multi: bool = False,
    dynamic: bool = False,
) -> list[dict[str, Any]]:
    return [
        {
            "portID": "input-0",
            "displayName": "",
            "allowMultiInputs": allow_multi,
            "isDynamicPort": dynamic,
            "dependencies": [],
        }
    ]


def _output_port() -> list[dict[str, Any]]:
    return [
        {
            "portID": "output-0",
            "displayName": "",
            "allowMultiInputs": False,
            "isDynamicPort": False,
        }
    ]


def _split_outputs() -> list[dict[str, Any]]:
    return [
        {
            "portID": "output-0",
            "displayName": "",
            "allowMultiInputs": False,
            "isDynamicPort": False,
        },
        {
            "portID": "output-1",
            "displayName": "",
            "allowMultiInputs": False,
            "isDynamicPort": False,
        },
    ]


def _projection_attrs(feature_cols: list[str], target_col: str) -> list[dict[str, Any]]:
    attrs: list[dict[str, Any]] = []
    for c in feature_cols:
        attrs.append({"originalAttribute": c})
    attrs.append({"alias": "", "originalAttribute": target_col})
    return attrs


def _sklearn_classifier_props(target_col: str) -> dict[str, Any]:
    return {
        "countVectorizer": False,
        "tfidfTransformer": False,
        "target": target_col,
    }


def _sklearn_linear_regression_props(target_col: str) -> dict[str, Any]:
    return {
        "target": target_col,
        "degree": 1,
    }


def _map_classifier_operator(algorithm: str | None) -> str:
    if algorithm is None or not str(algorithm).strip():
        return "SklearnLogisticRegression"
    a = str(algorithm).strip()
    low = a.lower()
    if a == "SklearnLogisticRegression" or "logistic" in low:
        return "SklearnLogisticRegression"
    if a == "SklearnDecisionTree" or "decision" in low and "tree" in low:
        return "SklearnDecisionTree"
    if a == "SklearnRandomForest" or "random" in low and "forest" in low:
        return "SklearnRandomForest"
    if a == "SklearnPerceptron" or "perceptron" in low:
        return "SklearnPerceptron"
    return "SklearnLogisticRegression"


def _map_regression_operator(algorithm: str | None) -> str:
    if algorithm is None or not str(algorithm).strip():
        return "SklearnLinearRegression"
    a = str(algorithm).strip()
    low = a.lower()
    if a == "SklearnLinearRegression" or "linear" in low:
        return "SklearnLinearRegression"
    return "SklearnLinearRegression"


def _link(
    src_op: str,
    src_port: str,
    tgt_op: str,
    tgt_port: str,
) -> dict[str, Any]:
    return {
        "linkID": _new_link_id(),
        "source": {"operatorID": src_op, "portID": src_port},
        "target": {"operatorID": tgt_op, "portID": tgt_port},
    }


def _python_udf_props(
    code: str,
    *,
    workers: int = 1,
    retain_input: bool = False,
    output_columns: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    props: dict[str, Any] = {
        "code": code,
        "workers": workers,
        "retainInputColumns": retain_input,
    }
    if output_columns is not None:
        props["outputColumns"] = output_columns
    return props


def _insight_output_columns_5() -> list[dict[str, str]]:
    return [
        {"attributeName": "summary", "attributeType": "string"},
        {"attributeName": "top_predictors", "attributeType": "string"},
        {"attributeName": "interpretation", "attributeType": "string"},
        {"attributeName": "next_steps", "attributeType": "string"},
        {"attributeName": "caveat", "attributeType": "string"},
    ]


def classification_insight_output_column_count() -> int:
    """Service boot log: confirms this workflow_builder build is loaded (multi-column insight)."""
    return len(_insight_output_columns_5())


def _exploration_insight_output_columns() -> list[dict[str, str]]:
    return [
        {"attributeName": "summary", "attributeType": "string"},
        {"attributeName": "top_associations", "attributeType": "string"},
        {"attributeName": "interpretation", "attributeType": "string"},
        {"attributeName": "next_steps", "attributeType": "string"},
        {"attributeName": "caveat", "attributeType": "string"},
    ]


def _udf_prediction_and_model_input_ports() -> list[dict[str, Any]]:
    """Two independent inputs: predictions (port 0) and trainer output with model (port 1)."""
    return [
        {
            "portID": "input-0",
            "displayName": "predictions",
            "allowMultiInputs": False,
            "isDynamicPort": False,
            "dependencies": [],
        },
        {
            "portID": "input-1",
            "displayName": "trained_model",
            "allowMultiInputs": False,
            "isDynamicPort": False,
            "dependencies": [],
        },
    ]


def _classification_insight_code(
    target_col: str,
    trainer_operator_type: str,
    feature_cols: list[str],
) -> str:
    # Build-time placeholders are injected via str.replace to avoid having to
    # double-escape every runtime f-string brace.
    template = '''from pytexera import *
import pandas as pd
from typing import Iterator, Optional

class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        # Only consume the predictions port; the trainer/model port is ignored.
        if port != 0:
            return
        df = pd.DataFrame(table)
        target_col = "{target_col}"

        # Score
        pred_col = next((c for c in df.columns if str(c).startswith("predicted")), None)
        if pred_col is None and "prediction" in df.columns:
            pred_col = "prediction"
        if pred_col is not None and target_col in df.columns:
            correct = int((df[pred_col] == df[target_col]).sum())
            total = int(len(df))
            accuracy = correct / total if total else 0.0
            summary = f"{trainer_operator_type}: {correct}/{total} correct ({accuracy*100:.1f}% on this holdout split)"
        else:
            summary = "Predictions completed"

        # Top predictors — feature column names directly (excluding target + prediction).
        feature_cols = [
            c for c in df.columns
            if c not in (target_col, pred_col, "prediction") and pd.api.types.is_numeric_dtype(df[c])
        ]
        if len(feature_cols) >= 3:
            top_predictors = ", ".join(feature_cols[:3])
        elif feature_cols:
            top_predictors = ", ".join(feature_cols)
        else:
            top_predictors = "(features list not available)"

        interpretation = "Stronger predictors (when available) align with how the fitted model uses inputs."
        next_steps = "Apply the same Projection + Prediction chain to score new CSV rows."
        caveat = "For high-stakes screening, weigh false positives vs. false negatives before acting."

        out = pd.DataFrame({
            "summary": [summary],
            "top_predictors": [top_predictors],
            "interpretation": [interpretation],
            "next_steps": [next_steps],
            "caveat": [caveat],
        })
        yield out
'''
    return template.replace("{target_col}", target_col).replace(
        "{trainer_operator_type}", trainer_operator_type
    )


def _regression_insight_code(target_col: str, feature_cols: list[str]) -> str:
    # Build-time placeholder is injected via str.replace; runtime f-strings use single braces.
    template = '''from pytexera import *
import pandas as pd
from typing import Iterator, Optional

class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        # Only consume the predictions port; the trainer/model port is ignored.
        if port != 0:
            return
        df = pd.DataFrame(table)
        target_col = "{target_col}"

        pred_col = next((c for c in df.columns if str(c).startswith("predicted")), None)
        if pred_col is None and "prediction" in df.columns:
            pred_col = "prediction"
        if pred_col is not None and target_col in df.columns:
            y_true = df[target_col].astype(float)
            y_pred = df[pred_col].astype(float)
            ss_res = ((y_true - y_pred) ** 2).sum()
            ss_tot = ((y_true - y_true.mean()) ** 2).sum()
            r2 = 1 - (ss_res / ss_tot) if ss_tot else 0.0
            mae = (y_true - y_pred).abs().mean()
            summary = f"R² = {r2:.2f}; MAE = {mae:.2e} on this holdout split"
        else:
            summary = "Predictions completed"

        # Top predictors — feature column names directly (excluding target + prediction).
        feature_cols = [
            c for c in df.columns
            if c not in (target_col, pred_col, "prediction") and pd.api.types.is_numeric_dtype(df[c])
        ]
        if len(feature_cols) >= 3:
            top_predictors = ", ".join(feature_cols[:3])
        elif feature_cols:
            top_predictors = ", ".join(feature_cols)
        else:
            top_predictors = "(features list not available)"

        interpretation = "Larger |coefficient| features (when exposed) tend to drive predictions more."
        next_steps = "Reuse Projection + Prediction on new rows with the same schema."
        caveat = "Check residuals before extrapolating outside the training feature range."

        out = pd.DataFrame({
            "summary": [summary],
            "top_predictors": [top_predictors],
            "interpretation": [interpretation],
            "next_steps": [next_steps],
            "caveat": [caveat],
        })
        yield out
'''
    return template.replace("{target_col}", target_col)


def _pearson_code(target_col: str) -> str:
    tc = json.dumps(target_col)
    return f'''from pytexera import *
import pandas as pd
from typing import Iterator, Optional

class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        df = pd.DataFrame(table)
        tgt = {tc}
        rows = []
        for col in df.columns:
            if col == tgt:
                continue
            if not (pd.api.types.is_numeric_dtype(df[col]) and pd.api.types.is_numeric_dtype(df[tgt])):
                continue
            pair = df[[col, tgt]].dropna()
            if len(pair) < 2:
                continue
            r = float(pair[col].corr(pair[tgt]))
            rows.append({{"feature": col, "correlation": r, "_abs": abs(r)}})
        out = pd.DataFrame(rows)
        if not out.empty:
            out = out.sort_values(by="_abs", ascending=False).drop(columns=["_abs"]).reset_index(drop=True)
        else:
            out = pd.DataFrame(columns=["feature", "correlation"])
        yield out
'''


def _exploration_insight_code() -> str:
    return """from pytexera import *
import pandas as pd
from typing import Iterator, Optional

def _ranked_pairs(names: list[str], rs: list[float], maxn: int = 3) -> str:
    parts = []
    for i in range(min(maxn, len(names))):
        parts.append(f"{names[i]} (r={rs[i]:.2f})")
    return ", ".join(parts)

class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        try:
            df = pd.DataFrame(table)
            if df.empty:
                yield pd.DataFrame({
                    "summary": ["No correlations computed"],
                    "top_associations": ["—"],
                    "interpretation": [""],
                    "next_steps": [""],
                    "caveat": [""],
                })
                return
            feat_col, corr_col = "feature", "correlation"
            if feat_col not in df.columns or corr_col not in df.columns:
                raise KeyError(
                    "expected 'feature' and 'correlation' columns from Pearson UDF, got "
                    + repr(list(df.columns))
                )
            top = df.head(3)
            names = top[feat_col].astype(str).tolist()
            rs = [float(x) for x in top[corr_col].tolist()]
            ranked = _ranked_pairs(names, rs, 3)
            if len(names) >= 3:
                summary = (
                    f"{names[0]} correlates strongest (r={rs[0]:.2f}), then {names[1]} and {names[2]}."
                )
                interpretation = "Linear correlation ranks risk-relevant signals vs. the target; validate clinically."
            elif len(names) == 2:
                summary = f"{names[0]} and {names[1]} show the strongest linear links (r={rs[0]:.2f}, {rs[1]:.2f})."
                interpretation = "Use both signals together; correlation is not causation."
            elif len(names) == 1:
                summary = f"{names[0]} has the strongest linear association (r={rs[0]:.2f})."
                interpretation = "Single-feature signal; check confounding and non-linear effects."
            else:
                summary = "No numeric feature-target correlations were computed."
                interpretation = ""
            top_associations = ranked if ranked else "—"
            next_steps = "Follow up with supervised models (classification/regression) on the same Projection."
            caveat = "Screening use-cases still need domain review and independent validation."
            yield pd.DataFrame({
                "summary": [summary],
                "top_associations": [top_associations],
                "interpretation": [interpretation],
                "next_steps": [next_steps],
                "caveat": [caveat],
            })
        except Exception as e:
            yield pd.DataFrame({
                "summary": ["Could not compute insight"],
                "top_associations": [str(e)[:200]],
                "interpretation": [""],
                "next_steps": [""],
                "caveat": [""],
            })
"""


def _automl_insight_code(target_col: str) -> str:
    tc = json.dumps(target_col)
    return f'''from pytexera import *
import pandas as pd
from typing import Iterator, Optional

_MODEL_LABELS = ("Logistic Regression", "Decision Tree", "Random Forest")

class ProcessTableOperator(UDFTableOperator):
    """Merges three parallel prediction branches (port 0/1/2 = LR/DT/RF). Per-instance state only."""

    @overrides
    def open(self) -> None:
        self._port_metrics = {{}}

    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        df = pd.DataFrame(table)
        actual = df[{tc}]
        pred_col = [c for c in df.columns if str(c).startswith("predicted")][0]
        pred = df[pred_col]
        correct = (actual == pred).sum()
        total = len(df)
        acc = float(correct / total) if total > 0 else 0.0
        name = (
            _MODEL_LABELS[port]
            if 0 <= port < len(_MODEL_LABELS)
            else f"model_{{port}}"
        )
        self._port_metrics[port] = (name, acc)
        if len(self._port_metrics) < 3:
            return
        ranked = sorted(self._port_metrics.values(), key=lambda x: -x[1])
        best_name, best_acc = ranked[0]
        top_predictors = ", ".join(f"{{n}} ({{a:.1%}})" for n, a in ranked)
        summary = f"Best: {{best_name}} ({{best_acc:.1%}} accuracy on this holdout)"
        interpretation = (
            "Compares three sklearn classifiers on the same train/test split; winner is relative to this sample."
        )
        next_steps = "Deploy the best model behind the same Projection + Prediction path for scoring."
        caveat = "Only three candidates evaluated; monitor calibration and drift before production."
        yield pd.DataFrame({{
            "summary": [summary],
            "top_predictors": [top_predictors],
            "interpretation": [interpretation],
            "next_steps": [next_steps],
            "caveat": [caveat],
        }})
        self._port_metrics.clear()
'''


def default_feature_cols(all_columns: list[str], target_col: str | None) -> list[str]:
    if not target_col:
        return list(all_columns)
    return [c for c in all_columns if c != target_col]


def _effective_chart_operators() -> frozenset[str]:
    """Operators allowed for native charts; override via GENESIS_CHART_OPERATORS=PieChart,BarChart,…"""
    raw = os.environ.get("GENESIS_CHART_OPERATORS", "").strip()
    if raw:
        ex = frozenset(x.strip() for x in raw.split(",") if x.strip())
        if ex:
            return ex
    path = os.environ.get("TEXERA_LOGICAL_OP_PATH", "").strip()
    if path and Path(path).is_file():
        text = Path(path).read_text(encoding="utf-8", errors="ignore")
        found: set[str] = set(re.findall(r'name = "([A-Za-z0-9]+)"', text))
        allowed = found & DEFAULT_CHART_OPERATORS
        if allowed:
            return frozenset(allowed)
    return DEFAULT_CHART_OPERATORS


def _aggregate_count_op_props(group_col: str, count_attr: str, result_col: str) -> dict[str, Any]:
    return {
        "aggregations": [
            {
                "aggFunction": "count",
                "attribute": count_attr,
                "result attribute": result_col,
            }
        ],
        "groupByKeys": [group_col],
    }


def _visualization_insight_code(target_col: str) -> str:
    tc = json.dumps(target_col)
    return f"""from pytexera import *
import pandas as pd
from typing import Iterator, Optional

class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        try:
            df = pd.DataFrame(table)
            if df.empty or {tc} not in df.columns:
                yield pd.DataFrame({{
                    "summary": ["No rows to summarize for this chart."],
                    "top_predictors": ["—"],
                    "interpretation": [""],
                    "next_steps": [""],
                    "caveat": [""],
                }})
                return
            ser = df[{tc}]
            vc = ser.value_counts(dropna=False)
            total = int(vc.sum())
            parts = []
            detail = []
            for val, cnt in vc.items():
                pct = 100.0 * float(cnt) / total if total else 0.0
                label = str(val)
                parts.append(f"{{pct:.1f}}% {{label}}")
                detail.append(f"{{label}}: {{pct:.1f}}% (n={{int(cnt)}})")
            summary = "; ".join(parts[:8])
            if len(parts) > 8:
                summary += " …"
            top_predictors = ", ".join(detail[:6]) if detail else "—"
            interpretation = (
                "These percentages describe the projected column feeding the chart on this CSV."
            )
            next_steps = "Filter rows or join another cohort, then rebuild to compare distributions."
            caveat = "Descriptive counts only — not a prevalence estimate for a broader population."
            yield pd.DataFrame({{
                "summary": [summary],
                "top_predictors": [top_predictors],
                "interpretation": [interpretation],
                "next_steps": [next_steps],
                "caveat": [caveat],
            }})
        except Exception as e:
            yield pd.DataFrame({{
                "summary": ["Could not summarize chart data"],
                "top_predictors": [str(e)[:200]],
                "interpretation": [""],
                "next_steps": [""],
                "caveat": [""],
            }})
"""


def _matplotlib_chart_fallback_code(target_col: str, chart_kind: str) -> str:
    """PNG→base64 embedded in HTML when no native chart operator is available."""
    tc = json.dumps(target_col)
    ck = json.dumps(chart_kind)
    return f"""from pytexera import *
import pandas as pd
import io
import base64
from typing import Iterator, Optional
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
except Exception:
    plt = None

class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        kind = {ck}
        if plt is None:
            html = "<p>Chart fallback unavailable (matplotlib not installed).</p>"
            yield pd.DataFrame({{"html-content": [html]}})
            return
        df = pd.DataFrame(table)
        if df.empty or {tc} not in df.columns:
            yield pd.DataFrame({{"html-content": ["<p>No data to chart.</p>"]}})
            return
        fig, ax = plt.subplots(figsize=(5.5, 4.2))
        col = df[{tc}]
        if kind == "bar":
            vc = col.astype(str).value_counts()
            ax.bar(range(len(vc)), vc.values, tick_label=[str(i) for i in vc.index])
            ax.set_xticklabels([str(i) for i in vc.index], rotation=45, ha="right")
            ax.set_ylabel("count")
        else:
            vc = col.astype(str).value_counts()
            ax.pie(vc.values, labels=[str(i) for i in vc.index], autopct="%1.1f%%")
            ax.axis("equal")
        ax.set_title("Distribution — " + {tc})
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        html = f'<img src="data:image/png;base64,{{b64}}" alt="chart" style="max-width:100%"/>'
        yield pd.DataFrame({{"html-content": [html]}})
"""


def _resolve_viz_native_op(
    chart_type: str,
    target_col: str,
    feature_cols: list[str],
    registered: frozenset[str],
) -> tuple[str | None, bool, dict[str, Any]]:
    """Return (operatorType or None, needs_aggregate, meta with props + proj_cols)."""
    ct = (chart_type or "auto").strip().lower()
    count_name = "#count"

    def dist_meta_pie() -> dict[str, Any]:
        return {
            "props": {"value": count_name, "name": target_col},
            "proj_cols": [target_col],
        }

    def dist_meta_bar() -> dict[str, Any]:
        return {
            "props": {
                "categoryColumn": target_col,
                "horizontalOrientation": False,
                "fields": target_col,
                "value": count_name,
            },
            "proj_cols": [target_col],
        }

    def pick_dist_chart(*, prefer_pie_first: bool) -> tuple[str | None, bool, dict[str, Any]]:
        if prefer_pie_first and "PieChart" in registered:
            return "PieChart", True, dist_meta_pie()
        if "BarChart" in registered:
            return "BarChart", True, dist_meta_bar()
        if "PieChart" in registered:
            return "PieChart", True, dist_meta_pie()
        return None, False, {}

    if ct == "scatter":
        if "Scatterplot" not in registered:
            return None, False, {}
        xs = [c for c in feature_cols if c and c != target_col]
        if len(xs) < 2:
            return None, False, {}
        x0, x1 = xs[0], xs[1]
        props = {
            "xColumn": x0,
            "yColumn": x1,
            "colorColumn": target_col if target_col else "",
            "xLogScale": False,
            "yLogScale": False,
            "alpha": 1,
        }
        proj_cols = [x0, x1] + ([target_col] if target_col and target_col not in (x0, x1) else [])
        return "Scatterplot", False, {"props": props, "proj_cols": proj_cols}

    if ct == "histogram":
        if "Histogram" not in registered:
            return None, False, {}
        val_col = (
            target_col
            if target_col in feature_cols
            else (feature_cols[0] if feature_cols else target_col)
        )
        props = {"value": val_col, "color": "", "separateBy": "", "marginal": "", "pattern": ""}
        proj_cols = list(dict.fromkeys([c for c in [val_col, target_col] if c]))
        return "Histogram", False, {"props": props, "proj_cols": proj_cols}

    if ct == "bar":
        op, need, meta = pick_dist_chart(prefer_pie_first=False)
        if op:
            return op, need, meta
        return None, False, {}

    if ct == "pie":
        op, need, meta = pick_dist_chart(prefer_pie_first=True)
        if op:
            return op, need, meta
        return None, False, {}

    if ct == "auto":
        op, need, meta = pick_dist_chart(prefer_pie_first=True)
        if op:
            return op, need, meta
        return None, False, {}

    return None, False, {}


def _build_visualization(
    target_col: str,
    feature_cols: list[str],
    dataset_path: str,
    workflow_name: str,
    chart_type: str = "auto",
) -> dict[str, Any]:
    """
    Native path: CSVScan → Projection → (Aggregate if pie/bar) → Chart → Insight UDF.
    Falls back to matplotlib UDF when no chart operators are registered.
    """
    _ = workflow_name
    if not str(target_col or "").strip():
        raise ValueError("target_col is required for visualization workflows")
    registered = _effective_chart_operators()
    native, need_agg, meta = _resolve_viz_native_op(
        chart_type, target_col, feature_cols, registered
    )

    op_csv = _new_op_id("CSVFileScan")
    op_proj = _new_op_id("Projection")
    op_insight = _new_op_id("PythonUDFV2")

    operators: list[dict[str, Any]] = [
        {
            "operatorID": op_csv,
            "operatorType": "CSVFileScan",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "fileEncoding": "UTF_8",
                "customDelimiter": ",",
                "hasHeader": True,
                "fileName": dataset_path,
            },
            "inputPorts": [],
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Read dataset",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": True,
        },
    ]

    links: list[dict[str, Any]] = []
    positions: dict[str, dict[str, int]] = {}

    if native is None:
        # No usable native chart op — UDF + matplotlib (HTML embedding)
        fb_kind = "pie" if chart_type.strip().lower() in ("pie", "auto") else "bar"
        proj_attrs = [{"originalAttribute": target_col}]
        op_fb = _new_op_id("PythonUDFV2")
        operators.append(
            {
                "operatorID": op_proj,
                "operatorType": "Projection",
                "operatorVersion": "N/A",
                "operatorProperties": {"isDrop": False, "attributes": proj_attrs},
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": "Columns for chart",
                "dynamicInputPorts": False,
                "dynamicOutputPorts": False,
                "viewResult": False,
            }
        )
        operators.append(
            {
                "operatorID": op_fb,
                "operatorType": "PythonUDFV2",
                "operatorVersion": "N/A",
                "operatorProperties": _python_udf_props(
                    _matplotlib_chart_fallback_code(target_col, fb_kind),
                    retain_input=False,
                    output_columns=[
                        {"attributeName": "html-content", "attributeType": "string"}
                    ],
                ),
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": "Chart (matplotlib fallback)",
                "dynamicInputPorts": True,
                "dynamicOutputPorts": True,
                "viewResult": True,
            }
        )
        operators.append(
            {
                "operatorID": op_insight,
                "operatorType": "PythonUDFV2",
                "operatorVersion": "N/A",
                "operatorProperties": _python_udf_props(
                    _visualization_insight_code(target_col),
                    retain_input=False,
                    output_columns=_insight_output_columns_5(),
                ),
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": "AI insight",
                "dynamicInputPorts": True,
                "dynamicOutputPorts": True,
                "viewResult": True,
            }
        )
        links = [
            _link(op_csv, "output-0", op_proj, "input-0"),
            _link(op_proj, "output-0", op_fb, "input-0"),
            _link(op_proj, "output-0", op_insight, "input-0"),
        ]
        positions = {
            op_csv: {"x": 0, "y": 160},
            op_proj: {"x": 220, "y": 160},
            op_fb: {"x": 440, "y": 160},
            op_insight: {"x": 680, "y": 160},
        }
        return {
            "operators": operators,
            "operatorPositions": positions,
            "links": links,
            "commentBoxes": [],
            "settings": {"dataTransferBatchSize": 400},
        }

    proj_cols = list(meta.get("proj_cols") or [target_col])
    proj_attrs_v = [{"originalAttribute": c} for c in proj_cols]
    operators.append(
        {
            "operatorID": op_proj,
            "operatorType": "Projection",
            "operatorVersion": "N/A",
            "operatorProperties": {"isDrop": False, "attributes": proj_attrs_v},
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Columns for chart",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        }
    )
    props = meta["props"]
    op_chart = _new_op_id(native)

    if need_agg:
        op_agg = _new_op_id("Aggregate")
        operators.append(
            {
                "operatorID": op_agg,
                "operatorType": "Aggregate",
                "operatorVersion": "N/A",
                "operatorProperties": _aggregate_count_op_props(
                    target_col, target_col, "#count"
                ),
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": "Count by category",
                "dynamicInputPorts": False,
                "dynamicOutputPorts": False,
                "viewResult": False,
            }
        )
        operators.append(
            {
                "operatorID": op_chart,
                "operatorType": native,
                "operatorVersion": "N/A",
                "operatorProperties": props,
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": native,
                "dynamicInputPorts": False,
                "dynamicOutputPorts": False,
                "viewResult": True,
            }
        )
        operators.append(
            {
                "operatorID": op_insight,
                "operatorType": "PythonUDFV2",
                "operatorVersion": "N/A",
                "operatorProperties": _python_udf_props(
                    _visualization_insight_code(target_col),
                    retain_input=False,
                    output_columns=_insight_output_columns_5(),
                ),
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": "AI insight",
                "dynamicInputPorts": True,
                "dynamicOutputPorts": True,
                "viewResult": True,
            }
        )
        links = [
            _link(op_csv, "output-0", op_proj, "input-0"),
            _link(op_proj, "output-0", op_agg, "input-0"),
            _link(op_agg, "output-0", op_chart, "input-0"),
            _link(op_proj, "output-0", op_insight, "input-0"),
        ]
        positions = {
            op_csv: {"x": 0, "y": 160},
            op_proj: {"x": 220, "y": 160},
            op_agg: {"x": 440, "y": 160},
            op_chart: {"x": 660, "y": 160},
            op_insight: {"x": 880, "y": 160},
        }
    else:
        operators.append(
            {
                "operatorID": op_chart,
                "operatorType": native,
                "operatorVersion": "N/A",
                "operatorProperties": props,
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": native,
                "dynamicInputPorts": False,
                "dynamicOutputPorts": False,
                "viewResult": True,
            }
        )
        operators.append(
            {
                "operatorID": op_insight,
                "operatorType": "PythonUDFV2",
                "operatorVersion": "N/A",
                "operatorProperties": _python_udf_props(
                    _visualization_insight_code(target_col),
                    retain_input=False,
                    output_columns=_insight_output_columns_5(),
                ),
                "inputPorts": _single_input_port(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": "AI insight",
                "dynamicInputPorts": True,
                "dynamicOutputPorts": True,
                "viewResult": True,
            }
        )
        links = [
            _link(op_csv, "output-0", op_proj, "input-0"),
            _link(op_proj, "output-0", op_chart, "input-0"),
            _link(op_proj, "output-0", op_insight, "input-0"),
        ]
        positions = {
            op_csv: {"x": 0, "y": 160},
            op_proj: {"x": 220, "y": 160},
            op_chart: {"x": 440, "y": 160},
            op_insight: {"x": 660, "y": 160},
        }

    return {
        "operators": operators,
        "operatorPositions": positions,
        "links": links,
        "commentBoxes": [],
        "settings": {"dataTransferBatchSize": 400},
    }


def build_workflow_json(
    task_type: str,
    target_col: str,
    feature_cols: list[str],
    dataset_path: str,
    workflow_name: str,
    algorithm: str | None = None,
    chart_type: str = "auto",
) -> dict[str, Any]:
    """Return a Texera workflow content dict (operators, links, positions, settings)."""
    t = str(task_type).strip().lower()
    if t not in TASK_TYPES:
        raise ValueError(f"unsupported task_type: {task_type!r}")
    if not target_col and t in ("classification", "regression", "automl"):
        raise ValueError("target_col is required for this task type")
    if t == "classification":
        return _build_classification(
            target_col, feature_cols, dataset_path, workflow_name, algorithm
        )
    if t == "regression":
        return _build_regression(
            target_col, feature_cols, dataset_path, workflow_name, algorithm
        )
    if t == "exploration":
        return _build_exploration(target_col, feature_cols, dataset_path, workflow_name)
    if t == "visualization":
        return _build_visualization(
            target_col, feature_cols, dataset_path, workflow_name, chart_type=chart_type
        )
    return _build_automl(target_col, feature_cols, dataset_path, workflow_name)


def _build_classification(
    target_col: str,
    feature_cols: list[str],
    dataset_path: str,
    workflow_name: str,
    algorithm: str | None,
) -> dict[str, Any]:
    op_csv = _new_op_id("CSVFileScan")
    op_proj = _new_op_id("Projection")
    op_split = _new_op_id("Split")
    op_train = _new_op_id(_map_classifier_operator(algorithm))
    op_pred = _new_op_id("SklearnPrediction")
    op_insight = _new_op_id("PythonUDFV2")

    pred_attr = f"predicted_{target_col}"
    trainers_type = _map_classifier_operator(algorithm)

    operators: list[dict[str, Any]] = [
        {
            "operatorID": op_csv,
            "operatorType": "CSVFileScan",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "fileEncoding": "UTF_8",
                "customDelimiter": ",",
                "hasHeader": True,
                "fileName": dataset_path,
            },
            "inputPorts": [],
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Read dataset",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": True,
        },
        {
            "operatorID": op_proj,
            "operatorType": "Projection",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "isDrop": False,
                "attributes": _projection_attrs(feature_cols, target_col),
            },
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Select features and target",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_split,
            "operatorType": "Split",
            "operatorVersion": "N/A",
            "operatorProperties": {"k": 80, "random": True, "seed": 1},
            "inputPorts": _single_input_port(),
            "outputPorts": _split_outputs(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Train/test split 80/20",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": False,
        },
        {
            "operatorID": op_train,
            "operatorType": trainers_type,
            "operatorVersion": "N/A",
            "operatorProperties": _sklearn_classifier_props(target_col),
            "inputPorts": _sklearn_train_ports(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": trainers_type.replace("Sklearn", ""),
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_pred,
            "operatorType": "SklearnPrediction",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "Model Attribute": "model",
                "Output Attribute Name": pred_attr,
                "Ground Truth Attribute Name to Ignore": target_col,
            },
            "inputPorts": _sklearn_prediction_ports(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Sklearn Prediction",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_insight,
            "operatorType": "PythonUDFV2",
            "operatorVersion": "N/A",
            "operatorProperties": _python_udf_props(
                _classification_insight_code(target_col, trainers_type, feature_cols),
                retain_input=False,
                output_columns=_insight_output_columns_5(),
            ),
            "inputPorts": _udf_prediction_and_model_input_ports(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "AI insight",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": True,
        },
    ]

    links = [
        _link(op_csv, "output-0", op_proj, "input-0"),
        _link(op_proj, "output-0", op_split, "input-0"),
        _link(op_split, "output-0", op_train, "input-0"),
        _link(op_split, "output-1", op_train, "input-1"),
        _link(op_train, "output-0", op_pred, "input-0"),
        _link(op_split, "output-1", op_pred, "input-1"),
        _link(op_pred, "output-0", op_insight, "input-0"),
        _link(op_train, "output-0", op_insight, "input-1"),
    ]

    positions = {
        op_csv: {"x": 0, "y": 160},
        op_proj: {"x": 220, "y": 160},
        op_split: {"x": 440, "y": 160},
        op_train: {"x": 660, "y": 80},
        op_pred: {"x": 880, "y": 160},
        op_insight: {"x": 1100, "y": 160},
    }

    return {
        "operators": operators,
        "operatorPositions": positions,
        "links": links,
        "commentBoxes": [],
        "settings": {"dataTransferBatchSize": 400},
    }


def _build_regression(
    target_col: str,
    feature_cols: list[str],
    dataset_path: str,
    workflow_name: str,
    algorithm: str | None,
) -> dict[str, Any]:
    op_csv = _new_op_id("CSVFileScan")
    op_proj = _new_op_id("Projection")
    op_impute = _new_op_id("PythonUDFV2")
    op_split = _new_op_id("Split")
    train_type = _map_regression_operator(algorithm)
    op_train = _new_op_id(train_type)
    op_pred = _new_op_id("SklearnPrediction")
    op_insight = _new_op_id("PythonUDFV2")
    pred_attr = f"predicted_{target_col}"

    impute_code = (
        "from pytexera import *\n"
        "import pandas as pd\n"
        "\n"
        "class ProcessTableOperator(UDFTableOperator):\n"
        "    @overrides\n"
        "    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n"
        "        df = pd.DataFrame(table)\n"
        "        # Numeric columns: fill NaN with median\n"
        "        for col in df.select_dtypes(include='number').columns:\n"
        "            df[col] = df[col].fillna(df[col].median())\n"
        "        # String/object columns: fill NaN with mode, then label-encode to integers\n"
        "        # This keeps the schema intact while making the data sklearn-compatible.\n"
        "        for col in df.select_dtypes(include='object').columns:\n"
        "            mode_val = df[col].mode().iloc[0] if not df[col].mode().empty else 'unknown'\n"
        "            df[col] = df[col].fillna(mode_val)\n"
        "            # Label encode: 'NEAR BAY' -> 0, 'INLAND' -> 1, etc.\n"
        "            df[col] = pd.Categorical(df[col]).codes.astype('float64')\n"
        "        yield df\n"
    )

    reg_props = (
        _sklearn_linear_regression_props(target_col)
        if train_type == "SklearnLinearRegression"
        else _sklearn_linear_regression_props(target_col)
    )

    operators: list[dict[str, Any]] = [
        {
            "operatorID": op_csv,
            "operatorType": "CSVFileScan",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "fileEncoding": "UTF_8",
                "customDelimiter": ",",
                "hasHeader": True,
                "fileName": dataset_path,
            },
            "inputPorts": [],
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Read dataset",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": True,
        },
        {
            "operatorID": op_proj,
            "operatorType": "Projection",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "isDrop": False,
                "attributes": _projection_attrs(feature_cols, target_col),
            },
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Select features and target",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_impute,
            "operatorType": "PythonUDFV2",
            "operatorVersion": "N/A",
            "operatorProperties": _python_udf_props(impute_code, retain_input=True),
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Handle missing values",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_split,
            "operatorType": "Split",
            "operatorVersion": "N/A",
            "operatorProperties": {"k": 80, "random": True, "seed": 1},
            "inputPorts": _single_input_port(),
            "outputPorts": _split_outputs(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Train/test split 80/20",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": False,
        },
        {
            "operatorID": op_train,
            "operatorType": train_type,
            "operatorVersion": "N/A",
            "operatorProperties": reg_props,
            "inputPorts": _sklearn_train_ports(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Train regressor",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_pred,
            "operatorType": "SklearnPrediction",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "Model Attribute": "model",
                "Output Attribute Name": pred_attr,
                "Ground Truth Attribute Name to Ignore": target_col,
            },
            "inputPorts": _sklearn_prediction_ports(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Sklearn Prediction",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_insight,
            "operatorType": "PythonUDFV2",
            "operatorVersion": "N/A",
            "operatorProperties": _python_udf_props(
                _regression_insight_code(target_col, feature_cols),
                retain_input=False,
                output_columns=_insight_output_columns_5(),
            ),
            "inputPorts": _udf_prediction_and_model_input_ports(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "AI insight",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": True,
        },
    ]

    links = [
        _link(op_csv, "output-0", op_proj, "input-0"),
        _link(op_proj, "output-0", op_impute, "input-0"),
        _link(op_impute, "output-0", op_split, "input-0"),
        _link(op_split, "output-0", op_train, "input-0"),
        _link(op_split, "output-1", op_train, "input-1"),
        _link(op_train, "output-0", op_pred, "input-0"),
        _link(op_split, "output-1", op_pred, "input-1"),
        _link(op_pred, "output-0", op_insight, "input-0"),
        _link(op_train, "output-0", op_insight, "input-1"),
    ]

    positions = {
        op_csv: {"x": 0, "y": 160},
        op_proj: {"x": 220, "y": 160},
        op_impute: {"x": 440, "y": 160},
        op_split: {"x": 660, "y": 160},
        op_train: {"x": 880, "y": 80},
        op_pred: {"x": 1100, "y": 160},
        op_insight: {"x": 1320, "y": 160},
    }

    return {
        "operators": operators,
        "operatorPositions": positions,
        "links": links,
        "commentBoxes": [],
        "settings": {"dataTransferBatchSize": 400},
    }


def _build_exploration(
    target_col: str,
    feature_cols: list[str],
    dataset_path: str,
    workflow_name: str,
) -> dict[str, Any]:
    _ = workflow_name
    op_csv = _new_op_id("CSVFileScan")
    op_proj = _new_op_id("Projection")
    op_pearson = _new_op_id("PythonUDFV2")
    op_insight = _new_op_id("PythonUDFV2")

    # Exploration uses all numeric-ish columns including target for correlation to target.
    attrs: list[dict[str, Any]] = []
    for c in feature_cols:
        attrs.append({"originalAttribute": c})
    if target_col and target_col not in feature_cols:
        attrs.append({"alias": "", "originalAttribute": target_col})

    operators: list[dict[str, Any]] = [
        {
            "operatorID": op_csv,
            "operatorType": "CSVFileScan",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "fileEncoding": "UTF_8",
                "customDelimiter": ",",
                "hasHeader": True,
                "fileName": dataset_path,
            },
            "inputPorts": [],
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Read dataset",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": True,
        },
        {
            "operatorID": op_proj,
            "operatorType": "Projection",
            "operatorVersion": "N/A",
            "operatorProperties": {"isDrop": False, "attributes": attrs},
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Columns for analysis",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_pearson,
            "operatorType": "PythonUDFV2",
            "operatorVersion": "N/A",
            "operatorProperties": _python_udf_props(
                _pearson_code(target_col),
                retain_input=False,
                output_columns=[
                    {"attributeName": "feature", "attributeType": "string"},
                    {"attributeName": "correlation", "attributeType": "double"},
                ],
            ),
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Pearson correlations",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": False,
        },
        {
            "operatorID": op_insight,
            "operatorType": "PythonUDFV2",
            "operatorVersion": "N/A",
            "operatorProperties": _python_udf_props(
                _exploration_insight_code(),
                retain_input=False,
                output_columns=_exploration_insight_output_columns(),
            ),
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Insight summary",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": True,
        },
    ]

    links = [
        _link(op_csv, "output-0", op_proj, "input-0"),
        _link(op_proj, "output-0", op_pearson, "input-0"),
        _link(op_pearson, "output-0", op_insight, "input-0"),
    ]

    positions = {
        op_csv: {"x": 0, "y": 160},
        op_proj: {"x": 220, "y": 160},
        op_pearson: {"x": 440, "y": 160},
        op_insight: {"x": 660, "y": 160},
    }

    return {
        "operators": operators,
        "operatorPositions": positions,
        "links": links,
        "commentBoxes": [],
        "settings": {"dataTransferBatchSize": 400},
    }


def _build_automl(
    target_col: str,
    feature_cols: list[str],
    dataset_path: str,
    workflow_name: str,
) -> dict[str, Any]:
    _ = workflow_name
    op_csv = _new_op_id("CSVFileScan")
    op_proj = _new_op_id("Projection")
    op_split = _new_op_id("Split")
    insight = _new_op_id("PythonUDFV2")

    trainers = [
        ("SklearnLogisticRegression", _new_op_id("SklearnLogisticRegression")),
        ("SklearnDecisionTree", _new_op_id("SklearnDecisionTree")),
        ("SklearnRandomForest", _new_op_id("SklearnRandomForest")),
    ]

    preds: list[tuple[str, str, str]] = []
    for i, (tname, _tid) in enumerate(trainers):
        preds.append((tname, _new_op_id("SklearnPrediction"), f"predicted_{target_col}_m{i}"))

    operators: list[dict[str, Any]] = [
        {
            "operatorID": op_csv,
            "operatorType": "CSVFileScan",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "fileEncoding": "UTF_8",
                "customDelimiter": ",",
                "hasHeader": True,
                "fileName": dataset_path,
            },
            "inputPorts": [],
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Read dataset",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": True,
        },
        {
            "operatorID": op_proj,
            "operatorType": "Projection",
            "operatorVersion": "N/A",
            "operatorProperties": {
                "isDrop": False,
                "attributes": _projection_attrs(feature_cols, target_col),
            },
            "inputPorts": _single_input_port(),
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Select features and target",
            "dynamicInputPorts": False,
            "dynamicOutputPorts": False,
            "viewResult": False,
        },
        {
            "operatorID": op_split,
            "operatorType": "Split",
            "operatorVersion": "N/A",
            "operatorProperties": {"k": 80, "random": True, "seed": 1},
            "inputPorts": _single_input_port(),
            "outputPorts": _split_outputs(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "Train/test split 80/20",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": False,
        },
    ]

    for (tname, train_id) in trainers:
        operators.append(
            {
                "operatorID": train_id,
                "operatorType": tname,
                "operatorVersion": "N/A",
                "operatorProperties": _sklearn_classifier_props(target_col),
                "inputPorts": _sklearn_train_ports(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": tname.replace("Sklearn", ""),
                "dynamicInputPorts": False,
                "dynamicOutputPorts": False,
                "viewResult": False,
            }
        )

    for tname, pred_id, pred_attr in preds:
        operators.append(
            {
                "operatorID": pred_id,
                "operatorType": "SklearnPrediction",
                "operatorVersion": "N/A",
                "operatorProperties": {
                    "Model Attribute": "model",
                    "Output Attribute Name": pred_attr,
                    "Ground Truth Attribute Name to Ignore": target_col,
                },
                "inputPorts": _sklearn_prediction_ports(),
                "outputPorts": _output_port(),
                "showAdvanced": False,
                "isDisabled": False,
                "customDisplayName": f"Predict ({tname})",
                "dynamicInputPorts": False,
                "dynamicOutputPorts": False,
                "viewResult": False,
            }
        )

    insight_ports = []
    for i, (_tname, pred_id, _attr) in enumerate(preds):
        insight_ports.append(
            {
                "portID": f"input-{i}",
                "displayName": f"branch-{i}",
                "allowMultiInputs": True,
                "isDynamicPort": False,
                "dependencies": [],
            }
        )

    operators.append(
        {
            "operatorID": insight,
            "operatorType": "PythonUDFV2",
            "operatorVersion": "N/A",
            "operatorProperties": _python_udf_props(
                _automl_insight_code(target_col),
                retain_input=False,
                output_columns=_insight_output_columns_5(),
            ),
            "inputPorts": insight_ports,
            "outputPorts": _output_port(),
            "showAdvanced": False,
            "isDisabled": False,
            "customDisplayName": "AI insight",
            "dynamicInputPorts": True,
            "dynamicOutputPorts": True,
            "viewResult": True,
        }
    )

    links: list[dict[str, Any]] = [
        _link(op_csv, "output-0", op_proj, "input-0"),
        _link(op_proj, "output-0", op_split, "input-0"),
    ]

    for i, ((_tname, train_id), (_unused, pred_id, _attr)) in enumerate(
        zip(trainers, preds, strict=True)
    ):
        links.append(_link(op_split, "output-0", train_id, "input-0"))
        links.append(_link(op_split, "output-1", train_id, "input-1"))
        links.append(_link(train_id, "output-0", pred_id, "input-0"))
        links.append(_link(op_split, "output-1", pred_id, "input-1"))
        links.append(_link(pred_id, "output-0", insight, f"input-{i}"))

    positions: dict[str, dict[str, int]] = {
        op_csv: {"x": 0, "y": 300},
        op_proj: {"x": 200, "y": 300},
        op_split: {"x": 420, "y": 300},
        insight: {"x": 1100, "y": 300},
    }
    y_off = 0
    for _tname, train_id in trainers:
        positions[train_id] = {"x": 620, "y": y_off}
        y_off += 180
    y_off = 0
    for _tname, pred_id, _a in preds:
        positions[pred_id] = {"x": 820, "y": y_off}
        y_off += 180

    return {
        "operators": operators,
        "operatorPositions": positions,
        "links": links,
        "commentBoxes": [],
        "settings": {"dataTransferBatchSize": 400},
    }
