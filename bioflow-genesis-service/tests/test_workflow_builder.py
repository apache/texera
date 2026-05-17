"""Unit tests for core.workflow_builder (deterministic Genesis workflows)."""

from __future__ import annotations

import json

from core.workflow_builder import build_workflow_json


def test_classification_builds_6_ops_8_links_insight_dual_input():
    """Iris-style ML chain: Split feeds trainer+pred; insight merges predictions + model."""
    wf = build_workflow_json(
        "classification",
        "species",
        ["SepalLengthCm", "SepalWidthCm"],
        "/texera/iris/v1/Iris.csv",
        "[Genesis] Iris demo",
        algorithm="SklearnLogisticRegression",
    )
    assert len(wf["operators"]) == 6
    assert len(wf["links"]) == 8
    types = [o["operatorType"] for o in wf["operators"]]
    assert types.count("SklearnLogisticRegression") == 1
    split = next(o for o in wf["operators"] if o["operatorType"] == "Split")
    train = next(o for o in wf["operators"] if o["operatorType"] == "SklearnLogisticRegression")
    pred = next(o for o in wf["operators"] if o["operatorType"] == "SklearnPrediction")
    to_train = [l for l in wf["links"] if l["target"]["operatorID"] == train["operatorID"]]
    assert {l["source"]["portID"] for l in to_train} == {"output-0", "output-1"}
    assert all(l["source"]["operatorID"] == split["operatorID"] for l in to_train)
    to_pred = [l for l in wf["links"] if l["target"]["operatorID"] == pred["operatorID"]]
    srcs = {(l["source"]["operatorID"], l["source"]["portID"]) for l in to_pred}
    assert (train["operatorID"], "output-0") in srcs
    assert (split["operatorID"], "output-1") in srcs
    insight = next(o for o in wf["operators"] if o.get("customDisplayName") == "AI insight")
    assert {p["portID"] for p in insight["inputPorts"]} == {"input-0", "input-1"}
    to_insight = [l for l in wf["links"] if l["target"]["operatorID"] == insight["operatorID"]]
    assert len(to_insight) == 2


def test_regression_uses_linear_regression():
    wf = build_workflow_json(
        "regression",
        "price",
        ["sqft"],
        "/texera/homes/v1/data.csv",
        "[Genesis] Reg",
        None,
    )
    types = [o["operatorType"] for o in wf["operators"]]
    assert "SklearnLinearRegression" in types


def test_regression_includes_nan_imputation_between_projection_and_split():
    """Regression skeleton must impute NaNs before Split so sklearn trainers don't crash on missing values."""
    wf = build_workflow_json(
        "regression",
        "median_house_value",
        ["total_rooms", "total_bedrooms"],
        "/texera/housing/v1/houses.csv",
        "[Genesis] Reg",
        None,
    )
    assert len(wf["operators"]) == 7
    impute = next(
        o for o in wf["operators"] if o.get("customDisplayName") == "Handle missing values"
    )
    assert impute["operatorType"] == "PythonUDFV2"
    code = impute["operatorProperties"]["code"]
    assert "fillna" in code
    assert "median" in code
    proj = next(o for o in wf["operators"] if o["operatorType"] == "Projection")
    split = next(o for o in wf["operators"] if o["operatorType"] == "Split")
    proj_to_impute = [
        l
        for l in wf["links"]
        if l["source"]["operatorID"] == proj["operatorID"]
        and l["target"]["operatorID"] == impute["operatorID"]
    ]
    impute_to_split = [
        l
        for l in wf["links"]
        if l["source"]["operatorID"] == impute["operatorID"]
        and l["target"]["operatorID"] == split["operatorID"]
    ]
    assert len(proj_to_impute) == 1
    assert len(impute_to_split) == 1
    # Projection no longer feeds Split directly.
    assert not any(
        l["source"]["operatorID"] == proj["operatorID"]
        and l["target"]["operatorID"] == split["operatorID"]
        for l in wf["links"]
    )


def test_exploration_4_nodes_pearson_sorted_no_sort_op():
    """Exploration: CSV → Projection → Pearson UDF (sorted) → Insight UDF."""
    wf = build_workflow_json(
        "exploration",
        "Outcome",
        ["Glucose", "BMI"],
        "/texera/pima/v1/diabetes.csv",
        "[Genesis] Explore",
    )
    types = [o["operatorType"] for o in wf["operators"]]
    assert types.count("CSVFileScan") == 1
    assert types.count("Sort") == 0
    assert types.count("PythonUDFV2") == 2
    assert len(wf["operators"]) == 4
    pearson = next(
        o for o in wf["operators"] if o.get("customDisplayName") == "Pearson correlations"
    )
    cols = pearson["operatorProperties"].get("outputColumns") or []
    assert [c.get("attributeName") for c in cols] == ["feature", "correlation"]


def test_automl_3_parallel_trainers_all_with_double_input():
    wf = build_workflow_json(
        "automl",
        "Outcome",
        ["Glucose", "BMI"],
        "/texera/pima/v1/diabetes.csv",
        "[Genesis] AutoML",
    )
    trainer_types = {
        "SklearnLogisticRegression",
        "SklearnDecisionTree",
        "SklearnRandomForest",
    }
    trainers = [o for o in wf["operators"] if o["operatorType"] in trainer_types]
    assert len(trainers) == 3
    for t in trainers:
        assert len(t["inputPorts"]) == 2
        assert {p["portID"] for p in t["inputPorts"]} == {"input-0", "input-1"}
    insight_ops = [o for o in wf["operators"] if o["operatorType"] == "PythonUDFV2"]
    assert len(insight_ops) == 1
    assert insight_ops[0].get("customDisplayName") == "AI insight"
    code = insight_ops[0]["operatorProperties"]["code"]
    assert "_STATE" not in code
    assert "self._port_metrics" in code
    compile(code, "<automl_insight>", "exec")


def test_sklearn_prediction_input_ports_are_strings():
    wf = build_workflow_json(
        "classification",
        "y",
        ["a", "b"],
        "/d.csv",
        "w",
        algorithm="SklearnDecisionTree",
    )
    preds = [o for o in wf["operators"] if o["operatorType"] == "SklearnPrediction"]
    assert len(preds) == 1
    ports = preds[0]["inputPorts"]
    assert ports[0]["portID"] == "input-0"
    assert ports[1]["portID"] == "input-1"


def test_classification_insight_output_columns_are_short_text_fields():
    wf = build_workflow_json(
        "classification",
        "y",
        ["a", "b"],
        "/d.csv",
        "w",
        algorithm="SklearnRandomForest",
    )
    insight_op = next(o for o in wf["operators"] if o.get("customDisplayName") == "AI insight")
    names = [c["attributeName"] for c in insight_op["operatorProperties"]["outputColumns"]]
    assert names == ["summary", "top_predictors", "interpretation", "next_steps", "caveat"]


def test_insight_code_has_target_col_substituted():
    wf = build_workflow_json(
        "classification",
        "Outcome",
        ["Glucose"],
        "/d.csv",
        "w",
    )
    insight_op = next(
        o for o in wf["operators"] if o.get("customDisplayName") == "AI insight"
    )
    code = insight_op["operatorProperties"]["code"]
    assert "Outcome" in code
    assert "{TARGET" not in code
    assert "op_hint" not in code
    assert "SklearnLogisticRegression" in code or "Sklearn" in code
    compile(code, "<classification_insight>", "exec")
    # JSON-safe embedded code (parses as part of workflow dump)
    json.dumps(wf)


def test_visualization_pie_chart_builds_3_nodes():
    """Visualization (pie): CSV → Projection → Aggregate → PieChart → insight UDF."""
    wf = build_workflow_json(
        "visualization",
        "Outcome",
        ["Glucose", "BMI"],
        "/texera/pima/v1/diabetes.csv",
        "[Genesis] Viz",
        chart_type="pie",
    )
    types = [o["operatorType"] for o in wf["operators"]]
    assert types.count("CSVFileScan") == 1
    assert types.count("Projection") == 1
    assert "PieChart" in types
    assert types.count("Aggregate") == 1
    assert types.count("PythonUDFV2") == 1
    assert len(wf["operators"]) == 5
    insight = next(o for o in wf["operators"] if o.get("customDisplayName") == "AI insight")
    assert insight["operatorType"] == "PythonUDFV2"


def test_visualization_pie_falls_back_to_bar_when_pie_disabled(monkeypatch):
    import core.workflow_builder as wb

    monkeypatch.setattr(wb, "_effective_chart_operators", lambda: frozenset({"BarChart"}))
    wf = build_workflow_json(
        "visualization",
        "Outcome",
        ["Glucose"],
        "/d.csv",
        "w",
        chart_type="pie",
    )
    assert any(o["operatorType"] == "BarChart" for o in wf["operators"])
    assert not any(o["operatorType"] == "PieChart" for o in wf["operators"])
