from unittest.mock import patch

from core.classifier import classify, classify_dataset

PIMA_COLUMNS = [
    "Pregnancies",
    "Glucose",
    "BloodPressure",
    "SkinThickness",
    "Insulin",
    "BMI",
    "DiabetesPedigreeFunction",
    "Age",
    "Outcome",
]

_SAMPLE = [
    {
        "Pregnancies": 6,
        "Glucose": 148,
        "BloodPressure": 72,
        "SkinThickness": 35,
        "Insulin": 0,
        "BMI": 33.6,
        "DiabetesPedigreeFunction": 0.627,
        "Age": 50,
        "Outcome": 1,
    }
]


def _fake_llm_diabetes() -> dict:
    g1 = (
        "Build classification: target Outcome (0/1) with SklearnLogisticRegression, "
        "train/test Split 70/30, SklearnPrediction on test rows, Scatterplot predictions."
    )
    g2 = (
        "Train SklearnRandomForest for feature importance; BarChart; target Outcome."
    )
    g3 = "KMeans via PythonUDFV2 on numeric vitals (exclude Outcome); Scatterplot clusters."
    g4 = (
        "Parallel SklearnLogisticRegression, SklearnDecisionTree, SklearnRandomForest; "
        "each with SklearnPrediction; PythonUDFV2 compare accuracy."
    )
    feats = [
        "Pregnancies",
        "Glucose",
        "BloodPressure",
        "SkinThickness",
        "Insulin",
        "BMI",
        "DiabetesPedigreeFunction",
        "Age",
    ]
    return {
        "scenario_label": "diabetes risk",
        "dataset_summary": "Pima Indians diabetes data with clinical measurements.",
        "target_column": "Outcome",
        "confidence": 0.9,
        "suggestions": [
            {
                "id": "card_a",
                "title": "Predict Diabetes Outcome",
                "description": "Predict onset from clinical vitals using logistic regression.",
                "task_type": "classification",
                "goal_for_agent": g1,
                "target_column": "Outcome",
                "algorithm": "SklearnLogisticRegression",
                "feature_cols": feats,
            },
            {
                "id": "card_b",
                "title": "Identify Key Risk Factors",
                "description": "Use a random forest for feature importance and a bar chart.",
                "task_type": "exploration",
                "goal_for_agent": g2,
                "target_column": "Outcome",
                "algorithm": "SklearnRandomForest",
                "feature_cols": feats,
            },
            {
                "id": "card_c",
                "title": "Cluster Patients into Risk Groups",
                "description": "Unsupervised KMeans on numeric vitals (exclude Outcome).",
                "task_type": "exploration",
                "goal_for_agent": g3,
                "target_column": None,
                "algorithm": None,
                "feature_cols": feats,
            },
            {
                "id": "card_d_automl",
                "title": "✨ Find Best Model Automatically",
                "description": "Compare several classifiers side by side on the same split.",
                "task_type": "automl",
                "goal_for_agent": g4,
                "target_column": "Outcome",
                "algorithm": None,
                "feature_cols": feats,
            },
        ],
    }


@patch("core.classifier.chat_completion")
def test_classifier_llm_returns_suggestions_with_goals(mock_chat):
    mock_chat.return_value = _fake_llm_diabetes()
    result = classify_dataset(PIMA_COLUMNS, _SAMPLE, row_count=768)
    assert result["scenario_label"] == "diabetes risk"
    assert len(result["suggestions"]) == 4
    assert any(s.get("task_type") == "exploration" for s in result["suggestions"])
    for s in result["suggestions"]:
        assert s.get("task_type") in (
            "classification",
            "regression",
            "exploration",
            "automl",
            "visualization",
        )
        assert isinstance(s.get("feature_cols"), list)
        assert len(s["feature_cols"]) >= 1


@patch("core.classifier.chat_completion")
def test_classifier_enforces_at_least_one_exploration_when_llm_over_classifies(mock_chat):
    feats = PIMA_COLUMNS[:-1]
    mock_chat.return_value = {
        "scenario_label": "diabetes",
        "dataset_summary": "clinical data",
        "target_column": "Outcome",
        "confidence": 0.9,
        "suggestions": [
            {
                "id": "card_a",
                "title": "Predict outcome",
                "description": "Classifier",
                "task_type": "classification",
                "target_column": "Outcome",
                "feature_cols": feats,
            },
            {
                "id": "card_b",
                "title": "Another predictor",
                "description": "More classification",
                "task_type": "classification",
                "target_column": "Outcome",
                "feature_cols": feats,
            },
            {
                "id": "card_c",
                "title": "Third predictor",
                "description": "Still classification",
                "task_type": "classification",
                "target_column": "Outcome",
                "feature_cols": feats,
            },
            {
                "id": "card_d_automl",
                "title": "AutoML",
                "description": "Compare",
                "task_type": "automl",
                "target_column": "Outcome",
                "feature_cols": feats,
            },
        ],
    }
    result = classify_dataset(PIMA_COLUMNS, _SAMPLE, row_count=10)
    assert any(s.get("task_type") == "exploration" for s in result["suggestions"])
    assert any(s.get("task_type") == "classification" for s in result["suggestions"])


@patch("core.classifier.chat_completion", side_effect=RuntimeError("LLM down"))
def test_classifier_llm_failure_emits_error_card(mock_chat):
    result = classify_dataset(PIMA_COLUMNS, _SAMPLE, row_count=10)
    assert result.get("llm_error") is True
    assert len(result["suggestions"]) == 1
    assert result["suggestions"][0].get("error") is True


@patch("core.classifier.chat_completion")
def test_classify_adapter_returns_detected_scenario_alias(mock_chat):
    mock_chat.return_value = _fake_llm_diabetes()
    out = classify(PIMA_COLUMNS)
    assert out["detected_scenario"] == out["scenario_label"]
    assert "dataset_summary" in out
    assert len(out["suggestions"]) >= 3


def test_empty_columns_errors_without_llm_call():
    with patch("core.classifier.chat_completion") as mock_chat:
        out = classify([])
        mock_chat.assert_not_called()
    assert out["llm_error"] is True


@patch("core.classifier.chat_completion")
def test_free_text_pie_chart_routes_to_visualization(mock_chat):
    """Chart keywords override LLM classification."""
    mock_chat.return_value = {
        "task_type": "classification",
        "target_column": "Outcome",
        "algorithm": "SklearnLogisticRegression",
        "feature_cols": PIMA_COLUMNS[:-1],
    }
    from core.classifier import infer_task_from_free_text

    out = infer_task_from_free_text(
        "show me a pie chart of diabetes outcomes",
        columns=PIMA_COLUMNS,
        sample_rows=_SAMPLE,
        row_count=768,
        dataset_summary="Pima",
        scenario_label="diabetes",
    )
    assert out["task_type"] == "visualization"
    assert out.get("chart_type") == "pie"
