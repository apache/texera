import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from core import upload_cache
from main import app

client = TestClient(app)


def test_health_endpoint():
    r = client.get("/api/genesis/health")
    assert r.status_code == 200
    body = r.json()
    assert body == {"status": "ok", "version": "0.2.0"}


@patch("api.analyze.classify_dataset")
def test_analyze_endpoint_with_upload_id(mock_classify):
    uid = upload_cache.create_upload_id()
    upload_cache.put(
        uid,
        {
            "file_path": "/texera/diabetes/v1/diabetes.csv",
            "dataset_id": 5,
            "dataset_name": "diabetes",
            "columns": [
                "Pregnancies",
                "Glucose",
                "BloodPressure",
                "SkinThickness",
                "Insulin",
                "BMI",
                "DiabetesPedigreeFunction",
                "Age",
                "Outcome",
            ],
            "sample_rows": [[6, 148, 72, 35, 0, 33.6, 0.627, 50, 1]],
            "row_count": 768,
        },
    )
    mock_classify.return_value = {
        "scenario_label": "diabetes",
        "dataset_summary": "Pima Indians diabetes cohort.",
        "target_column": "Outcome",
        "confidence": 0.88,
        "suggestions": [
            {
                "id": "card_a",
                "title": "T1",
                "description": "D1",
                "goal_for_agent": "G1",
                "analysis_type": "classification",
                "target_column": "Outcome",
                "estimated_runtime_seconds": 12,
            },
            {
                "id": "card_b",
                "title": "T2",
                "description": "D2",
                "goal_for_agent": "G2",
                "analysis_type": "regression",
                "target_column": "Outcome",
                "estimated_runtime_seconds": 12,
            },
            {
                "id": "card_c",
                "title": "T3",
                "description": "D3",
                "goal_for_agent": "G3",
                "analysis_type": "clustering",
                "target_column": None,
                "estimated_runtime_seconds": 12,
            },
        ],
    }
    r = client.post("/api/genesis/analyze", json={"upload_id": uid})
    assert r.status_code == 200
    body = r.json()
    assert body["upload_id"] == uid
    assert body["dataset_summary"] == "Pima Indians diabetes cohort."
    assert body["detected_scenario"] == "diabetes"
    assert body["row_count"] == 768
    assert len(body["suggestions"]) == 3
    for s in body["suggestions"]:
        assert {"id", "title", "description", "goal_for_agent", "estimated_runtime_seconds"} <= set(
            s.keys()
        )


@patch("api.analyze.classify_dataset")
def test_analyze_legacy_columns(mock_classify):
    mock_classify.return_value = {
        "scenario_label": "iris",
        "dataset_summary": "Iris flower measurements.",
        "target_column": "species",
        "confidence": 0.7,
        "suggestions": [
            {
                "id": "c1",
                "title": "Classify",
                "description": "Species from measures.",
                "goal_for_agent": "Classify species with native sklearn ops.",
                "analysis_type": "classification",
                "target_column": "species",
                "estimated_runtime_seconds": 12,
            },
            {
                "id": "c2",
                "title": "Regress",
                "description": "Numeric.",
                "goal_for_agent": "Regression goal.",
                "analysis_type": "regression",
                "target_column": "sepal_length",
                "estimated_runtime_seconds": 12,
            },
            {
                "id": "c3",
                "title": "Clusters",
                "description": "Groups.",
                "goal_for_agent": "Clustering goal.",
                "analysis_type": "clustering",
                "target_column": None,
                "estimated_runtime_seconds": 12,
            },
        ],
    }
    payload = {
        "dataset_id": 3,
        "dataset_name": "iris",
        "file_path": "/texera/iris/v1/iris.csv",
        "columns": ["sepal_length", "species"],
        "sample_rows": [[5.1, "setosa"]],
        "row_count": 150,
    }
    r = client.post("/api/genesis/analyze", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["detected_scenario"] == "iris"


def test_scenarios_endpoint_empty():
    r = client.get("/api/genesis/scenarios")
    assert r.status_code == 200
    body = r.json()
    assert body["scenarios"] == []


def test_instantiate_substitutes_placeholders():
    payload = {
        "suggestion_id": "diabetes_prediction",
        "dataset_id": 5,
        "file_path": "/texera/diabetes/v1/diabetes.csv",
        "target_column": "Outcome",
    }
    r = client.post("/api/genesis/instantiate", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["workflow_name"]
    content = body["workflow_content"]
    assert isinstance(content, str)
    json.loads(content)
    assert "{{DATASET_PATH}}" not in content
    assert "{{TARGET_COLUMN}}" not in content
    assert "{{DATASET_ID}}" not in content
    assert "/texera/diabetes/v1/diabetes.csv" in content
    assert "Outcome" in content


def test_instantiate_unknown_suggestion_returns_404():
    r = client.post(
        "/api/genesis/instantiate",
        json={
            "suggestion_id": "nope_xyz",
            "dataset_id": 1,
            "file_path": "/x/v1/f.csv",
            "target_column": "y",
        },
    )
    assert r.status_code == 404
    assert "suggestion_id not found" in r.json()["error"]


@patch("api.build.TexeraClient")
def test_build_endpoint_creates_workflow(mock_tc_cls):
    mock_inst = mock_tc_cls.return_value
    mock_inst.create_workflow_from_dict.return_value = 42

    uid = upload_cache.create_upload_id()
    upload_cache.put(
        uid,
        {
            "file_path": "/texera/x/v1/y.csv",
            "dataset_id": 9,
            "columns": ["a", "b", "label"],
            "dataset_summary": "Test set.",
            "scenario_label": "demo",
            "target_column": "label",
            "suggestions": [
                {
                    "id": "dyn_1",
                    "title": "Dynamic One",
                    "description": "Do classification.",
                    "task_type": "classification",
                    "goal_for_agent": "",
                    "target_column": "label",
                    "algorithm": "SklearnLogisticRegression",
                    "feature_cols": ["a", "b"],
                },
                {
                    "id": "x2",
                    "title": "Two",
                    "description": "d",
                    "task_type": "regression",
                    "goal_for_agent": "",
                    "target_column": "label",
                },
                {
                    "id": "x3",
                    "title": "Three",
                    "description": "d",
                    "task_type": "exploration",
                    "goal_for_agent": "",
                    "target_column": None,
                },
            ],
        },
    )

    r = client.post(
        "/api/genesis/build",
        json={"upload_id": uid, "card_index": 0, "jwt_token": "test-jwt"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["wid"] == 42
    assert "[Genesis]" in body["workflow_name"]
    mock_inst.create_workflow_from_dict.assert_called_once()
