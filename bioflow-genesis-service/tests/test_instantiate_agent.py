from fastapi.testclient import TestClient

from core import upload_cache
from main import app

client = TestClient(app)


def _post(payload):
    return client.post("/api/genesis/instantiate", json=payload)


def test_template_mode_default_is_unchanged():
    """Omitting mode keeps template behavior; response echoes `mode: \"template\"`."""
    r = _post({
        "suggestion_id": "diabetes_prediction",
        "dataset_id": 5,
        "file_path": "/texera/diabetes/v1/diabetes.csv",
        "target_column": "Outcome",
    })
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"mode", "workflow_name", "workflow_content"}
    assert body["mode"] == "template"
    assert isinstance(body["workflow_content"], str)


def test_template_mode_explicit():
    r = _post({
        "suggestion_id": "diabetes_prediction",
        "dataset_id": 5,
        "file_path": "/texera/diabetes/v1/diabetes.csv",
        "target_column": "Outcome",
        "mode": "template",
    })
    assert r.status_code == 200
    assert "workflow_content" in r.json()


def test_agent_mode_without_upload_returns_400():
    r = _post({
        "suggestion_id": "diabetes_prediction",
        "dataset_id": 5,
        "file_path": "/texera/diabetes/v1/diabetes.csv",
        "target_column": "Outcome",
        "columns": ["Pregnancies", "Glucose", "BMI", "Outcome"],
        "mode": "agent",
    })
    assert r.status_code == 400
    assert "build" in r.json()["error"].lower()


def test_agent_mode_with_upload_returns_400_deprecation():
    uid = upload_cache.create_upload_id()
    upload_cache.put(
        uid,
        {
            "file_path": "/texera/diabetes/v1/diabetes.csv",
            "dataset_id": 5,
            "columns": ["Pregnancies", "Glucose", "Outcome"],
            "sample_rows": [[1, 85, 0]],
            "dataset_summary": "demo",
            "scenario_label": "diabetes",
            "suggestions": [
                {
                    "id": "dyn_pred",
                    "title": "Predict",
                    "description": "Classifier",
                    "task_type": "classification",
                    "goal_for_agent": "",
                    "target_column": "Outcome",
                },
                {
                    "id": "x2",
                    "title": "Two",
                    "description": "d",
                    "task_type": "regression",
                    "goal_for_agent": "",
                },
                {
                    "id": "x3",
                    "title": "Three",
                    "description": "d",
                    "task_type": "exploration",
                    "goal_for_agent": "",
                },
            ],
        },
    )
    r = _post({
        "upload_id": uid,
        "suggestion_id": "dyn_pred",
        "mode": "agent",
    })
    assert r.status_code == 400
    assert "build" in r.json()["error"].lower()


def test_custom_goal_instantiate_returns_400():
    uid = upload_cache.create_upload_id()
    upload_cache.put(
        uid,
        {
            "file_path": "/texera/x/v1/y.csv",
            "dataset_id": 2,
            "columns": ["a", "b"],
            "sample_rows": [[1, 2]],
            "dataset_summary": "",
            "scenario_label": "",
        },
    )
    r = _post({
        "upload_id": uid,
        "suggestion_id": "ignored",
        "mode": "agent",
        "custom_goal": "Plot a and b with Scatterplot.",
    })
    assert r.status_code == 400


def test_agent_mode_unknown_suggestion_returns_404():
    uid = upload_cache.create_upload_id()
    upload_cache.put(
        uid,
        {
            "file_path": "/x.csv",
            "dataset_id": 1,
            "columns": ["a"],
            "sample_rows": [],
            "suggestions": [],
        },
    )
    r = _post({
        "upload_id": uid,
        "suggestion_id": "totally_made_up",
        "dataset_id": 1,
        "file_path": "/x.csv",
        "target_column": "y",
        "mode": "agent",
    })
    assert r.status_code == 404


def test_invalid_mode_value_rejected():
    """Pydantic Literal should reject anything outside template|agent."""
    r = _post({
        "suggestion_id": "diabetes_prediction",
        "dataset_id": 1,
        "file_path": "/x.csv",
        "target_column": "y",
        "mode": "magic",
    })
    assert r.status_code == 422
