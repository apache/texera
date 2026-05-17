import httpx
import pytest

from core.texera_client import TexeraClient, TexeraClientError


def test_find_dataset_id_handles_nested_dashboard_dataset_shape():
    """The file-service /api/dataset/list returns List[DashboardDataset], where
    each item wraps the actual Dataset under a `dataset` key. The parser must
    look at entry.dataset.name + entry.dataset.did, not the top-level.
    """
    client = TexeraClient()
    entries = [
        {
            "dataset": {"did": 5, "name": "diabetes", "description": "x"},
            "ownerEmail": "texera",
            "accessPrivilege": "WRITE",
            "isOwner": True,
            "size": 0,
        },
        {
            "dataset": {"did": 7, "name": "iris"},
            "ownerEmail": "texera",
            "isOwner": True,
        },
    ]
    assert client._find_dataset_id_by_name(entries, "diabetes") == 5
    assert client._find_dataset_id_by_name(entries, "iris") == 7
    assert client._find_dataset_id_by_name(entries, "missing") is None


def test_find_dataset_id_falls_back_to_top_level_shape():
    """Old/alternate shape with name+did at the top level still works."""
    client = TexeraClient()
    entries = [{"did": 9, "name": "diabetes"}]
    assert client._find_dataset_id_by_name(entries, "diabetes") == 9


def test_dataset_entries_unwraps_common_dict_keys():
    client = TexeraClient()
    item = {"dataset": {"did": 1, "name": "x"}, "isOwner": True}
    assert client._dataset_entries_from_list_body([item]) == [item]
    assert client._dataset_entries_from_list_body({"datasets": [item]}) == [item]
    assert client._dataset_entries_from_list_body({"data": [item]}) == [item]
    assert client._dataset_entries_from_list_body({"unrelated": 1}) == []


# ---------- step 5 (version/create) idempotency ----------


def _make_mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_resolve_version_segment_happy_path_uses_create_response():
    tc = TexeraClient()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/dataset/6/version/create"
        return httpx.Response(
            200,
            json={"datasetVersion": {"dvid": 1, "name": "v1"}, "fileNodes": []},
        )

    with _make_mock_client(handler) as client:
        seg = tc._resolve_version_segment(client, {"Authorization": "Bearer x"}, 6)
    assert seg == "v1"


def test_resolve_version_segment_falls_back_to_latest_on_no_changes_detected():
    """When uploading byte-identical content, Texera replies
    400 "No changes detected in dataset. Version creation aborted." We must
    GET /version/latest and reuse that version instead of 502-ing."""
    tc = TexeraClient()
    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(f"{request.method} {request.url.path}")
        if request.url.path == "/api/dataset/6/version/create":
            return httpx.Response(
                400,
                json={
                    "code": 400,
                    "message": "No changes detected in dataset. Version creation aborted.",
                },
            )
        if request.url.path == "/api/dataset/6/version/latest":
            return httpx.Response(
                200,
                json={
                    "datasetVersion": {"dvid": 1, "name": "v1"},
                    "fileNodes": [],
                },
            )
        return httpx.Response(404)

    with _make_mock_client(handler) as client:
        seg = tc._resolve_version_segment(client, {"Authorization": "Bearer x"}, 6)
    assert seg == "v1"
    assert call_log == [
        "POST /api/dataset/6/version/create",
        "GET /api/dataset/6/version/latest",
    ]


def test_resolve_version_segment_raises_on_other_400():
    tc = TexeraClient()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"code": 400, "message": "something else"})

    with _make_mock_client(handler) as client:
        with pytest.raises(TexeraClientError):
            tc._resolve_version_segment(client, {"Authorization": "Bearer x"}, 6)


def test_resolve_version_segment_raises_when_latest_also_fails():
    """If /version/create says 'no changes' but /version/latest is broken, we
    should surface a real error rather than silently inventing a version."""
    tc = TexeraClient()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/version/create"):
            return httpx.Response(
                400, json={"message": "No changes detected in dataset."}
            )
        return httpx.Response(500, text="boom")

    with _make_mock_client(handler) as client:
        with pytest.raises(TexeraClientError):
            tc._resolve_version_segment(client, {"Authorization": "Bearer x"}, 6)
