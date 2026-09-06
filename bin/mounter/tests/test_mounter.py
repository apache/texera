# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Unit tests for the texera-mounter (`bin/mounter/mounter.py`).

Covers the logic that is easy to get wrong and expensive to debug in a cluster: how
mounts are discovered (from /proc/mounts, so a mount whose FUSE server died is still
seen), how a CU's mounts are torn down when its pod goes away, and how pod-watch
events are interpreted. The mounting itself is GeeseFS's job and is not re-tested
here — only the command we build for it and how we react to it failing."""

from __future__ import annotations

import json
import os

import pytest


# ─────────────────── mount_targets_under() / is_mounted() ───────────────────

def test_mount_targets_are_found_under_a_path_deepest_first(mounter, cu_dir):
    _, target = cu_dir("7")
    parent = os.path.dirname(target)
    mounter.set_mounts(parent, target)

    found = mounter.mount_targets_under(os.path.join(mounter.MOUNT_ROOT, "7"))

    # Deepest first, so nested mounts are detached before the ones containing them.
    assert found == [target, parent]


def test_mount_targets_exclude_unrelated_and_sibling_paths(mounter, cu_dir):
    _, seven = cu_dir("7")
    _, eight = cu_dir("8")
    mounter.set_mounts(seven, eight, "/var/lib/something-else")

    assert mounter.mount_targets_under(os.path.join(mounter.MOUNT_ROOT, "7")) == [seven]


def test_mount_targets_do_not_match_a_path_that_is_only_a_string_prefix(mounter):
    # "/mnt/7x" must not be treated as living under "/mnt/7".
    seven = os.path.join(mounter.MOUNT_ROOT, "7")
    mounter.set_mounts(seven + "x")

    assert mounter.mount_targets_under(seven) == []


def test_mount_targets_decode_escaped_characters(mounter):
    spaced = os.path.join(mounter.MOUNT_ROOT, "7", "data set", "abc")
    mounter.set_mounts(spaced)

    assert mounter.mount_targets_under(mounter.MOUNT_ROOT) == [spaced]


def test_mount_targets_survive_an_unreadable_proc_mounts(mounter):
    mounter.PROC_MOUNTS = os.path.join(mounter.MOUNT_ROOT, "does-not-exist")

    assert mounter.mount_targets_under(mounter.MOUNT_ROOT) == []
    assert any("failed" in line for line in mounter.logs)


def test_is_mounted_matches_the_path_itself_not_its_children(mounter, cu_dir):
    _, target = cu_dir("7")
    mounter.set_mounts(target)

    assert mounter.is_mounted(target)
    assert not mounter.is_mounted(os.path.dirname(target))


def test_a_dead_mount_is_still_reported_as_mounted(mounter, cu_dir):
    """The regression this whole module exists for.

    Restarting the mounter kills every GeeseFS it started. The mount entry survives in
    /proc/mounts but stat() on it fails, so os.path.ismount() — what this used to
    use — reports False, and the mount was never cleaned up."""
    _, target = cu_dir("7")
    mounter.set_mounts(target)

    assert not os.path.ismount(target)  # what the buggy version asked
    assert mounter.is_mounted(target)  # what it should have asked


# ─────────────────── _cuid_of() ───────────────────

@pytest.mark.parametrize(
    "pod_name, expected",
    [
        ("computing-unit-17", "17"),
        ("computing-unit-0", "0"),
        ("computing-unit-", None),  # no cuid at all
        ("texera-file-service-abc", None),
        ("", None),
    ],
)
def test_cuid_is_parsed_only_from_computing_unit_pod_names(mounter, pod_name, expected):
    assert mounter._cuid_of(pod_name) == expected


def test_the_pod_name_prefix_is_configurable(mounter, monkeypatch):
    """The helm chart gives this and the CU manager the same prefix; honour it."""
    monkeypatch.setattr(mounter, "CU_POD_NAME_PREFIX", "other-deployment-cu")

    assert mounter._cuid_of("other-deployment-cu-17") == "17"
    assert mounter._cuid_of("computing-unit-17") is None


# ─────────────────── do_mount() ───────────────────

@pytest.mark.parametrize("missing", ["cuid", "repo", "commit", "jwt", "base"])
def test_do_mount_rejects_incomplete_requests(mounter, missing):
    args = {"cuid": "7", "repo": "dataset-1", "commit": "abc", "jwt": "t", "base": "http://fs"}
    args[missing] = ""

    with pytest.raises(ValueError):
        mounter.do_mount(args["cuid"], args["repo"], args["commit"], args["jwt"], args["base"])

    assert mounter.runs == []


def test_do_mount_passes_the_jwt_as_the_s3_access_key(mounter):
    mounter.do_mount("7", "dataset-1", "abc", "the-user-jwt", "http://file-service:9092")

    cmd, kwargs = mounter.runs[0]
    assert cmd[0] == "geesefs"
    assert "--endpoint" in cmd and "http://file-service:9092" in cmd
    assert "dataset-1:abc" in cmd
    # allow_other: the mounter is root but the UDF is not, so the propagated mount must
    # be readable by another uid. ro: mounts are never writable.
    assert "ro,allow_other" in cmd
    # The pod's own JWT is the credential; no global LakeFS secret ever reaches the node.
    assert kwargs["env"]["AWS_ACCESS_KEY_ID"] == "the-user-jwt"


def test_do_mount_is_idempotent_for_a_live_mount(mounter, cu_dir):
    _, target = cu_dir("7", commit="abc")
    mounter.set_mounts(target)

    assert mounter.do_mount("7", "dataset-1", "abc", "jwt", "http://fs") == target
    assert mounter.runs == []  # no second GeeseFS for an already-mounted commit


def test_do_mount_replaces_a_dead_mount(mounter, cu_dir, monkeypatch):
    _, target = cu_dir("7", commit="abc")
    mounter.set_mounts(target)
    monkeypatch.setattr(mounter, "_responds", lambda path: False)  # FUSE server is gone

    assert mounter.do_mount("7", "dataset-1", "abc", "jwt", "http://fs") == target

    commands = [cmd[0] for cmd, _ in mounter.runs]
    assert commands == ["umount", "geesefs"]  # detached first, then mounted again
    assert any("dead mount" in line for line in mounter.logs)


def test_do_mount_reports_what_geesefs_printed_when_it_fails(mounter):
    mounter.geesefs_returncode = 1

    with pytest.raises(RuntimeError, match="geesefs exited 1"):
        mounter.do_mount("7", "dataset-1", "abc", "jwt", "http://fs")


def test_a_rejected_mount_leaves_no_empty_directory_behind(mounter):
    """A mount the proxy refuses is routine; it should not litter until the next resync."""
    mounter.geesefs_returncode = 1

    with pytest.raises(RuntimeError):
        mounter.do_mount("7", "dataset-1", "abc", "jwt", "http://fs")

    assert not os.path.exists(os.path.join(mounter.MOUNT_ROOT, "7"))


def test_a_rejected_mount_keeps_a_sibling_commit_that_is_mounted(mounter, cu_dir):
    _, live = cu_dir("7", commit="already-here")
    mounter.set_mounts(live)
    mounter.geesefs_returncode = 1

    with pytest.raises(RuntimeError):
        mounter.do_mount("7", "dataset-1", "new-commit", "jwt", "http://fs")

    assert not os.path.exists(os.path.join(mounter.MOUNT_ROOT, "7", "dataset-1", "new-commit"))
    assert os.path.isdir(live)  # the working mount is untouched


def test_do_mount_times_out_if_the_mount_never_appears(mounter, monkeypatch):
    monkeypatch.setattr(mounter, "MOUNT_TIMEOUT_S", 0)
    mounter.geesefs_mounts = False  # exits 0 without the mount ever appearing

    with pytest.raises(RuntimeError, match="did not appear as a mount"):
        mounter.do_mount("7", "dataset-1", "abc", "jwt", "http://fs")


# ─────────────────── clean_cu_dir() ───────────────────

def test_clean_unmounts_then_removes_the_directory(mounter, cu_dir):
    directory, target = cu_dir("7")
    mounter.set_mounts(target)

    assert mounter.clean_cu_dir("7") is True
    assert [cmd for cmd, _ in mounter.runs] == [["umount", "-l", target]]
    assert not os.path.exists(directory)
    assert any("unmounted and removed" in line for line in mounter.logs)


def test_clean_unmounts_nested_mounts_deepest_first(mounter, cu_dir):
    _, target = cu_dir("7")
    parent = os.path.dirname(target)
    mounter.set_mounts(parent, target)

    mounter.clean_cu_dir("7")

    assert [cmd[-1] for cmd, _ in mounter.runs] == [target, parent]


def test_clean_removes_a_directory_that_has_no_mounts(mounter, cu_dir):
    directory, _ = cu_dir("7")

    assert mounter.clean_cu_dir("7") is True
    assert mounter.runs == []
    assert not os.path.exists(directory)


def test_clean_keeps_a_directory_whose_mount_is_still_busy(mounter, cu_dir):
    directory, target = cu_dir("7")
    mounter.set_mounts(target)
    mounter.umount_succeeds = False  # lazy unmount has not completed yet

    assert mounter.clean_cu_dir("7") is False
    # The directory must survive: removing it under a live mount is what the original
    # code did, and it also made the removal fail on every cycle forever.
    assert os.path.isdir(directory)
    assert any("still busy" in line for line in mounter.logs)


def test_clean_is_silent_on_a_retry(mounter, cu_dir):
    cu_dir("7")
    mounter.set_mounts(os.path.join(mounter.MOUNT_ROOT, "7", "dataset-1", "abc123"))
    mounter.umount_succeeds = False

    mounter.clean_cu_dir("7")
    first_pass = len(mounter.logs)
    mounter.clean_cu_dir("7", quiet=True)

    assert len(mounter.logs) == first_pass  # a stuck orphan is reported once, not per cycle


def test_clean_never_touches_the_shared_root(mounter):
    """A cuid of "" would resolve to MOUNT_ROOT, whose unmount would break every CU."""
    mounter.set_mounts(mounter.MOUNT_ROOT)

    assert mounter.clean_cu_dir("") is True
    assert mounter.runs == []
    assert os.path.isdir(mounter.MOUNT_ROOT)


def test_clean_accepts_a_directory_that_is_already_gone(mounter):
    assert mounter.clean_cu_dir("404") is True


# ─────────────────── reconcile() ───────────────────

def test_reconcile_removes_orphans_and_keeps_live_computing_units(mounter, cu_dir):
    orphan, _ = cu_dir("7")
    live, _ = cu_dir("8")

    mounter.reconcile({"8"})

    assert not os.path.exists(orphan)
    assert os.path.isdir(live)


def test_reconcile_retries_a_stuck_orphan_until_it_can_be_removed(mounter, cu_dir):
    directory, target = cu_dir("7")
    mounter.set_mounts(target)
    mounter.umount_succeeds = False

    mounter.reconcile(set())
    assert mounter._pending == {"7"}
    assert os.path.isdir(directory)

    mounter.umount_succeeds = True  # the reference finally goes away
    mounter.reconcile(set())

    assert mounter._pending == set()
    assert not os.path.exists(directory)


def test_reconcile_ignores_stray_files_in_the_mount_root(mounter):
    stray = os.path.join(mounter.MOUNT_ROOT, "notes.txt")
    open(stray, "w").close()

    mounter.reconcile(set())

    assert os.path.exists(stray)


# ─────────────────── watch events ───────────────────

def _event(event_type, pod_name):
    return json.dumps({"type": event_type, "object": {"metadata": {"name": pod_name}}}).encode()


def test_a_deleted_computing_unit_pod_is_cleaned_up_immediately(mounter, cu_dir):
    directory, _ = cu_dir("7")

    mounter._handle_event(_event("DELETED", "computing-unit-7"))

    assert not os.path.exists(directory)


@pytest.mark.parametrize(
    "line",
    [
        _event("ADDED", "computing-unit-7"),
        _event("MODIFIED", "computing-unit-7"),
        _event("DELETED", "texera-file-service-xyz"),
        _event("DELETED", "computing-unit-"),
        b"{ not json\n",
        b"\n",
    ],
    ids=["added", "modified", "other-pod", "no-cuid", "malformed", "blank"],
)
def test_irrelevant_watch_events_leave_every_mount_alone(mounter, cu_dir, line):
    directory, _ = cu_dir("7")

    mounter._handle_event(line)

    assert os.path.isdir(directory)
    assert mounter.runs == []


def test_a_delete_that_cannot_finish_is_retried_on_the_next_resync(mounter, cu_dir):
    directory, target = cu_dir("7")
    mounter.set_mounts(target)
    mounter.umount_succeeds = False

    mounter._handle_event(_event("DELETED", "computing-unit-7"))

    assert os.path.isdir(directory)
    assert mounter._pending == {"7"}


# ─────────────────── _list_cu_pods() ───────────────────

def test_listing_returns_computing_unit_ids_and_the_resource_version(mounter, monkeypatch):
    payload = {
        "metadata": {"resourceVersion": "4242"},
        "items": [
            {"metadata": {"name": "computing-unit-7"}},
            {"metadata": {"name": "computing-unit-8"}},
            {"metadata": {"name": "some-other-pod"}},
        ],
    }
    monkeypatch.setattr(mounter, "_k8s_open", lambda path, timeout: _FakeResponse(payload))

    assert mounter._list_cu_pods() == ({"7", "8"}, "4242")


def test_listing_failure_reports_no_answer_rather_than_an_empty_cluster(mounter, monkeypatch):
    """A failed LIST must not read as "no CU pods exist" — that would unmount everything."""

    def explode(path, timeout):
        raise OSError("connection refused")

    monkeypatch.setattr(mounter, "_k8s_open", explode)

    live, resource_version = mounter._list_cu_pods()

    assert live is None and resource_version is None
    assert any("listing CU pods failed" in line for line in mounter.logs)


# ─────────────────── request validation (path components) ───────────────────
#
# Every component of MOUNT_ROOT/<cuid>/<repo>/<commit> arrives from the request. LakeFS
# only sees a request after geesefs runs, which is after the directory has been created,
# so the mounter cannot delegate path safety to it. These cases pin the escapes down.

@pytest.mark.parametrize(
    "cuid, escapes_to",
    [
        ("5/../8", "8"),        # traverses sideways into another CU's directory
        ("../..", "outside"),   # walks above the mount root entirely
        ("/etc", "absolute"),   # os.path.join drops MOUNT_ROOT for an absolute component
        ("/", "absolute"),
        ("8 ", "whitespace"),
        ("-8", "negative"),
        ("abc", "non-numeric"),
        ("", "empty"),
    ],
)
def test_do_mount_rejects_a_cuid_that_is_not_a_single_numeric_segment(mounter, cuid, escapes_to):
    with pytest.raises(ValueError, match="cuid"):
        mounter.do_mount(cuid, "dataset-1", "abc123", "jwt", "http://fs")

    assert mounter.runs == []  # geesefs never ran
    # Nothing was created anywhere: not under another CU, not outside the mount root.
    assert os.listdir(mounter.MOUNT_ROOT) == [], escapes_to


def test_a_manipulated_cuid_cannot_plant_a_mount_in_another_computing_units_directory(mounter):
    """kunwp1's report: CU 5 asking for cuid='5/../8' must not land in CU 8's tree."""
    victim = os.path.join(mounter.MOUNT_ROOT, "8")
    os.makedirs(victim)

    with pytest.raises(ValueError, match="cuid"):
        mounter.do_mount("5/../8", "dataset-1", "abc123", "jwt", "http://fs")

    assert os.listdir(victim) == []


@pytest.mark.parametrize("cuid", ["5/../8", "../..", "/etc", "abc"])
def test_list_mounts_rejects_a_cuid_that_is_not_a_single_numeric_segment(mounter, cuid, cu_dir):
    _, target = cu_dir("8")
    mounter.set_mounts(target)

    # Enumerating another CU's mounts by traversal is refused rather than answered.
    with pytest.raises(ValueError, match="cuid"):
        mounter.list_mounts(cuid)


@pytest.mark.parametrize(
    "repo",
    ["../../8/dataset-1", "..", "a/b", "/etc", "-o", ".", ""],
)
def test_do_mount_rejects_a_repository_name_that_is_not_a_single_segment(mounter, repo):
    with pytest.raises(ValueError, match="repositoryName|required"):
        mounter.do_mount("7", repo, "abc123", "jwt", "http://fs")

    assert mounter.runs == []
    assert os.listdir(mounter.MOUNT_ROOT) == []


@pytest.mark.parametrize("commit", ["../../..", "..", "a/b", "/etc", "-o", ".", ""])
def test_do_mount_rejects_a_commit_hash_that_is_not_a_single_segment(mounter, commit):
    with pytest.raises(ValueError, match="commitHash|required"):
        mounter.do_mount("7", "dataset-1", commit, "jwt", "http://fs")

    assert mounter.runs == []
    assert os.listdir(mounter.MOUNT_ROOT) == []


def test_legitimate_values_still_mount(mounter):
    """The guard must not reject what the platform actually sends: dataset-<did> + a hex digest."""
    target = mounter.do_mount("42", "dataset-17", "0a1b2c3d4e5f", "jwt", "http://fs")

    assert target == os.path.join(mounter.MOUNT_ROOT, "42", "dataset-17", "0a1b2c3d4e5f")
    assert [cmd[0] for cmd, _ in mounter.runs] == ["geesefs"]


_MOUNT_REQUEST = {
    "cuid": "7",
    "repositoryName": "dataset-1",
    "commitHash": "abc123",
    "jwt": "the-users-jwt",
    "fileServiceBase": "http://file-service:9092",
}


# ─────────────────── caller authentication ───────────────────
# The mounter runs privileged on every node and listens on a hostPort, so anything routable
# to a node IP -- including computing-unit pods running untrusted user code -- can open a
# connection to it. What separates the platform from user code is not reachability but a
# service-account token bound to the mounter's audience, verified by the API server. These
# tests stub only `review_token`, so everything `authenticate_caller` decides is real.

def test_a_request_without_a_token_is_refused(mounter, http_client):
    status, body = http_client.post("/mount", _MOUNT_REQUEST, token=None)

    assert status == 401
    assert "bearer token" in body["error"]
    assert mounter.runs == []


@pytest.mark.parametrize("header", ["", "Bearer ", "Bearer    ", "the-token", "Basic dXNlcg=="])
def test_a_malformed_authorization_header_is_refused(mounter, header):
    with pytest.raises(mounter.Unauthorized):
        mounter.authenticate_caller(header)


def test_a_token_the_api_server_cannot_verify_is_refused(mounter, http_client):
    status, body = http_client.post("/mount", _MOUNT_REQUEST, token="forged")

    assert status == 401
    assert "not valid" in body["error"]
    assert mounter.runs == []


def test_a_token_minted_for_another_audience_is_refused(mounter):
    """The pod's ordinary kube-apiserver token authenticates -- just not to the mounter."""
    mounter.token_reviews["kube-token"] = {
        "authenticated": True,
        "audiences": [],  # the API server echoes back only the audiences the token carries
        "user": {"username": mounter.ALLOWED_CALLER},
    }

    with pytest.raises(mounter.Unauthorized, match="audience"):
        mounter.authenticate_caller("Bearer kube-token")


def test_a_valid_token_belonging_to_another_identity_is_refused(mounter):
    """A CU pod can mint a token for this audience; it still is not an allowed caller."""
    mounter.token_reviews["cu-pod-token"] = {
        "authenticated": True,
        "audiences": [mounter.MOUNTER_AUDIENCE],
        "user": {"username": "system:serviceaccount:texera-workflow-computing-unit-pool:default"},
    }

    with pytest.raises(mounter.Unauthorized, match="not permitted"):
        mounter.authenticate_caller("Bearer cu-pod-token")


def test_an_unreachable_api_server_denies_rather_than_admits(mounter):
    """Failing closed: a mounter that cannot tell who is calling must not mount."""
    mounter.review_token_error = RuntimeError("connection refused")

    with pytest.raises(mounter.Unauthorized, match="token review failed"):
        mounter.authenticate_caller(f"Bearer {mounter.VALID_TOKEN}")


def test_no_configured_callers_denies_everything(mounter):
    """An unset MOUNTER_ALLOWED_CALLERS is not an invitation to serve everyone."""
    mounter.ALLOWED_CALLERS = frozenset()

    with pytest.raises(mounter.Unauthorized, match="no allowed callers"):
        mounter.authenticate_caller(f"Bearer {mounter.VALID_TOKEN}")


def test_the_allowed_caller_is_admitted(mounter):
    assert mounter.authenticate_caller(f"Bearer {mounter.VALID_TOKEN}") == mounter.ALLOWED_CALLER


def test_authentication_does_not_replace_path_validation(mounter, http_client):
    """A properly authenticated caller still cannot name a path outside the CU's directory."""
    status, body = http_client.post("/mount", {**_MOUNT_REQUEST, "cuid": "../.."})

    assert status == 400
    assert "cuid" in body["error"]
    assert mounter.runs == []


def test_the_readiness_probe_stays_unauthenticated(mounter, http_client):
    """The kubelet holds no token for this audience, and /healthz exposes no mount state."""
    assert http_client.get("/healthz", token=None) == (200, {"status": "ok"})


# ─────────────────── HTTP surface ───────────────────

def test_a_manipulated_cuid_is_answered_with_400_over_http(mounter, http_client):
    status, body = http_client.post(
        "/mount",
        {
            "cuid": "../..",
            "repositoryName": "dataset-1",
            "commitHash": "abc123",
            "jwt": "jwt",
            "fileServiceBase": "http://fs",
        },
    )

    assert status == 400
    assert "cuid" in body["error"]
    assert mounter.runs == []


def test_a_manipulated_repository_name_is_answered_with_400_over_http(mounter, http_client):
    status, body = http_client.post(
        "/mount",
        {
            "cuid": "7",
            "repositoryName": "../../8/dataset-1",
            "commitHash": "abc123",
            "jwt": "jwt",
            "fileServiceBase": "http://fs",
        },
    )

    assert status == 400
    assert "repositoryName" in body["error"]
    assert mounter.runs == []


def test_listing_another_computing_unit_by_traversal_is_answered_with_400_over_http(
    mounter, http_client, cu_dir
):
    _, target = cu_dir("8")
    mounter.set_mounts(target)

    assert http_client.get("/mounts?cuid=8")[1] == {
        "mounts": [{"repositoryName": "dataset-1", "commitHash": "abc123", "mountPath": target}]
    }

    status, body = http_client.get("/mounts?cuid=5/../8")

    assert status == 400
    assert "cuid" in body["error"]


def test_a_valid_mount_request_is_answered_with_the_mount_path_over_http(mounter, http_client):
    status, body = http_client.post(
        "/mount",
        {
            "cuid": "7",
            "repositoryName": "dataset-1",
            "commitHash": "abc123",
            "jwt": "the-user-jwt",
            "fileServiceBase": "http://fs",
        },
    )

    assert status == 200
    assert body == {"mountPath": os.path.join(mounter.MOUNT_ROOT, "7", "dataset-1", "abc123")}


# ─────────────────── _remove_empty_dirs() ───────────────────

def test_removing_empty_dirs_never_walks_above_the_computing_units_directory(mounter, cu_dir):
    cu, target = cu_dir("7")

    mounter._remove_empty_dirs(target, "7")

    # The CU directory itself goes (DirectoryOrCreate recreates it), MOUNT_ROOT stays.
    assert not os.path.exists(cu)
    assert os.path.isdir(mounter.MOUNT_ROOT)


def test_removing_empty_dirs_does_not_treat_a_sibling_as_a_child(mounter):
    """"/mounts/7x" merely starts with "/mounts/7"; a prefix test would delete it."""
    sibling = os.path.join(mounter.MOUNT_ROOT, "7x")
    os.makedirs(sibling)

    mounter._remove_empty_dirs(sibling, "7")

    assert os.path.isdir(sibling)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = json.dumps(payload).encode()

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
