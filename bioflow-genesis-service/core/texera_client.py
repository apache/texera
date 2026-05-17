from __future__ import annotations

import base64
import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class TexeraAuthError(Exception):
    pass


class TexeraClientError(Exception):
    pass


def decode_jwt_username(jwt: str) -> str:
    """Best-effort username extraction from a JWT.

    Does NOT verify the signature; Texera verifies the token itself when the
    dashboard / file service receives it. We only need the claim to construct
    the dataset path. Raises TexeraAuthError if no recognizable claim exists.
    """
    try:
        parts = jwt.split(".")
        if len(parts) < 2:
            raise ValueError("not a JWT")
        payload_b64 = parts[1]
        # base64url padding
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        payload = json.loads(decoded)
    except Exception as e:
        raise TexeraAuthError(f"invalid jwt_token: {e}") from e

    for key in ("userName", "username", "email", "name", "sub"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    raise TexeraAuthError("invalid jwt_token: no username claim")


class TexeraClient:
    def __init__(
        self,
        dashboard_base: str = "http://localhost:8080",
        file_base: str = "http://localhost:9092",
        timeout: float = 30.0,
    ):
        self.dashboard_base = dashboard_base.rstrip("/")
        self.file_base = file_base.rstrip("/")
        self.timeout = timeout

    def _dataset_entries_from_list_body(self, body: Any) -> list[dict[str, Any]]:
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("datasets", "data", "items", "results"):
                v = body.get(key)
                if isinstance(v, list):
                    return v
        return []

    def _find_dataset_id_by_name(
        self, entries: list[dict[str, Any]], dataset_name: str
    ) -> int | None:
        # The file-service /api/dataset/list endpoint returns a List[DashboardDataset]
        # where each entry has shape:
        #   { "dataset": { "did": <int>, "name": <str>, ... },
        #     "ownerEmail": ..., "accessPrivilege": ..., "isOwner": ..., "size": ... }
        # so the dataset name lives under `entry.dataset.name`, not at top level.
        for item in entries:
            if not isinstance(item, dict):
                continue
            inner = item.get("dataset") if isinstance(item.get("dataset"), dict) else None
            name = (
                (inner.get("name") if inner else None)
                or item.get("name")
                or item.get("datasetName")
                or item.get("dataset_name")
            )
            if name != dataset_name:
                continue
            did = (inner.get("did") if inner else None) or item.get("did")
            if did is not None:
                return int(did)
        return None

    @staticmethod
    def _extract_version_name(payload: Any) -> str | None:
        """Pull a version-name string out of either a DashboardDatasetVersion
        envelope ({datasetVersion: {...}, fileNodes: [...]}) or a bare
        DatasetVersion object."""
        if not isinstance(payload, dict):
            return None
        inner = payload.get("datasetVersion") or payload.get("dataset_version") or payload
        if not isinstance(inner, dict):
            return None
        for key in ("versionName", "name", "version"):
            val = inner.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip().lstrip("/")
        return None

    def _fetch_latest_version_segment(
        self, client: httpx.Client, headers: dict[str, str], dataset_id: int
    ) -> str | None:
        resp = client.get(
            f"{self.file_base}/api/dataset/{dataset_id}/version/latest",
            headers=headers,
        )
        logger.info(
            "step 5 fallback: version/latest -> %s body=%s",
            resp.status_code,
            resp.text[:300],
        )
        if resp.status_code >= 400:
            return None
        try:
            return self._extract_version_name(resp.json())
        except Exception:
            logger.warning("version/latest body was not JSON", exc_info=True)
            return None

    def _resolve_version_segment(
        self, client: httpx.Client, headers: dict[str, str], dataset_id: int
    ) -> str:
        """POST version/create; if Texera replies 400 "No changes detected"
        because the uploaded content matches an existing version, fall back to
        GET version/latest and reuse that version. Returns the path-segment
        (e.g. "v1") to use in the dataset file path. Raises TexeraClientError
        on other failures.
        """
        create_resp = client.post(
            f"{self.file_base}/api/dataset/{dataset_id}/version/create",
            headers={**headers, "Content-Type": "text/plain"},
            content="",
        )
        logger.info(
            "step 5: version/create -> %s body=%s",
            create_resp.status_code,
            create_resp.text[:300],
        )

        if create_resp.status_code < 400:
            try:
                seg = self._extract_version_name(create_resp.json())
                if seg:
                    return seg
            except Exception:
                logger.warning("version/create body was not JSON", exc_info=True)
            # Successful create but unparseable name — fall through to /latest.
            seg = self._fetch_latest_version_segment(client, headers, dataset_id)
            return seg or "v1"

        if (
            create_resp.status_code == 400
            and "no changes detected" in create_resp.text.lower()
        ):
            logger.info("step 5: no changes, reusing latest version")
            seg = self._fetch_latest_version_segment(client, headers, dataset_id)
            if seg:
                return seg
            raise TexeraClientError(
                "version/create reported no changes, but version/latest "
                f"returned no usable version for did={dataset_id}"
            )

        raise TexeraClientError(
            f"dataset version/create failed ({create_resp.status_code}): {create_resp.text}"
        )

    def _lookup_dataset_id(
        self, client: httpx.Client, headers: dict[str, str], dataset_name: str
    ) -> int | None:
        list_resp = client.get(
            f"{self.file_base}/api/dataset/list",
            headers=headers,
        )
        if list_resp.status_code in (401, 403):
            raise TexeraAuthError("invalid jwt_token")
        if list_resp.status_code >= 400:
            logger.warning(
                "dataset/list failed (%s): %s",
                list_resp.status_code,
                list_resp.text[:500],
            )
            return None
        try:
            body = list_resp.json()
        except Exception:
            return None
        return self._find_dataset_id_by_name(
            self._dataset_entries_from_list_body(body), dataset_name
        )

    # ---- dataset upload ----

    def upload_csv_as_dataset(
        self, jwt: str, file_content: bytes, dataset_name: str
    ) -> dict[str, Any]:
        """Create a dataset, upload a single CSV as one part, then cut v1.

        Returns:
            { "dataset_id": int, "file_path": str, "owner_email": str }
        """
        owner_email = decode_jwt_username(jwt)
        headers = {"Authorization": f"Bearer {jwt}"}
        file_name = f"{dataset_name}.csv"
        file_size = len(file_content)
        logger.info(
            "upload start: owner=%s dataset=%s file=%s size=%d",
            owner_email, dataset_name, file_name, file_size,
        )

        with httpx.Client(timeout=self.timeout) as client:
            # 1. create dataset (or reuse when name already exists for this user)
            dataset_id = self._lookup_dataset_id(client, headers, dataset_name)
            if dataset_id is not None:
                logger.info(
                    "step 1: dataset already exists, reusing did=%s", dataset_id
                )
            else:
                logger.info("step 1: creating dataset %r", dataset_name)
                create_resp = client.post(
                    f"{self.file_base}/api/dataset/create",
                    headers={**headers, "Content-Type": "application/json"},
                    json={
                        "datasetName": dataset_name,
                        "datasetDescription": "Created by BioFlow Genesis",
                        "isDatasetPublic": True,
                        "isDatasetDownloadable": True,
                    },
                )
                logger.info(
                    "step 1: dataset/create -> %s body=%s",
                    create_resp.status_code,
                    create_resp.text[:300],
                )
                if create_resp.status_code in (401, 403):
                    raise TexeraAuthError("invalid jwt_token")
                if create_resp.status_code >= 400:
                    if create_resp.status_code == 400 and (
                        "already exists" in create_resp.text.lower()
                    ):
                        logger.info(
                            "step 1: 'already exists' from create, re-looking-up did"
                        )
                        dataset_id = self._lookup_dataset_id(
                            client, headers, dataset_name
                        )
                    if dataset_id is None:
                        raise TexeraClientError(
                            "dataset/create failed "
                            f"({create_resp.status_code}): {create_resp.text}"
                        )
                else:
                    create_body = create_resp.json()
                    dataset_id = create_body.get("did") or create_body.get(
                        "dataset", {}
                    ).get("did")
                    if dataset_id is None:
                        raise TexeraClientError(
                            f"dataset/create returned no did: {create_body}"
                        )
            logger.info("step 1: dataset_id=%s", dataset_id)

            # 2. init multipart upload. Pass restart=true so any stale session
            # for the same (owner, dataset, filePath) is reset cleanly — common
            # when the user drops the same CSV twice during a demo.
            init_params = {
                "type": "init",
                "ownerEmail": owner_email,
                "datasetName": dataset_name,
                "filePath": file_name,
                "fileSizeBytes": file_size,
                "partSizeBytes": file_size,
                "restart": "true",
            }
            init_resp = client.post(
                f"{self.file_base}/api/dataset/multipart-upload",
                headers={**headers, "Content-Type": "application/json"},
                params=init_params,
            )
            logger.info(
                "step 2: multipart-upload init -> %s body=%s",
                init_resp.status_code,
                init_resp.text[:300],
            )
            if init_resp.status_code >= 400 or "missingParts" not in init_resp.text:
                raise TexeraClientError(
                    f"multipart-upload init failed ({init_resp.status_code}): {init_resp.text}"
                )

            # 3. upload single part
            part_resp = client.post(
                f"{self.file_base}/api/dataset/multipart-upload/part",
                headers={
                    **headers,
                    "Content-Type": "application/octet-stream",
                },
                params={
                    "ownerEmail": owner_email,
                    "datasetName": dataset_name,
                    "filePath": file_name,
                    "partNumber": 1,
                },
                content=file_content,
            )
            logger.info(
                "step 3: multipart-upload part -> %s body=%s",
                part_resp.status_code,
                part_resp.text[:300],
            )
            if part_resp.status_code != 200:
                # try to abort to keep things clean
                try:
                    client.post(
                        f"{self.file_base}/api/dataset/multipart-upload",
                        headers={**headers, "Content-Type": "application/json"},
                        params={
                            "type": "abort",
                            "ownerEmail": owner_email,
                            "datasetName": dataset_name,
                            "filePath": file_name,
                        },
                    )
                except Exception:
                    logger.warning("abort upload failed", exc_info=True)
                raise TexeraClientError(
                    f"multipart-upload part failed ({part_resp.status_code}): {part_resp.text}"
                )

            # 4. finish
            finish_resp = client.post(
                f"{self.file_base}/api/dataset/multipart-upload",
                headers={**headers, "Content-Type": "application/json"},
                params={
                    "type": "finish",
                    "ownerEmail": owner_email,
                    "datasetName": dataset_name,
                    "filePath": file_name,
                },
            )
            logger.info(
                "step 4: multipart-upload finish -> %s body=%s",
                finish_resp.status_code,
                finish_resp.text[:300],
            )
            if finish_resp.status_code >= 400:
                raise TexeraClientError(
                    f"multipart-upload finish failed ({finish_resp.status_code}): {finish_resp.text}"
                )

            # 5. create version, or — if Texera says "No changes detected" because
            # this content is byte-identical to an existing version — fall back to
            # the dataset's current latest version. Re-uploading the same CSV is
            # explicitly part of the demo flow, so this is not a fatal error.
            version_segment = self._resolve_version_segment(
                client, headers, int(dataset_id)
            )

        file_path = f"/{owner_email}/{dataset_name}/{version_segment}/{file_name}"
        return {
            "dataset_id": int(dataset_id),
            "file_path": file_path,
            "owner_email": owner_email,
        }

    # ---- workflow create (kept for completeness; frontend may call this itself) ----

    def create_workflow(self, jwt: str, name: str, content_json_str: str) -> int:
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(
                f"{self.dashboard_base}/api/workflow/create",
                headers={
                    "Authorization": f"Bearer {jwt}",
                    "Content-Type": "application/json",
                },
                json={"name": name, "content": content_json_str},
            )
            if resp.status_code in (401, 403):
                raise TexeraAuthError("invalid jwt_token")
            if resp.status_code >= 400:
                raise TexeraClientError(
                    f"workflow/create failed ({resp.status_code}): {resp.text}"
                )
            body = resp.json()
            wid = body.get("wid") or body.get("workflow", {}).get("wid")
            if wid is None:
                raise TexeraClientError(f"workflow/create returned no wid: {body}")
            return int(wid)

    def create_workflow_from_dict(self, jwt: str, name: str, content: dict[str, Any]) -> int:
        """Create workflow from a content dict (operators, links, …)."""
        return self.create_workflow(jwt, name, json.dumps(content))

    def persist_workflow(
        self,
        jwt: str,
        wid: int,
        name: str,
        content: dict[str, Any] | str,
        *,
        description: str = "",
        is_public: int = 0,
    ) -> None:
        """POST /api/workflow/persist — same shape as the Angular WorkflowPersistService."""
        content_str = json.dumps(content) if isinstance(content, dict) else content
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(
                f"{self.dashboard_base}/api/workflow/persist",
                headers={
                    "Authorization": f"Bearer {jwt}",
                    "Content-Type": "application/json",
                },
                json={
                    "wid": wid,
                    "name": name,
                    "description": description,
                    "content": content_str,
                    "isPublic": is_public,
                },
            )
            if resp.status_code in (401, 403):
                raise TexeraAuthError("invalid jwt_token")
            if resp.status_code >= 400:
                raise TexeraClientError(
                    f"workflow/persist failed ({resp.status_code}): {resp.text}"
                )
