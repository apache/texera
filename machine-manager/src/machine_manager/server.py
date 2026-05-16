"""Texera machine-manager service.

Runs on the target host. Exposes a small HTTP API so Texera services can:
- run shell commands         (POST /exec)
- run a Python snippet with an injected tuple   (POST /python)
- write a file under a sandbox directory        (POST /deploy-code)
- upload a local file into a Texera dataset     (POST /upload-to-dataset)

Auth: shared Bearer token from env MACHINE_MANAGER_TOKEN (skipped if unset, for dev).
Port:  env MACHINE_MANAGER_PORT, default 5555.
Sandbox: env MACHINE_MANAGER_SANDBOX_DIR, default ~/.texera/machine-manager/sandbox.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
import textwrap
import traceback
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


# --- config -----------------------------------------------------------------

TOKEN = os.environ.get("MACHINE_MANAGER_TOKEN", "").strip()
SANDBOX_DIR = Path(
    os.environ.get(
        "MACHINE_MANAGER_SANDBOX_DIR",
        str(Path.home() / ".texera" / "machine-manager" / "sandbox"),
    )
).resolve()
SANDBOX_DIR.mkdir(parents=True, exist_ok=True)

# Python interpreter used for /python execution. Defaults to machine-manager's
# own interpreter, which typically has only FastAPI/uvicorn. Override with a
# data-science venv (sklearn, pandas, matplotlib, ...) when the agent needs to
# run real analysis workloads.
PYTHON_INTERPRETER = os.environ.get("MACHINE_MANAGER_PYTHON", sys.executable)


# --- auth -------------------------------------------------------------------

async def require_token(request: Request) -> None:
    if not TOKEN:
        # dev mode: no token configured, allow all
        return
    header = request.headers.get("authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    if header.removeprefix("Bearer ").strip() != TOKEN:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "bad token")


# --- request/response models ------------------------------------------------

class ExecRequest(BaseModel):
    cmd: str = Field(..., description="Command to run. Passed to a shell.")
    cwd: str | None = None
    timeout_seconds: float = 60.0
    env: dict[str, str] | None = None


class ExecResponse(BaseModel):
    exit_code: int
    stdout: str
    stderr: str


class PythonRequest(BaseModel):
    code: str
    # `tuple_in` is whatever JSON the caller wants injected as a Python global.
    # For per-tuple MachineUDF this is a dict; for batch MachineUDF it's a list of
    # dicts; for ad-hoc runs (`runPythonOnMachine`) callers pass null.
    tuple_in: Any = None
    timeout_seconds: float = 60.0


class PythonResponse(BaseModel):
    exit_code: int
    stdout: str
    stderr: str
    result: Any | None = None  # parsed last JSON line of stdout, if any


class DeployRequest(BaseModel):
    relative_path: str = Field(..., description="Path under the sandbox dir.")
    content: str
    overwrite: bool = True


class DeployResponse(BaseModel):
    absolute_path: str
    bytes_written: int


class UploadRequest(BaseModel):
    local_path: str
    dataset_id: int
    file_path: str = Field(..., description="Destination path inside the dataset.")
    file_service_url: str = Field(..., description="Base URL of file-service, e.g. http://localhost:9092")
    auth_token: str = Field(..., description="Texera JWT for the calling user.")


class UploadResponse(BaseModel):
    dataset_id: int
    file_path: str
    bytes_uploaded: int
    version_name: str | None = None
    dataset_name: str | None = None


# --- app --------------------------------------------------------------------

app = FastAPI(
    title="texera-machine-manager",
    version="0.1.0",
    dependencies=[Depends(require_token)],
)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {
        "ok": True,
        "sandbox": str(SANDBOX_DIR),
        "auth_required": bool(TOKEN),
        "python_interpreter": PYTHON_INTERPRETER,
    }


@app.post("/exec", response_model=ExecResponse)
async def run_exec(req: ExecRequest) -> ExecResponse:
    try:
        proc = await asyncio.create_subprocess_shell(
            req.cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=req.cwd or None,
            env={**os.environ, **(req.env or {})},
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=req.timeout_seconds
            )
        except asyncio.TimeoutError:
            proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
            raise HTTPException(status.HTTP_504_GATEWAY_TIMEOUT, "command timed out")
        return ExecResponse(
            exit_code=proc.returncode if proc.returncode is not None else -1,
            stdout=stdout.decode("utf-8", "replace"),
            stderr=stderr.decode("utf-8", "replace"),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e))


@app.middleware("http")
async def log_invalid_python(request: Request, call_next):
    if request.url.path == "/python":
        body = await request.body()
        print("[MM-DBG] /python body:", body[:500])
        async def receive():
            return {"type": "http.request", "body": body}
        request._receive = receive
    return await call_next(request)


@app.post("/python", response_model=PythonResponse)
async def run_python(req: PythonRequest) -> PythonResponse:
    # Inject tuple_in as a global. The user code can `print(json.dumps({...}))`
    # to return a row; we'll parse the last JSON line.
    preamble = textwrap.dedent(
        """
        import json as __mm_json
        import sys as __mm_sys
        tuple_in = __mm_json.loads(__mm_sys.stdin.read() or 'null')
        """
    ).strip()
    full = preamble + "\n" + req.code

    with tempfile.NamedTemporaryFile(
        prefix="mm-", suffix=".py", delete=False, mode="w", encoding="utf-8"
    ) as f:
        f.write(full)
        script_path = f.name

    try:
        proc = await asyncio.create_subprocess_exec(
            PYTHON_INTERPRETER,
            script_path,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(
                    json.dumps(req.tuple_in).encode("utf-8") if req.tuple_in is not None else b"null"
                ),
                timeout=req.timeout_seconds,
            )
        except asyncio.TimeoutError:
            proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
            raise HTTPException(status.HTTP_504_GATEWAY_TIMEOUT, "python timed out")

        out = stdout.decode("utf-8", "replace")
        err = stderr.decode("utf-8", "replace")
        result: Any | None = None
        for line in reversed(out.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                result = json.loads(line)
                break
            except json.JSONDecodeError:
                continue

        return PythonResponse(
            exit_code=proc.returncode if proc.returncode is not None else -1,
            stdout=out,
            stderr=err,
            result=result,
        )
    finally:
        with contextlib.suppress(Exception):
            os.unlink(script_path)


@app.post("/deploy-code", response_model=DeployResponse)
async def deploy_code(req: DeployRequest) -> DeployResponse:
    target = (SANDBOX_DIR / req.relative_path).resolve()
    if SANDBOX_DIR not in target.parents and target != SANDBOX_DIR:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "path escapes sandbox")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not req.overwrite:
        raise HTTPException(status.HTTP_409_CONFLICT, "file exists and overwrite=false")
    data = req.content.encode("utf-8")
    target.write_bytes(data)
    return DeployResponse(absolute_path=str(target), bytes_written=len(data))


@app.post("/upload-to-dataset", response_model=UploadResponse)
async def upload_to_dataset(req: UploadRequest) -> UploadResponse:
    local = Path(os.path.expanduser(req.local_path)).resolve()
    if not local.is_file():
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"local file not found: {local}")

    size = local.stat().st_size
    headers = {
        "Authorization": f"Bearer {req.auth_token}",
        "Content-Type": "application/octet-stream",
        "Content-Length": str(size),
    }

    version_name: str | None = None
    dataset_name: str | None = None
    async with httpx.AsyncClient(timeout=300.0) as client:
        with local.open("rb") as f:
            resp = await client.post(
                f"{req.file_service_url.rstrip('/')}/api/dataset/{req.dataset_id}/upload",
                params={
                    "filePath": req.file_path,
                    "message": f"Uploaded {local.name} via machine-manager",
                },
                content=f.read(),
                headers=headers,
            )
        if resp.status_code >= 300:
            raise HTTPException(resp.status_code, f"file-service upload failed: {resp.text}")

        # The upload only stages the file; commit it as a new dataset version
        # so the workflow can read it via <name>/<version>/<file>. Send an empty
        # message body so file-service names the version cleanly (e.g. "v2"
        # rather than "v2 - <message>") for simpler downstream path handling.
        commit_resp = await client.post(
            f"{req.file_service_url.rstrip('/')}/api/dataset/{req.dataset_id}/version/create",
            content="",
            headers={
                "Authorization": f"Bearer {req.auth_token}",
                "Content-Type": "text/plain",
            },
        )
        # "No changes" => the staged file matches the latest version already.
        # Treat as success and fall back to the latest version metadata.
        no_changes = (
            commit_resp.status_code == 400
            and "No changes detected" in commit_resp.text
        )
        if commit_resp.status_code >= 300 and not no_changes:
            raise HTTPException(
                commit_resp.status_code,
                f"file-service version create failed: {commit_resp.text}",
            )
        version_name = None
        dataset_name = None
        if not no_changes:
            try:
                commit_body = commit_resp.json()
                version_name = commit_body.get("datasetVersion", {}).get("name")
                dataset_name = commit_body.get("dataset", {}).get("name") or commit_body.get(
                    "datasetName"
                )
            except Exception:
                pass
        if version_name is None:
            try:
                latest_resp = await client.get(
                    f"{req.file_service_url.rstrip('/')}/api/dataset/{req.dataset_id}/version/latest",
                    headers={"Authorization": f"Bearer {req.auth_token}"},
                )
                if latest_resp.status_code < 300:
                    latest_body = latest_resp.json()
                    version_name = latest_body.get("datasetVersion", {}).get("name")
            except Exception:
                pass

    return UploadResponse(
        dataset_id=req.dataset_id,
        file_path=req.file_path,
        bytes_uploaded=size,
        version_name=version_name,
        dataset_name=dataset_name,
    )


@app.exception_handler(Exception)
async def all_errors(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"error": str(exc), "trace": traceback.format_exc()},
    )


def main() -> None:
    import uvicorn

    uvicorn.run(
        "machine_manager.server:app",
        host=os.environ.get("MACHINE_MANAGER_HOST", "0.0.0.0"),
        port=int(os.environ.get("MACHINE_MANAGER_PORT", "5555")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
