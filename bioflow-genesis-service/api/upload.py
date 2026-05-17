from __future__ import annotations

import csv
import io
import logging
from pathlib import PurePosixPath
from typing import Any

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import JSONResponse

from core.texera_client import TexeraAuthError, TexeraClient, TexeraClientError
from core import upload_cache

router = APIRouter()
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = (".csv", ".tsv")
MAX_SAMPLE_ROWS = 5


def _is_csv_like(filename: str | None, content_type: str | None) -> bool:
    if filename and filename.lower().endswith(ALLOWED_EXTENSIONS):
        return True
    if content_type in {"text/csv", "text/tab-separated-values", "application/csv"}:
        return True
    return False


def _parse_csv(
    content: bytes, filename: str | None
) -> tuple[list[str], list[list[Any]], int]:
    text = content.decode("utf-8-sig", errors="replace")
    delimiter = "\t" if filename and filename.lower().endswith(".tsv") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    columns: list[str] = []
    sample_rows: list[list[Any]] = []
    row_count = 0
    for i, row in enumerate(reader):
        if i == 0:
            columns = [c.strip() for c in row]
            continue
        row_count += 1
        if len(sample_rows) < MAX_SAMPLE_ROWS:
            sample_rows.append(_coerce_row(row))
    return columns, sample_rows, row_count


def _coerce_row(row: list[str]) -> list[Any]:
    out: list[Any] = []
    for cell in row:
        s = cell.strip()
        if s == "":
            out.append("")
            continue
        try:
            out.append(int(s))
            continue
        except ValueError:
            pass
        try:
            out.append(float(s))
            continue
        except ValueError:
            pass
        out.append(s)
    return out


def _sanitize_dataset_name(filename: str | None) -> str:
    if not filename:
        return "dataset"
    stem = PurePosixPath(filename).stem
    safe = "".join(ch if (ch.isalnum() or ch in "-_") else "_" for ch in stem).strip("_")
    return safe or "dataset"


@router.post("/upload")
async def upload(
    file: UploadFile = File(...),
    jwt_token: str = Form(...),
):
    if not _is_csv_like(file.filename, file.content_type):
        return JSONResponse(
            status_code=400, content={"error": "only CSV/TSV files are supported"}
        )
    content = await file.read()
    if not content:
        return JSONResponse(status_code=400, content={"error": "empty file"})

    try:
        columns, sample_rows, row_count = _parse_csv(content, file.filename)
    except Exception as e:
        logger.exception("CSV parse failed")
        return JSONResponse(status_code=400, content={"error": f"CSV parse failed: {e}"})

    if not columns:
        return JSONResponse(
            status_code=400, content={"error": "CSV has no header row"}
        )

    dataset_name = _sanitize_dataset_name(file.filename)
    logger.info(
        "upload received: filename=%r dataset_name=%s bytes=%d columns=%d",
        file.filename, dataset_name, len(content), len(columns),
    )

    client = TexeraClient()
    try:
        upload_info = client.upload_csv_as_dataset(jwt_token, content, dataset_name)
    except TexeraAuthError as e:
        logger.warning("texera auth error: %s", e)
        return JSONResponse(status_code=401, content={"error": str(e)})
    except TexeraClientError as e:
        logger.exception("texera upload failed")
        return JSONResponse(status_code=502, content={"error": str(e)})

    logger.info(
        "upload ok: did=%s file_path=%s",
        upload_info["dataset_id"], upload_info["file_path"],
    )

    upload_id = upload_cache.create_upload_id()
    upload_cache.put(
        upload_id,
        {
            "file_path": upload_info["file_path"],
            "dataset_id": upload_info["dataset_id"],
            "dataset_name": dataset_name,
            "columns": columns,
            "sample_rows": sample_rows,
            "row_count": row_count,
        },
    )

    return {
        "upload_id": upload_id,
        "dataset_id": upload_info["dataset_id"],
        "dataset_name": dataset_name,
        "file_path": upload_info["file_path"],
        "columns": columns,
        "sample_rows": sample_rows,
        "row_count": row_count,
    }
