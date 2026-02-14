import io
import logging
import os
from urllib.parse import quote
from typing import Optional

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from app.core.config import settings
from app.schemas import ExtractionResult
from app.services.csv_parallel_extractor import extract_csv_parallel_to_excel
from app.services.extractor import extract_metrics
from app.services.file_reader import UnsupportedFileTypeError, read_file_to_text

logger = logging.getLogger(__name__)


def _content_disposition(filename: str) -> str:
    ascii_name = filename.encode("ascii", "ignore").decode("ascii") or "extraction.xlsx"
    quoted_name = quote(filename)
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quoted_name}"

router = APIRouter()


@router.post("/extract")
async def extract(
    file: UploadFile = File(...),
    description: Optional[str] = None,
    output: str = "json",
):
    content = await file.read()
    max_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024
    if len(content) > max_bytes:
        raise HTTPException(status_code=413, detail="File too large")

    try:
        text, file_type = read_file_to_text(file.filename, content)
    except UnsupportedFileTypeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Failed to read file")
        raise HTTPException(status_code=500, detail="Failed to read file") from exc

    if description:
        text = f"文件描述: {description}\n\n{text}"

    try:
        extracted = extract_metrics(text)
    except Exception as exc:
        logger.exception("Extraction failed")
        raise HTTPException(status_code=500, detail="Extraction failed") from exc

    if output.lower() in {"xlsx", "xls"}:
        try:
            records_df = pd.DataFrame(extracted["records"])
            meta_df = pd.DataFrame(
                [
                    {
                        "file_name": file.filename,
                        "file_type": file_type,
                        "model_provider": settings.LLM_PROVIDER,
                        "model_name": settings.LLM_MODEL,
                        "raw_summary": extracted.get("raw_summary") or "",
                    }
                ]
            )

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                records_df.to_excel(writer, index=False, sheet_name="records")
                meta_df.to_excel(writer, index=False, sheet_name="meta")
            buffer.seek(0)

            base_name = os.path.splitext(file.filename)[0] or "extraction"
            filename = f"{base_name}.xlsx"
            headers = {"Content-Disposition": _content_disposition(filename)}
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as exc:
            logger.exception("XLSX export failed")
            raise HTTPException(status_code=500, detail="XLSX export failed") from exc

    if output.lower() not in {"json"}:
        raise HTTPException(status_code=400, detail="Unsupported output format")

    return ExtractionResult(
        file_name=file.filename,
        file_type=file_type,
        records=extracted["records"],
        raw_summary=extracted.get("raw_summary"),
        model_provider=settings.LLM_PROVIDER,
        model_name=settings.LLM_MODEL,
    )


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/extract-csv-parallel")
async def extract_csv_parallel(
    file: UploadFile = File(...),
    workers: int = 2,
    timeout: float = 120.0,
    max_chars: int = 12000,
    retries: int = 1,
    retry_wait: float = 1.5,
    limit: int = 0,
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV file is supported")

    content = await file.read()
    max_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024
    if len(content) > max_bytes:
        raise HTTPException(status_code=413, detail="File too large")

    try:
        excel_bytes, meta = extract_csv_parallel_to_excel(
            content,
            workers=max(1, workers),
            timeout=max(30.0, timeout),
            max_chars=max(1000, max_chars),
            retries=max(0, retries),
            retry_wait=max(0.0, retry_wait),
            limit=max(0, limit),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Parallel CSV extraction failed")
        raise HTTPException(status_code=500, detail="Parallel CSV extraction failed") from exc

    base_name = os.path.splitext(file.filename)[0] or "who_docs_emergencies_detail"
    filename = f"{base_name}_extracted.xlsx"
    headers = {
        "Content-Disposition": _content_disposition(filename),
        "X-Rows-Total": str(meta.get("rows_total", 0)),
        "X-Rows-Failed": str(meta.get("rows_failed", 0)),
        "X-Workers": str(meta.get("workers", workers)),
        "X-Model-Name": str(meta.get("model_name", settings.LLM_MODEL)),
    }
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
