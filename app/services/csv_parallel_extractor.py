import io
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

from app.core.config import settings
from app.services.llm_client import LLMRequestError, parse_llm_json
from app.services.prompt_templates import CSV_FIELD_SYSTEM_PROMPT, CSV_FIELD_USER_PROMPT

TARGET_FIELDS = [
    "source_url",
    "title",
    "pathogen_type",
    "pathogen",
    "subtype",
    "original_continent",
    "original_country",
    "original_province",
    "spread_continent",
    "spread_country",
    "spread_province",
    "start_date",
    "end_date",
    "host",
    "infection_num",
    "death_num",
    "event_type",
]


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _truncate_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def _resolve_chat_completions_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    return f"{normalized}/chat/completions"


def _build_payload(prompt: str) -> Dict[str, Any]:
    return {
        "model": settings.LLM_MODEL,
        "messages": [
            {"role": "system", "content": CSV_FIELD_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "top_p": settings.LLM_TOP_P,
        "max_tokens": max(512, settings.LLM_MAX_TOKENS),
    }


def _call_model(prompt: str, timeout: float) -> str:
    url = _resolve_chat_completions_url(settings.LLM_BASE_URL)
    headers: Dict[str, str] = {}
    if settings.LLM_API_KEY:
        headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"

    response = requests.post(url=url, headers=headers, json=_build_payload(prompt), timeout=timeout)
    if response.status_code >= 400:
        raise LLMRequestError(f"LLM error {response.status_code}: {response.text}")

    data = response.json()
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise LLMRequestError("Unexpected LLM response structure") from exc


def _normalize_record(raw: Dict[str, Any], source_url: str) -> Dict[str, str]:
    record: Dict[str, str] = {field: "" for field in TARGET_FIELDS}
    record["source_url"] = source_url
    for field in TARGET_FIELDS:
        if field == "source_url":
            continue
        record[field] = _safe_str(raw.get(field, ""))
    return record


def _extract_one(
    row_index: int,
    source_url: str,
    full_text: str,
    timeout: float,
    max_chars: int,
    retries: int,
    retry_wait: float,
) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    prompt = CSV_FIELD_USER_PROMPT.format(
        source_url=source_url,
        content=_truncate_text(full_text, max_chars),
    )
    start = time.perf_counter()
    error_msg = ""

    for attempt in range(1, retries + 2):
        try:
            llm_text = _call_model(prompt, timeout=timeout)
            data = parse_llm_json(llm_text)
            elapsed = time.perf_counter() - start
            return row_index, _normalize_record(data, source_url), {
                "row_index": int(row_index),
                "source_url": source_url,
                "process_seconds": round(elapsed, 4),
                "status": "ok",
                "error": "",
                "attempts": attempt,
                "full_text_chars": len(full_text),
            }
        except Exception as exc:
            error_msg = str(exc)
            if attempt <= retries:
                time.sleep(retry_wait)
                continue

    elapsed = time.perf_counter() - start
    empty_row = {field: "" for field in TARGET_FIELDS}
    empty_row["source_url"] = source_url
    return row_index, empty_row, {
        "row_index": int(row_index),
        "source_url": source_url,
        "process_seconds": round(elapsed, 4),
        "status": "failed",
        "error": error_msg,
        "attempts": retries + 1,
        "full_text_chars": len(full_text),
    }


def extract_csv_parallel_to_excel(
    csv_bytes: bytes,
    *,
    workers: int = 2,
    timeout: float = 120.0,
    max_chars: int = 12000,
    retries: int = 1,
    retry_wait: float = 1.5,
    limit: int = 0,
) -> Tuple[bytes, Dict[str, Any]]:
    df = pd.read_csv(io.BytesIO(csv_bytes), encoding="utf-8")
    if limit > 0:
        df = df.head(limit)

    if "detail_url" not in df.columns or "full_text" not in df.columns:
        raise ValueError("CSV must include columns: detail_url, full_text")

    rows: List[Tuple[int, str, str]] = []
    for idx, row in df.iterrows():
        rows.append((int(idx), _safe_str(row.get("detail_url", "")), _safe_str(row.get("full_text", ""))))

    results_map: Dict[int, Dict[str, str]] = {}
    timings_map: Dict[int, Dict[str, Any]] = {}
    total_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [
            executor.submit(
                _extract_one,
                row_index,
                source_url,
                full_text,
                timeout,
                max_chars,
                max(0, retries),
                max(0.0, retry_wait),
            )
            for row_index, source_url, full_text in rows
        ]
        for future in as_completed(futures):
            row_index, record, timing = future.result()
            results_map[row_index] = record
            timings_map[row_index] = timing

    total_seconds = time.perf_counter() - total_start
    ordered_indices = sorted(results_map.keys())
    result_rows = [results_map[i] for i in ordered_indices]
    timing_rows = [timings_map[i] for i in ordered_indices]

    result_df = pd.DataFrame(result_rows, columns=TARGET_FIELDS)
    timing_df = pd.DataFrame(timing_rows)
    failed = sum(1 for item in timing_rows if item.get("status") != "ok")

    summary_df = pd.DataFrame(
        [
            {
                "model_provider": settings.LLM_PROVIDER,
                "model_name": settings.LLM_MODEL,
                "rows_total": len(result_rows),
                "rows_failed": failed,
                "workers": max(1, workers),
                "timeout_seconds": timeout,
                "max_chars": max_chars,
                "retries": max(0, retries),
                "total_seconds": round(total_seconds, 4),
            }
        ]
    )

    buffer = io.BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            result_df.to_excel(writer, index=False, sheet_name="extracted")
            timing_df.to_excel(writer, index=False, sheet_name="timing")
            summary_df.to_excel(writer, index=False, sheet_name="summary")
    except Exception:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            result_df.to_excel(writer, index=False, sheet_name="extracted")
            timing_df.to_excel(writer, index=False, sheet_name="timing")
            summary_df.to_excel(writer, index=False, sheet_name="summary")

    buffer.seek(0)
    meta = {
        "rows_total": len(result_rows),
        "rows_failed": failed,
        "workers": max(1, workers),
        "model_name": settings.LLM_MODEL,
    }
    return buffer.getvalue(), meta
