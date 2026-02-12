import argparse
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from app.core.config import settings
from app.services.llm_client import LLMRequestError, parse_llm_json
from app.services.prompt_templates import CSV_FIELD_SYSTEM_PROMPT, CSV_FIELD_USER_PROMPT

import requests


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


def _call_qwen(prompt: str, timeout: float) -> str:
    if not settings.LLM_API_KEY:
        raise LLMRequestError("LLM_API_KEY is not set")

    url = f"{settings.LLM_BASE_URL.rstrip('/')}/chat/completions"
    headers = {"Authorization": f"Bearer {settings.LLM_API_KEY}"}
    payload = _build_payload(prompt)

    response = requests.post(url=url, headers=headers, json=payload, timeout=timeout)
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


def extract_one(source_url: str, full_text: str, timeout: float, max_chars: int) -> Tuple[Dict[str, str], float, str]:
    prompt = CSV_FIELD_USER_PROMPT.format(
        source_url=source_url,
        content=_truncate_text(full_text, max_chars),
    )
    start = time.perf_counter()
    llm_text = _call_qwen(prompt, timeout=timeout)
    elapsed = time.perf_counter() - start
    data = parse_llm_json(llm_text)
    return _normalize_record(data, source_url), elapsed, llm_text


def run_batch(
    input_csv: Path,
    output_excel: Path,
    output_timing_csv: Path,
    timeout: float,
    max_chars: int,
    limit: int,
) -> None:
    df = pd.read_csv(input_csv, encoding="utf-8")
    if limit > 0:
        df = df.head(limit)

    if "detail_url" not in df.columns or "full_text" not in df.columns:
        raise ValueError("CSV must include columns: detail_url, full_text")

    result_rows: List[Dict[str, str]] = []
    timing_rows: List[Dict[str, Any]] = []
    failures = 0

    total_start = time.perf_counter()
    for idx, row in df.iterrows():
        source_url = _safe_str(row.get("detail_url", ""))
        full_text = _safe_str(row.get("full_text", ""))
        rec_start = time.perf_counter()
        error_msg = ""
        try:
            record, llm_elapsed, _ = extract_one(
                source_url=source_url,
                full_text=full_text,
                timeout=timeout,
                max_chars=max_chars,
            )
            result_rows.append(record)
            process_seconds = llm_elapsed
        except Exception as exc:
            failures += 1
            process_seconds = time.perf_counter() - rec_start
            error_msg = str(exc)
            empty_row = {field: "" for field in TARGET_FIELDS}
            empty_row["source_url"] = source_url
            result_rows.append(empty_row)

        timing_rows.append(
            {
                "row_index": int(idx),
                "source_url": source_url,
                "process_seconds": round(process_seconds, 4),
                "status": "ok" if not error_msg else "failed",
                "error": error_msg,
                "full_text_chars": len(full_text),
            }
        )

    total_seconds = time.perf_counter() - total_start

    result_df = pd.DataFrame(result_rows, columns=TARGET_FIELDS)
    timing_df = pd.DataFrame(timing_rows)

    if not timing_df.empty:
        p50 = float(timing_df["process_seconds"].quantile(0.5))
        p90 = float(timing_df["process_seconds"].quantile(0.9))
        p95 = float(timing_df["process_seconds"].quantile(0.95))
        avg = float(timing_df["process_seconds"].mean())
    else:
        p50 = p90 = p95 = avg = 0.0

    summary_df = pd.DataFrame(
        [
            {
                "model_name": settings.LLM_MODEL,
                "rows_total": len(df),
                "rows_failed": failures,
                "total_seconds": round(total_seconds, 4),
                "avg_seconds_per_row": round(avg, 4),
                "p50_seconds": round(p50, 4),
                "p90_seconds": round(p90, 4),
                "p95_seconds": round(p95, 4),
                "recommended_chars_per_text": max_chars,
                "recommended_batch_size_sync": 1,
                "notes": "Large models are usually most stable with per-row sync calls; use async queue for throughput.",
            }
        ]
    )

    with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
        result_df.to_excel(writer, index=False, sheet_name="extracted")
        timing_df.to_excel(writer, index=False, sheet_name="timing")
        summary_df.to_excel(writer, index=False, sheet_name="summary")

    timing_df.to_csv(output_timing_csv, index=False, encoding="utf-8-sig")

    print(f"Done. rows={len(df)}, failed={failures}, total_seconds={total_seconds:.2f}")
    print(f"Output excel: {output_excel}")
    print(f"Timing csv : {output_timing_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch extract outbreak fields from CSV by qwen3:235b")
    parser.add_argument(
        "--input-csv",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\don_text_extracted.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--output-excel",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\qwen_extracted_result.xlsx",
        help="Output Excel path",
    )
    parser.add_argument(
        "--output-timing-csv",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\qwen_extracted_timing.csv",
        help="Output timing csv path",
    )
    parser.add_argument("--timeout", type=float, default=max(120.0, settings.LLM_TIMEOUT))
    parser.add_argument("--max-chars", type=int, default=12000, help="Max chars from full_text per row")
    parser.add_argument("--limit", type=int, default=0, help="Process first N rows only, 0 means all rows")
    args = parser.parse_args()

    run_batch(
        input_csv=Path(args.input_csv),
        output_excel=Path(args.output_excel),
        output_timing_csv=Path(args.output_timing_csv),
        timeout=args.timeout,
        max_chars=args.max_chars,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
