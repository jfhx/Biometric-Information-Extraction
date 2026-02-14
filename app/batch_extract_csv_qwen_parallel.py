import argparse
import csv
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


def _resolve_chat_completions_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    return f"{normalized}/chat/completions"


def _call_qwen(prompt: str, timeout: float) -> str:
    url = _resolve_chat_completions_url(settings.LLM_BASE_URL)
    headers: Dict[str, str] = {}
    if settings.LLM_API_KEY:
        headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"
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


def _extract_one_with_retry(
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
            llm_text = _call_qwen(prompt, timeout=timeout)
            data = parse_llm_json(llm_text)
            elapsed = time.perf_counter() - start
            record = _normalize_record(data, source_url)
            timing = {
                "row_index": int(row_index),
                "source_url": source_url,
                "process_seconds": round(elapsed, 4),
                "status": "ok",
                "error": "",
                "attempts": attempt,
                "full_text_chars": len(full_text),
            }
            return row_index, record, timing
        except Exception as exc:
            error_msg = str(exc)
            if attempt <= retries:
                time.sleep(retry_wait)
                continue

    elapsed = time.perf_counter() - start
    empty_row = {field: "" for field in TARGET_FIELDS}
    empty_row["source_url"] = source_url
    timing = {
        "row_index": int(row_index),
        "source_url": source_url,
        "process_seconds": round(elapsed, 4),
        "status": "failed",
        "error": error_msg,
        "attempts": retries + 1,
        "full_text_chars": len(full_text),
    }
    return row_index, empty_row, timing


def run_batch_parallel(
    input_csv: Path,
    output_excel: Path,
    output_timing_csv: Path,
    output_json: Optional[Path],
    timeout: float,
    max_chars: int,
    limit: int,
    workers: int,
    retries: int,
    retry_wait: float,
    progress_every: int,
    progress_file: Optional[Path],
) -> None:
    df = pd.read_csv(input_csv, encoding="utf-8")
    if limit > 0:
        df = df.head(limit)

    if "detail_url" not in df.columns or "full_text" not in df.columns:
        raise ValueError("CSV must include columns: detail_url, full_text")

    rows: List[Tuple[int, str, str]] = []
    for idx, row in df.iterrows():
        rows.append((int(idx), _safe_str(row.get("detail_url", "")), _safe_str(row.get("full_text", ""))))

    total_start = time.perf_counter()
    results_map: Dict[int, Dict[str, str]] = {}
    timings_map: Dict[int, Dict[str, Any]] = {}
    completed_count = 0
    failed_count = 0
    sum_row_seconds = 0.0
    total_rows = len(rows)

    if progress_file:
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        with progress_file.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "timestamp",
                    "completed",
                    "total",
                    "failed",
                    "avg_row_seconds",
                    "elapsed_seconds",
                    "eta_seconds",
                    "workers",
                ]
            )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _extract_one_with_retry,
                row_index,
                source_url,
                full_text,
                timeout,
                max_chars,
                retries,
                retry_wait,
            )
            for row_index, source_url, full_text in rows
        ]
        for future in as_completed(futures):
            row_index, record, timing = future.result()
            results_map[row_index] = record
            timings_map[row_index] = timing
            completed_count += 1
            sum_row_seconds += float(timing.get("process_seconds", 0.0))
            if timing.get("status") != "ok":
                failed_count += 1

            should_log_progress = (
                completed_count % progress_every == 0 or completed_count == total_rows
            )
            if should_log_progress:
                elapsed_seconds = time.perf_counter() - total_start
                avg_row_seconds = sum_row_seconds / completed_count if completed_count else 0.0
                speed = completed_count / elapsed_seconds if elapsed_seconds > 0 else 0.0
                eta_seconds = (total_rows - completed_count) / speed if speed > 0 else 0.0
                now_text = time.strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"[{now_text}] progress: {completed_count}/{total_rows}, "
                    f"failed={failed_count}, avg_row={avg_row_seconds:.2f}s, "
                    f"elapsed={elapsed_seconds:.1f}s, eta={eta_seconds:.1f}s"
                )
                if progress_file:
                    with progress_file.open("a", newline="", encoding="utf-8-sig") as f:
                        writer = csv.writer(f)
                        writer.writerow(
                            [
                                now_text,
                                completed_count,
                                total_rows,
                                failed_count,
                                round(avg_row_seconds, 4),
                                round(elapsed_seconds, 4),
                                round(eta_seconds, 4),
                                workers,
                            ]
                        )

    total_seconds = time.perf_counter() - total_start

    ordered_indices = sorted(results_map.keys())
    result_rows = [results_map[i] for i in ordered_indices]
    timing_rows = [timings_map[i] for i in ordered_indices]
    failures = sum(1 for item in timing_rows if item["status"] != "ok")

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
                "workers": workers,
                "retries": retries,
                "total_seconds": round(total_seconds, 4),
                "avg_seconds_per_row": round(avg, 4),
                "p50_seconds": round(p50, 4),
                "p90_seconds": round(p90, 4),
                "p95_seconds": round(p95, 4),
                "throughput_rows_per_min": round((len(df) / total_seconds) * 60, 2) if total_seconds else 0.0,
                "recommended_workers": "2-4 for qwen3:235b (start from 2)",
                "recommended_chars_per_text": max_chars,
            }
        ]
    )

    output_excel.parent.mkdir(parents=True, exist_ok=True)
    output_timing_csv.parent.mkdir(parents=True, exist_ok=True)
    excel_written = False
    excel_error = ""
    for engine in ("xlsxwriter", "openpyxl"):
        try:
            with pd.ExcelWriter(output_excel, engine=engine) as writer:
                result_df.to_excel(writer, index=False, sheet_name="extracted")
                timing_df.to_excel(writer, index=False, sheet_name="timing")
                summary_df.to_excel(writer, index=False, sheet_name="summary")
            excel_written = True
            break
        except Exception as exc:
            excel_error = str(exc)

    timing_df.to_csv(output_timing_csv, index=False, encoding="utf-8-sig")

    final_output_json = output_json or output_excel.with_suffix(".json")
    if not excel_written:
        final_output_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "extracted": result_df.to_dict(orient="records"),
            "timing": timing_df.to_dict(orient="records"),
            "summary": summary_df.to_dict(orient="records"),
        }
        with final_output_json.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    print(
        f"Done. rows={len(df)}, failed={failures}, workers={workers}, "
        f"total_seconds={total_seconds:.2f}"
    )
    if excel_written:
        print(f"Output excel: {output_excel}")
    else:
        print(f"Excel write skipped: {excel_error}")
        print(f"Output json : {final_output_json}")
    print(f"Timing csv : {output_timing_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Parallel batch extraction from CSV by qwen3:235b")
    parser.add_argument(
        "--input-csv",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\don_text_extracted.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--output-excel",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\extracted_result_parallel.xlsx",
        help="Output Excel path",
    )
    parser.add_argument(
        "--output-timing-csv",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\extracted_timing_parallel.csv",
        help="Output timing CSV path",
    )
    parser.add_argument(
        "--output-json",
        default=r"",
        help="Optional JSON output path (auto used when Excel engine is unavailable)",
    )
    parser.add_argument("--timeout", type=float, default=max(120.0, settings.LLM_TIMEOUT))    #如果报 429/超时，降低并发到 1-2，并适当增大 --timeout（如 180）。
    parser.add_argument("--max-chars", type=int, default=12000, help="Max chars from full_text per row")
    parser.add_argument("--limit", type=int, default=0, help="Process first N rows only, 0 means all rows")
    parser.add_argument("--workers", type=int, default=2, help="Thread workers (recommended 2-4)")
    parser.add_argument("--retries", type=int, default=1, help="Retry times per row when request fails")
    parser.add_argument("--retry-wait", type=float, default=1.5, help="Seconds to wait before retry")
    parser.add_argument("--progress-every", type=int, default=100, help="Print progress every N completed rows")
    parser.add_argument(
        "--progress-file",
        default=r"",
        help="Optional progress CSV path for real-time checkpoint writing",
    )
    args = parser.parse_args()

    progress_file_path = Path(args.progress_file) if args.progress_file.strip() else None
    output_json_path = Path(args.output_json) if args.output_json.strip() else None

    run_batch_parallel(
        input_csv=Path(args.input_csv),
        output_excel=Path(args.output_excel),
        output_timing_csv=Path(args.output_timing_csv),
        output_json=output_json_path,
        timeout=args.timeout,
        max_chars=args.max_chars,
        limit=args.limit,
        workers=max(1, args.workers),
        retries=max(0, args.retries),
        retry_wait=max(0.0, args.retry_wait),
        progress_every=max(1, args.progress_every),
        progress_file=progress_file_path,
    )


if __name__ == "__main__":
    main()
