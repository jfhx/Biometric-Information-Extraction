import argparse  # 标准库：命令行参数解析
import csv
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests

from app.core.config import settings
from app.services.llm_client import LLMRequestError, parse_llm_json
from app.services.prompt_templates import CSV_FIELD_SYSTEM_PROMPT, CSV_FIELD_USER_PROMPT
from app.utils.standardize import (
    CountryProvinceStandardizer,
    HostStandardizer,
    PathogenStandardizer,
    enrich_record,
    save_all_unmatched,
)


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

BACKFILL_LOG_FIELDS = [
    "category",
    "raw_input",
    "resolved_value",
    "status",
    "usage_count",
    "applied_count",
    "error",
    "llm_output",
]

BACKFILL_SYSTEM_PROMPT = (
    "You normalize noisy epidemiology entities into canonical values. "
    "Always return strict JSON only."
)

OUTPUT_FIELDS = [
    "source_url",
    "title",
    "pathogen_type",
    "pathogen",
    "pathogen_rank_1",
    "pathogen_rank_2",
    "subtype",
    "original_continent",
    "original_country",
    "original_province",
    "spread_continent",
    "spread_country",
    "spread_province",
    "start_date",
    "start_date_year",
    "start_date_month",
    "start_date_day",
    "end_date",
    "end_date_year",
    "end_date_month",
    "end_date_day",
    "host",
    "host_rank_1",
    "host_rank_2",
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


def _build_payload(
    prompt: str,
    system_prompt: str = CSV_FIELD_SYSTEM_PROMPT,
) -> Dict[str, Any]:
    return {
        "model": settings.LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
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


def _call_qwen(
    prompt: str,
    timeout: float,
    system_prompt: str = CSV_FIELD_SYSTEM_PROMPT,
) -> str:
    url = _resolve_chat_completions_url(settings.LLM_BASE_URL)
    headers: Dict[str, str] = {}
    if settings.LLM_API_KEY:
        headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"
    payload = _build_payload(prompt, system_prompt=system_prompt)
    response = requests.post(
        url=url,
        headers=headers,
        json=payload,
        timeout=timeout,
    )

    if response.status_code >= 400:
        raise LLMRequestError(
            f"LLM error {response.status_code}: {response.text}"
        )

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


def _strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\\n?", "", cleaned)
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip()


def _parse_json_object(text: str) -> Dict[str, Any]:
    parsed = json.loads(_strip_code_fences(text))
    if not isinstance(parsed, dict):
        raise ValueError("Expected JSON object from LLM")
    return parsed


def _call_llm_json(
    prompt: str,
    timeout: float,
    retries: int,
) -> Tuple[Dict[str, Any], str, str]:
    last_error = ""
    last_output = ""
    for attempt in range(1, retries + 2):
        try:
            llm_text = _call_qwen(
                prompt,
                timeout=timeout,
                system_prompt=BACKFILL_SYSTEM_PROMPT,
            )
            last_output = llm_text
            return _parse_json_object(llm_text), llm_text, ""
        except Exception as exc:
            last_error = str(exc)
            if attempt <= retries:
                time.sleep(0.5 * attempt)
                continue
    return {}, last_output, last_error


def _build_geo_backfill_prompt(country: str, province: str) -> str:
    payload = {
        "country": country,
        "province_or_city": province,
    }
    return (
        "Normalize location names to: country + first-level province/state. "
        "If input is a city (e.g., Wuhan), map it to its province (Hubei). "
        "If uncertain, return empty string.\\n"
        f"Input: {json.dumps(payload, ensure_ascii=False)}\\n"
        "Return JSON with keys: country, province, reason."
    )


def _build_pathogen_backfill_prompt(raw_pathogen: str) -> str:
    payload = {"pathogen": raw_pathogen}
    return (
        "Normalize pathogen mention into a canonical pathogen "
        "name/alias for dictionary "
        "matching. Keep answer short. If uncertain, return empty string.\\n"
        f"Input: {json.dumps(payload, ensure_ascii=False)}\\n"
        "Return JSON with keys: canonical_pathogen, reason."
    )


def _build_host_backfill_prompt(raw_host: str) -> str:
    payload = {"host": raw_host}
    return (
        "Classify host text into host_rank_1 and host_rank_2. "
        "children/infants/neonates/indigenous groups should usually map to Human. "
        "If uncertain, leave fields empty.\\n"
        f"Input: {json.dumps(payload, ensure_ascii=False)}\\n"
        "Return JSON with keys: normalized_host, host_rank_1, host_rank_2, reason."
    )


def _clear_unmatched_trackers(
    standardizer: Optional[CountryProvinceStandardizer],
    pathogen_std: Optional[PathogenStandardizer],
    host_std: Optional[HostStandardizer],
) -> None:
    if standardizer:
        standardizer.clear_unmatched()
    if pathogen_std:
        pathogen_std.clear_unmatched()
    if host_std:
        host_std.clear_unmatched()


def _rebuild_unmatched_tracking(
    records: List[Dict[str, str]],
    standardizer: Optional[CountryProvinceStandardizer],
    pathogen_std: Optional[PathogenStandardizer],
    host_std: Optional[HostStandardizer],
) -> None:
    _clear_unmatched_trackers(standardizer, pathogen_std, host_std)

    for rec in records:
        if standardizer:
            for prefix in ("original", "spread"):
                country_field = f"{prefix}_country"
                province_field = f"{prefix}_province"
                std_country = standardizer.standardize_country(
                    rec.get(country_field, "")
                )
                standardizer.standardize_province(
                    rec.get(province_field, ""),
                    std_country,
                )
        if pathogen_std:
            pathogen_std.standardize(rec.get("pathogen", ""))
        if host_std:
            host_std.standardize(rec.get("host", ""))


def _run_llm_backfill_on_unmatched(
    records: List[Dict[str, str]],
    standardizer: Optional[CountryProvinceStandardizer],
    pathogen_std: Optional[PathogenStandardizer],
    host_std: Optional[HostStandardizer],
    timeout: float,
    retries: int,
) -> List[Dict[str, Any]]:
    if not records:
        return []

    unmatched_countries: Set[str] = set()
    unmatched_provinces: Set[str] = set()
    unmatched_pathogens: Set[str] = set()
    unmatched_hosts: Set[str] = set()

    if standardizer:
        unmatched_countries = set(standardizer.get_unmatched_countries())
        unmatched_provinces = set(standardizer.get_unmatched_provinces())
    if pathogen_std:
        unmatched_pathogens = set(pathogen_std.get_unmatched())
    if host_std:
        unmatched_hosts = set(host_std.get_unmatched())

    geo_cache: Dict[str, Dict[str, Any]] = {}
    pathogen_cache: Dict[str, Dict[str, Any]] = {}
    host_cache: Dict[str, Dict[str, Any]] = {}

    for rec in records:
        if standardizer:
            for prefix in ("original", "spread"):
                country_field = f"{prefix}_country"
                province_field = f"{prefix}_province"
                raw_country = _safe_str(rec.get(country_field, ""))
                raw_province = _safe_str(rec.get(province_field, ""))
                pair_key = f"{raw_country}|{raw_province}"

                needs_geo = False
                if raw_country and raw_country in unmatched_countries:
                    needs_geo = True
                if pair_key in unmatched_provinces:
                    needs_geo = True
                if not needs_geo:
                    continue

                cache_item = geo_cache.get(pair_key)
                if cache_item is None:
                    prompt = _build_geo_backfill_prompt(
                        raw_country,
                        raw_province,
                    )
                    data, llm_text, error = _call_llm_json(
                        prompt,
                        timeout=timeout,
                        retries=retries,
                    )
                    cache_item = {
                        "country": _safe_str(data.get("country", "")),
                        "province": _safe_str(data.get("province", "")),
                        "llm_output": llm_text,
                        "error": error,
                        "usage_count": 0,
                        "applied_count": 0,
                    }
                    geo_cache[pair_key] = cache_item

                cache_item["usage_count"] += 1
                changed = False

                llm_country = _safe_str(cache_item.get("country", ""))
                llm_province = _safe_str(cache_item.get("province", ""))

                if llm_country:
                    std_country = standardizer.standardize_country(llm_country)
                    if std_country and std_country != raw_country:
                        rec[country_field] = std_country
                        changed = True

                if llm_province:
                    country_ctx = _safe_str(rec.get(country_field, ""))
                    std_province = standardizer.standardize_province(
                        llm_province,
                        country_ctx,
                    )
                    if std_province and std_province != raw_province:
                        rec[province_field] = std_province
                        changed = True

                if changed:
                    cache_item["applied_count"] += 1

        raw_pathogen = _safe_str(rec.get("pathogen", ""))
        if (
            pathogen_std
            and raw_pathogen
            and raw_pathogen in unmatched_pathogens
        ):
            cache_item = pathogen_cache.get(raw_pathogen)
            if cache_item is None:
                prompt = _build_pathogen_backfill_prompt(raw_pathogen)
                data, llm_text, error = _call_llm_json(
                    prompt,
                    timeout=timeout,
                    retries=retries,
                )
                cache_item = {
                    "canonical_pathogen": _safe_str(
                        data.get("canonical_pathogen", "")
                    ),
                    "llm_output": llm_text,
                    "error": error,
                    "usage_count": 0,
                    "applied_count": 0,
                }
                pathogen_cache[raw_pathogen] = cache_item

            cache_item["usage_count"] += 1
            candidate = _safe_str(cache_item.get("canonical_pathogen", ""))
            if not candidate:
                continue

            std_p, std_r1, std_r2 = pathogen_std.standardize(candidate)
            if (
                pathogen_std.is_known_pathogen(candidate)
                or pathogen_std.is_known_pathogen(std_p)
            ):
                changed = False
                if std_p and std_p != rec.get("pathogen", ""):
                    rec["pathogen"] = std_p
                    changed = True
                if std_r1 and std_r1 != rec.get("pathogen_rank_1", ""):
                    rec["pathogen_rank_1"] = std_r1
                    changed = True
                if std_r2 and std_r2 != rec.get("pathogen_rank_2", ""):
                    rec["pathogen_rank_2"] = std_r2
                    changed = True
                if changed:
                    cache_item["applied_count"] += 1

        raw_host = _safe_str(rec.get("host", ""))
        if host_std and raw_host and raw_host in unmatched_hosts:
            cache_item = host_cache.get(raw_host)
            if cache_item is None:
                prompt = _build_host_backfill_prompt(raw_host)
                data, llm_text, error = _call_llm_json(
                    prompt,
                    timeout=timeout,
                    retries=retries,
                )
                cache_item = {
                    "normalized_host": _safe_str(
                        data.get("normalized_host", "")
                    ),
                    "host_rank_1": _safe_str(data.get("host_rank_1", "")),
                    "host_rank_2": _safe_str(data.get("host_rank_2", "")),
                    "llm_output": llm_text,
                    "error": error,
                    "usage_count": 0,
                    "applied_count": 0,
                }
                host_cache[raw_host] = cache_item

            cache_item["usage_count"] += 1
            changed = False

            normalized_host = _safe_str(cache_item.get("normalized_host", ""))
            llm_rank_1 = _safe_str(cache_item.get("host_rank_1", ""))
            llm_rank_2 = _safe_str(cache_item.get("host_rank_2", ""))

            std_rank_1 = ""
            std_rank_2 = ""
            if normalized_host:
                std_rank_1, std_rank_2 = host_std.standardize(normalized_host)

            if std_rank_1 or std_rank_2:
                if std_rank_1 and std_rank_1 != rec.get("host_rank_1", ""):
                    rec["host_rank_1"] = std_rank_1
                    changed = True
                final_rank_2 = std_rank_2 or std_rank_1
                if final_rank_2 and final_rank_2 != rec.get("host_rank_2", ""):
                    rec["host_rank_2"] = final_rank_2
                    changed = True
            else:
                if llm_rank_1 and llm_rank_1 != rec.get("host_rank_1", ""):
                    rec["host_rank_1"] = llm_rank_1
                    changed = True
                fallback_rank_2 = llm_rank_2 or llm_rank_1
                if (
                    fallback_rank_2
                    and fallback_rank_2 != rec.get("host_rank_2", "")
                ):
                    rec["host_rank_2"] = fallback_rank_2
                    changed = True

            if changed:
                cache_item["applied_count"] += 1

    logs: List[Dict[str, Any]] = []
    for raw_input, cache_item in sorted(geo_cache.items()):
        status = "error" if cache_item.get("error") else "ok"
        if cache_item.get("applied_count", 0) > 0:
            status = "applied"
        logs.append(
            {
                "category": "country_province",
                "raw_input": raw_input,
                "resolved_value": json.dumps(
                    {
                        "country": _safe_str(cache_item.get("country", "")),
                        "province": _safe_str(cache_item.get("province", "")),
                    },
                    ensure_ascii=False,
                ),
                "status": status,
                "usage_count": int(cache_item.get("usage_count", 0)),
                "applied_count": int(cache_item.get("applied_count", 0)),
                "error": _safe_str(cache_item.get("error", "")),
                "llm_output": _safe_str(cache_item.get("llm_output", "")),
            }
        )

    for raw_input, cache_item in sorted(pathogen_cache.items()):
        status = "error" if cache_item.get("error") else "ok"
        if cache_item.get("applied_count", 0) > 0:
            status = "applied"
        logs.append(
            {
                "category": "pathogen",
                "raw_input": raw_input,
                "resolved_value": _safe_str(
                    cache_item.get("canonical_pathogen", "")
                ),
                "status": status,
                "usage_count": int(cache_item.get("usage_count", 0)),
                "applied_count": int(cache_item.get("applied_count", 0)),
                "error": _safe_str(cache_item.get("error", "")),
                "llm_output": _safe_str(cache_item.get("llm_output", "")),
            }
        )

    for raw_input, cache_item in sorted(host_cache.items()):
        status = "error" if cache_item.get("error") else "ok"
        if cache_item.get("applied_count", 0) > 0:
            status = "applied"
        logs.append(
            {
                "category": "host",
                "raw_input": raw_input,
                "resolved_value": json.dumps(
                    {
                        "normalized_host": _safe_str(
                            cache_item.get("normalized_host", "")
                        ),
                        "host_rank_1": _safe_str(
                            cache_item.get("host_rank_1", "")
                        ),
                        "host_rank_2": _safe_str(
                            cache_item.get("host_rank_2", "")
                        ),
                    },
                    ensure_ascii=False,
                ),
                "status": status,
                "usage_count": int(cache_item.get("usage_count", 0)),
                "applied_count": int(cache_item.get("applied_count", 0)),
                "error": _safe_str(cache_item.get("error", "")),
                "llm_output": _safe_str(cache_item.get("llm_output", "")),
            }
        )

    return logs


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
                "row_index": row_index,
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
        "row_index": row_index,
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
    dict_xlsx: Optional[Path] = None,
    dict_pathogen_xlsx: Optional[Path] = None,
    dict_host_xlsx: Optional[Path] = None,
    unmatched_file: Optional[Path] = None,
    enable_llm_backfill: bool = False,
    llm_backfill_timeout: float = 0.0,
    llm_backfill_retries: int = 1,
    llm_backfill_log_file: Optional[Path] = None,
) -> None:
    standardizer: Optional[CountryProvinceStandardizer] = None
    if dict_xlsx and dict_xlsx.exists():
        standardizer = CountryProvinceStandardizer(dict_xlsx)
        print(f"Loaded country/province reference: {dict_xlsx}")
    elif dict_xlsx:
        print(f"WARNING: dict_xlsx not found: {dict_xlsx}, skipping standardization")

    pathogen_std: Optional[PathogenStandardizer] = None
    if dict_pathogen_xlsx and dict_pathogen_xlsx.exists():
        pathogen_std = PathogenStandardizer(dict_pathogen_xlsx)
        print(f"Loaded pathogen reference: {dict_pathogen_xlsx}")
    elif dict_pathogen_xlsx:
        print(f"WARNING: dict_pathogen_xlsx not found: {dict_pathogen_xlsx}, skipping")

    host_std: Optional[HostStandardizer] = None
    if dict_host_xlsx and dict_host_xlsx.exists():
        host_std = HostStandardizer(dict_host_xlsx)
        print(f"Loaded host reference: {dict_host_xlsx}")
    elif dict_host_xlsx:
        print(f"WARNING: dict_host_xlsx not found: {dict_host_xlsx}, skipping")
    df = pd.read_csv(input_csv, encoding="utf-8")
    if limit > 0:
        df = df.head(limit)

    if "detail_url" not in df.columns or "full_text" not in df.columns:
        raise ValueError("CSV must include columns: detail_url, full_text")

    rows: List[Tuple[int, str, str]] = []
    for row_index, (_, row) in enumerate(df.iterrows()):
        rows.append(
            (
                row_index,
                _safe_str(row.get("detail_url", "")),
                _safe_str(row.get("full_text", "")),
            )
        )

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
            progress_writer = csv.writer(f)
            progress_writer.writerow(
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
                        progress_writer = csv.writer(f)
                        progress_writer.writerow(
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

    for rec in result_rows:
        enrich_record(rec, standardizer, pathogen_std, host_std)

    llm_backfill_logs: List[Dict[str, Any]] = []
    if enable_llm_backfill and (standardizer or pathogen_std or host_std):
        print("Running optional stage-2 LLM backfill on unmatched values...")
        effective_backfill_timeout = (
            llm_backfill_timeout if llm_backfill_timeout > 0 else timeout
        )
        llm_backfill_logs = _run_llm_backfill_on_unmatched(
            result_rows,
            standardizer=standardizer,
            pathogen_std=pathogen_std,
            host_std=host_std,
            timeout=effective_backfill_timeout,
            retries=max(0, llm_backfill_retries),
        )
        _rebuild_unmatched_tracking(
            result_rows,
            standardizer=standardizer,
            pathogen_std=pathogen_std,
            host_std=host_std,
        )

    backfill_df = pd.DataFrame(llm_backfill_logs, columns=BACKFILL_LOG_FIELDS)
    backfill_applied = 0
    if not backfill_df.empty:
        backfill_applied = len(backfill_df[backfill_df["applied_count"] > 0])

    result_df = pd.DataFrame(result_rows, columns=OUTPUT_FIELDS)
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
                "throughput_rows_per_min": (
                    round((len(df) / total_seconds) * 60, 2)
                    if total_seconds
                    else 0.0
                ),
                "recommended_workers": "2-4 for qwen3:235b (start from 2)",
                "recommended_chars_per_text": max_chars,
                "llm_backfill_enabled": int(enable_llm_backfill),
                "llm_backfill_unique_queries": len(backfill_df),
                "llm_backfill_applied_queries": backfill_applied,
            }
        ]
    )

    output_excel.parent.mkdir(parents=True, exist_ok=True)
    output_timing_csv.parent.mkdir(parents=True, exist_ok=True)
    excel_written = False
    excel_error = ""
    for engine in ("xlsxwriter", "openpyxl"):
        try:
            with pd.ExcelWriter(output_excel, engine=engine) as excel_writer:
                result_df.to_excel(
                    excel_writer,
                    index=False,
                    sheet_name="extracted",
                )
                timing_df.to_excel(
                    excel_writer,
                    index=False,
                    sheet_name="timing",
                )
                summary_df.to_excel(
                    excel_writer,
                    index=False,
                    sheet_name="summary",
                )
                if enable_llm_backfill:
                    backfill_df.to_excel(
                        excel_writer,
                        index=False,
                        sheet_name="llm_backfill",
                    )
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
            "llm_backfill": backfill_df.to_dict(orient="records"),
        }
        with final_output_json.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    if enable_llm_backfill:
        _backfill_log_path = llm_backfill_log_file or output_excel.with_name(
            output_excel.stem + "_llm_backfill.csv"
        )
        _backfill_log_path.parent.mkdir(parents=True, exist_ok=True)
        backfill_df.to_csv(
            _backfill_log_path,
            index=False,
            encoding="utf-8-sig",
        )
        print(f"LLM backfill log csv: {_backfill_log_path}")

    if standardizer or pathogen_std or host_std:
        _unmatched_path = unmatched_file or output_excel.with_name(
            output_excel.stem + "_unmatched.txt"
        )
        save_all_unmatched(
            _unmatched_path,
            standardizer,
            pathogen_std,
            host_std,
        )

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
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\who_docs_emergencies_detail.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--output-excel",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\who_docs_emergencies_detail_result.xlsx",
        help="Output Excel path",
    )
    parser.add_argument(
        "--output-timing-csv",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\who_docs_emergencies_timing_result.csv",
        help="Output timing CSV path",
    )
    parser.add_argument(
        "--output-json",
        default=r"",
        help="Optional JSON output path (auto used when Excel engine is unavailable)",
    )
    parser.add_argument("--timeout", type=float, default=max(600.0, settings.LLM_TIMEOUT))    #如果报 429/超时，降低并发到 1-2，并适当增大 --timeout（如 180）。
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
    parser.add_argument(
        "--dict-xlsx",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\dict_country_global_all.xlsx",
        help="Path to country/province reference xlsx for standardization",
    )
    parser.add_argument(
        "--dict-pathogen-xlsx",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\dict_pathogen_feature.xlsx",
        help="Path to pathogen reference xlsx for standardization",
    )
    parser.add_argument(
        "--dict-host-xlsx",
        default=r"C:\Users\imcas\Desktop\Biometric Information Extraction\dict_host_tag.xlsx",
        help="Path to host reference xlsx for standardization",
    )
    parser.add_argument(
        "--unmatched-file",
        default=r"",
        help="Path to save unmatched names",
    )
    parser.add_argument(
        "--enable-llm-backfill",
        action="store_true",
        help=(
            "Run stage-2 LLM backfill only for values "
            "unmatched by dictionaries"
        ),
    )
    parser.add_argument(
        "--llm-backfill-timeout",
        type=float,
        default=max(120.0, settings.LLM_TIMEOUT),
        help="Timeout (seconds) for each LLM backfill request",
    )
    parser.add_argument(
        "--llm-backfill-retries",
        type=int,
        default=1,
        help="Retry times for each LLM backfill request",
    )
    parser.add_argument(
        "--llm-backfill-log-file",
        default=r"",
        help="Optional path to save stage-2 LLM backfill log CSV",
    )
    args = parser.parse_args()

    progress_file_path = Path(args.progress_file) if args.progress_file.strip() else None
    output_json_path = Path(args.output_json) if args.output_json.strip() else None
    dict_xlsx_path = Path(args.dict_xlsx) if args.dict_xlsx.strip() else None
    dict_pathogen_path = Path(args.dict_pathogen_xlsx) if args.dict_pathogen_xlsx.strip() else None
    dict_host_path = Path(args.dict_host_xlsx) if args.dict_host_xlsx.strip() else None
    unmatched_file_path = Path(args.unmatched_file) if args.unmatched_file.strip() else None
    llm_backfill_log_path = (
        Path(args.llm_backfill_log_file)
        if args.llm_backfill_log_file.strip()
        else None
    )

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
        dict_xlsx=dict_xlsx_path,
        dict_pathogen_xlsx=dict_pathogen_path,
        dict_host_xlsx=dict_host_path,
        unmatched_file=unmatched_file_path,
        enable_llm_backfill=bool(args.enable_llm_backfill),
        llm_backfill_timeout=max(1.0, float(args.llm_backfill_timeout)),
        llm_backfill_retries=max(0, int(args.llm_backfill_retries)),
        llm_backfill_log_file=llm_backfill_log_path,
    )


if __name__ == "__main__":
    main()
