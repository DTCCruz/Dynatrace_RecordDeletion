#!/usr/bin/env python3

"""
===============================================================================
DISCLAIMER - LEGAL NOTICE
===============================================================================
This script is provided AS IS as a best-effort starting point for Grail data
export and deletion workflows. It is not a Dynatrace product and includes no
warranties, official support, versioning, or maintenance commitments.

The customer or user is responsible for installation, customization,
implementation, validation, security review, and ongoing operation.
===============================================================================
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import requests
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency: requests\n"
        "Use the workspace venv interpreter:\n"
        "  .venv/bin/python grail_query_to_csv.py --cleanup\n"
        "Or install for your current python3:\n"
        "  python3 -m pip install requests"
    ) from exc

def load_env(env_path: str = ".env"):
    p = Path(env_path)
    if not p.exists():
        return

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            if len(val) >= 2 and ((val[0] == "\"" and val[-1] == "\"") or (val[0] == "'" and val[-1] == "'")):
                val = val[1:-1]
            os.environ.setdefault(key, val)


def ensure_python_version(min_major: int = 3, min_minor: int = 9) -> None:
    if sys.version_info < (min_major, min_minor):
        found = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        raise SystemExit(
            f"Unsupported Python version: {found}. "
            f"Use Python {min_major}.{min_minor}+ to run this script."
        )


def normalize_environment(env: str) -> str:
    env = env.strip().rstrip("/")
    if env.startswith("http://") or env.startswith("https://"):
        return env

    # If the environment already looks like a full domain name (contains a dot),
    # keep it as-is. For short IDs, default to .apps.dynatrace.com.
    if "." in env:
        return "https://" + env

    return f"https://{env}.apps.dynatrace.com"


def build_validation_query(query: str) -> str:
    validation_query = query.strip()
    if "| limit" not in validation_query.lower():
        validation_query = f"{validation_query} | limit 1"
    return validation_query


def run_query(base_url: str, token: str, query: str, tf_start: str, tf_end: str) -> tuple[dict, str]:
    # Prefer v2, fallback to v1. Final behavior uses whichever succeeds.
    headers = {
        "accept": "application/json",
        "enforce-query-consumption-limit": "true",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    payload = {
        "defaultTimeframeStart": tf_start,
        "defaultTimeframeEnd": tf_end,
        "enablePreview": False,
        "enforceQueryConsumptionLimit": False,
        "fetchTimeoutSeconds": 3600,
        "includeTypes": True,
        "locale": "en_US",
        "maxResultBytes": 107374182400,
        "maxResultRecords": 50000000,
        "query": query,
        "requestTimeoutMilliseconds": 60000,
        "timezone": "UTC",
    }

    for version in ("v2", "v1"):
        execute_url = f"{base_url}/platform/storage/query/{version}/query:execute"
        poll_url = f"{base_url}/platform/storage/query/{version}/query:poll"

        resp = requests.post(execute_url, headers=headers, json=payload, stream=True, timeout=(15, 3700))

        if resp.status_code == 404:
            continue

        if resp.status_code not in (200, 202):
            # on auth error, retry won't help; raise immediately
            raise RuntimeError(f"execute {version} status {resp.status_code}: {resp.text[:1000]}")

        data = resp.json()

        if resp.status_code == 200 and data.get("result"):
            return data, ""

        request_token = data.get("requestToken")
        if not request_token:
            raise RuntimeError(f"unexpected {version} response missing requestToken: {data}")

        while True:
            time.sleep(2)
            resp2 = requests.get(poll_url, headers=headers, params={"request-token": request_token}, timeout=(15, 3700))

            if resp2.status_code == 405 and version == "v2":
                # v2 poll might not support GET; try next version in top loop.
                break

            if resp2.status_code == 404 and version == "v1":
                # query not found maybe token expired; re-execute not recoverable here.
                raise RuntimeError(f"poll {version} 404 QUERY_NOT_FOUND for request-token {request_token}")

            if resp2.status_code != 200:
                raise RuntimeError(f"poll {version} status {resp2.status_code}: {resp2.text[:200]}")

            obj2 = resp2.json()
            state = obj2.get("state")

            if state in ("SUCCEEDED", "COMPLETED"):
                if "result" in obj2:
                    return obj2, request_token
                return obj2, request_token

            if state in ("FAILED", "ERROR", "CANCELLED"):
                raise RuntimeError(f"query failed state={state} body={obj2}")

            request_token = obj2.get("requestToken", request_token)

    raise RuntimeError("No supported query execute/poll endpoint available (v2 and v1 failed).")


def format_bytes(num_bytes: int) -> str:
    if num_bytes < 0:
        return "unknown"

    units = ("B", "KB", "MB", "GB", "TB", "PB")
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0

    return f"{num_bytes} B"


def format_currency(amount: float, currency: str = "USD") -> str:
    code = (currency or "USD").strip().upper()
    symbol = "R$" if code == "BRL" else "$"
    return f"{symbol}{amount:,.4f}"


def parse_positive_float(raw: str) -> Optional[float]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def load_cost_rate_per_gib(
    env_rate: Optional[str],
    legacy_env_rate: Optional[str],
) -> tuple[Optional[float], str]:
    env_rate_value = parse_positive_float(env_rate or "")
    if env_rate_value is not None:
        return env_rate_value, "env:DT_LOG_QUERY_COST_RATE_PER_GIB"

    legacy_rate_value = parse_positive_float(legacy_env_rate or "")
    if legacy_rate_value is not None:
        return legacy_rate_value, "env:DT_LOG_QUERY_COST_RATE_USD_PER_GIB (legacy)"

    return None, (
        "not configured (set DT_LOG_QUERY_COST_RATE_PER_GIB in .env; "
        "legacy DT_LOG_QUERY_COST_RATE_USD_PER_GIB is also supported)"
    )


def load_cost_currency(env_currency: Optional[str]) -> str:
    value = (env_currency or "").strip().upper()
    if value in ("USD", "BRL"):
        return value
    return "USD"


def extract_scanned_bytes(obj: object) -> int:
    if isinstance(obj, bool):
        return -1
    if isinstance(obj, (int, float)):
        return int(obj)
    if isinstance(obj, str):
        parsed = parse_positive_float(obj)
        return int(parsed) if parsed is not None else -1

    best = -1

    def _walk(node: object) -> None:
        nonlocal best
        if isinstance(node, dict):
            for key, value in node.items():
                normalized = str(key).replace("_", "").replace("-", "").lower()
                if normalized in ("scannedbytes", "queryscannedbytes", "bytesscanned", "readbytes"):
                    candidate = extract_scanned_bytes(value)
                    if candidate >= 0:
                        best = max(best, candidate)
                _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(obj)
    return best


def estimate_records_json_size_bytes(records) -> int:
    """Estimate UTF-8 payload size of in-memory records using compact JSON serialization."""
    total_bytes = 0
    for record in records:
        if isinstance(record, bytes):
            total_bytes += len(record)
            continue
        if isinstance(record, str):
            total_bytes += len(record.encode("utf-8"))
            continue

        record_json = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        total_bytes += len(record_json.encode("utf-8"))

    return total_bytes


def download_query_result(base_url: str, token: str, request_token: str, out_path: str) -> tuple[bool, int]:
    if not request_token:
        return False, 0

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/csv,application/json",
    }

    # v1 path confirmed in this tenant.
    download_url = f"{base_url}/platform/storage/query/v1/query:download"
    params = {
        "request-token": request_token,
        "outputFormat": "CSV",
    }

    resp = requests.get(download_url, headers=headers, params=params, stream=True, timeout=(15, 3600))
    if resp.status_code not in (200, 206):
        raise RuntimeError(f"download status {resp.status_code}: {resp.text[:1000]}")

    downloaded_bytes = 0
    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=32768):
            if chunk:
                f.write(chunk)
                downloaded_bytes += len(chunk)

    return True, downloaded_bytes


def delete_records_in_grail(base_url: str, token: str, delete_query: str, tf_start: str, tf_end: str) -> bool:
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    execute_url = f"{base_url}/platform/storage/record/v1/delete:execute"
    status_url = f"{base_url}/platform/storage/record/v1/delete:status"

    payload = {
        "query": delete_query,
        "timeFrame": {
            "start": tf_start,
            "end": tf_end,
        },
        "timezone": "UTC",
    }

    resp = requests.post(execute_url, headers=headers, json=payload, timeout=60)
    if resp.status_code != 202:
        raise RuntimeError(f"delete execute status {resp.status_code}: {resp.text[:1000]}")

    task_id = resp.json().get("taskId")
    if not task_id:
        raise RuntimeError(f"delete execute missing taskId: {resp.text[:1000]}")

    deadline = time.time() + 1800
    transient_errors = 0
    while time.time() < deadline:
        try:
            st = requests.post(status_url, headers=headers, json={"taskId": task_id}, timeout=60)
            transient_errors = 0
        except requests.RequestException as exc:
            transient_errors += 1
            if transient_errors >= 5:
                raise RuntimeError(
                    f"delete status polling failed after {transient_errors} transient errors: {exc}"
                ) from exc

            print(
                f"Transient delete status polling error ({transient_errors}/5): {exc}. Retrying in 5s..."
            )
            time.sleep(5)
            continue

        if st.status_code != 200:
            raise RuntimeError(f"delete status {st.status_code}: {st.text[:1000]}")

        body = st.json()
        status = (body.get("status") or "").lower()

        if status in ("finished", "completed", "succeeded", "success"):
            return True
        if status in ("failed", "error", "cancelled", "canceled"):
            raise RuntimeError(f"delete failed: {body}")

        time.sleep(5)

    raise TimeoutError(f"delete task timeout: {task_id}")


def validate_query_permission(base_url: str, token: str, query: str, tf_start: str, tf_end: str) -> tuple[bool, str]:
    try:
        validation_query = build_validation_query(query)
        run_query(base_url, token, validation_query, tf_start, tf_end)
        return True, "query access check passed"
    except Exception as exc:
        message = str(exc)
        if "status 401" in message or "status 403" in message:
            return False, f"query permission check failed: {message}"
        return False, f"query preflight failed: {message}"


def validate_delete_permission(base_url: str, token: str) -> tuple[bool, str]:
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    status_url = f"{base_url}/platform/storage/record/v1/delete:status"
    try:
        resp = requests.post(status_url, headers=headers, json={"taskId": "permission-check"}, timeout=30)
    except requests.RequestException as exc:
        return False, f"delete permission check request failed: {exc}"

    if resp.status_code in (401, 403):
        return False, f"delete permission check failed with HTTP {resp.status_code}"

    # 200/400/404/422 all indicate authenticated access to endpoint.
    if resp.status_code in (200, 400, 404, 422):
        return True, "delete access check passed"

    return False, f"delete permission check returned unexpected HTTP {resp.status_code}: {resp.text[:300]}"


def run_preflight_checks(
    base_url: str,
    token: str,
    query: str,
    tf_start: str,
    tf_end: str,
    cleanup_requested: bool,
    dry_run_delete: bool,
    delete_query: str,
    delete_tf_end: str,
) -> bool:
    print("Running preflight checks...")

    query_ok, query_msg = validate_query_permission(base_url, token, query, tf_start, tf_end)
    print(f"  Query access: {query_msg}")
    if not query_ok:
        return False

    if cleanup_requested or dry_run_delete:
        if not delete_query:
            print("  Delete preflight: missing delete query")
            return False
        if "| limit" in delete_query.lower():
            print("  Delete preflight: delete query must not contain limit")
            return False

        delete_end_ts = parse_iso8601(delete_tf_end)
        if delete_end_ts > (datetime.now(timezone.utc) - timedelta(hours=4)):
            print("  Delete preflight: delete end time must be at least 4 hours in the past")
            return False

        delete_ok, delete_msg = validate_delete_permission(base_url, token)
        print(f"  Delete access: {delete_msg}")
        if not delete_ok:
            return False

    print("Preflight checks passed.")
    return True


def count_records_in_grail(base_url: str, token: str, delete_query: str, tf_start: str, tf_end: str) -> int:
    """Return number of matching records in Grail for the given query and timeframe."""
    count_query = delete_query.strip()
    if "| limit" not in count_query.lower():
        count_query = f"{count_query} | limit 1"
    try:
        result, _ = run_query(base_url, token, count_query, tf_start, tf_end)
        records = result.get("result", {}).get("records") or []
        return len(records)
    except Exception:
        return -1  # unknown


def validate_records_deleted(
    base_url: str,
    token: str,
    delete_query: str,
    tf_start: str,
    tf_end: str,
    retries: int = 12,
    interval_seconds: int = 10,
) -> bool:
    validation_query = delete_query.strip()
    if "| limit" not in validation_query.lower():
        validation_query = f"{validation_query} | limit 1"

    for attempt in range(1, retries + 1):
        try:
            result, _ = run_query(base_url, token, validation_query, tf_start, tf_end)
            records = result.get("result", {}).get("records") or []
            if len(records) == 0:
                print(f"Post-delete validation passed on attempt {attempt}: no matching records found.")
                return True

            print(
                f"Post-delete validation attempt {attempt}/{retries}: "
                f"{len(records)} matching record(s) still visible."
            )
        except Exception as exc:
            print(f"Post-delete validation attempt {attempt}/{retries} query error: {exc}")

        if attempt < retries:
            time.sleep(interval_seconds)

    print("Post-delete validation did not confirm zero records within the configured wait window.")
    return False


def parse_iso8601(ts_str: str) -> datetime:
    """Parse ISO8601 timestamp string to datetime object."""
    # Handle formats like 2026-03-19T03:00:00.999999999Z or 2026-03-19T00:00:00.000000000Z
    ts_str = ts_str.strip()
    
    # Handle nanoseconds first (before timezone replacement)
    if "." in ts_str:
        dot_idx = ts_str.index(".")
        # Find timezone offset position
        tz_start = len(ts_str)
        for i in range(dot_idx + 1, len(ts_str)):
            if ts_str[i] in ('+', '-', 'Z'):
                tz_start = i
                break
        
        # Extract fractional seconds and timezone parts
        frac_part = ts_str[dot_idx + 1:tz_start]
        tz_part = ts_str[tz_start:]
        
        # Truncate fractional seconds to microseconds (6 digits) if needed
        if len(frac_part) > 6:
            frac_part = frac_part[:6]
            ts_str = ts_str[:dot_idx + 1] + frac_part + tz_part
    
    # Now handle Z -> +00:00 conversion
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    
    return datetime.fromisoformat(ts_str)


def calculate_24h_chunks(tf_start: str, tf_end: str) -> list[tuple[str, str]]:
    """
    Break date range into 24-hour chunks working backwards from tf_end to tf_start.
    Returns list of (chunk_start, chunk_end) tuples in chronological order.
    """
    start_dt = parse_iso8601(tf_start)
    end_dt = parse_iso8601(tf_end)

    if start_dt >= end_dt:
        raise ValueError(f"Start date must be before end date: {tf_start} >= {tf_end}")

    chunks = []
    current_end = end_dt

    # Work backwards from end_dt to start_dt
    while current_end > start_dt:
        current_start = current_end - timedelta(hours=24)
        # Don't go before the original start date
        if current_start < start_dt:
            current_start = start_dt

        # Format back to ISO8601 (chunks must be exactly 24h, not 24h + nanoseconds)
        chunk_start = current_start.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
        chunk_end = current_end.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")

        chunks.append((chunk_start, chunk_end))
        current_end = current_start

    # Reverse to get chronological order (oldest to newest)
    chunks.reverse()
    return chunks


def format_duration(seconds: float) -> str:
    if seconds < 0:
        return "unknown"

    total_seconds = int(round(seconds))
    hours, rem = divmod(total_seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def extract_first_numeric_value(record: dict) -> int:
    for value in record.values():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                if value.strip() and value.strip().lstrip("-").isdigit():
                    return int(value.strip())
            except Exception:
                continue
    return -1


def query_chunk_record_count(base_url: str, token: str, delete_query: str, tf_start: str, tf_end: str) -> tuple[int, int]:
    count_query = f"{delete_query.strip()} | summarize __count=count()"
    try:
        result, _ = run_query(base_url, token, count_query, tf_start, tf_end)
        records = result.get("result", {}).get("records") or []
        scanned_bytes = extract_scanned_bytes(result)
        if not records:
            return 0, scanned_bytes
        first = records[0]
        if isinstance(first, dict):
            return max(0, extract_first_numeric_value(first)), scanned_bytes
        return -1, scanned_bytes
    except Exception as exc:
        print(f"Chunk count query failed for {tf_start} to {tf_end}: {exc}")
        return -1, -1


def query_chunk_size_estimate(
    base_url: str,
    token: str,
    delete_query: str,
    tf_start: str,
    tf_end: str,
    sample_limit: int = 1000,
) -> tuple[int, int]:
    sample_query = f"{delete_query.strip()} | limit {sample_limit}"
    try:
        result, _ = run_query(base_url, token, sample_query, tf_start, tf_end)
        records = result.get("result", {}).get("records") or []
        if not records:
            return 0, 0

        sample_bytes = estimate_records_json_size_bytes(records)
        return len(records), sample_bytes
    except Exception as exc:
        print(f"Chunk size sample query failed for {tf_start} to {tf_end}: {exc}")
        return -1, -1


def estimate_delete_seconds(
    records: int,
    bytes_est: int,
    observed_chunks: list[dict],
) -> float:
    completed = [c for c in observed_chunks if c.get("deleted")]
    if completed:
        total_obs_delete = sum(c.get("delete_seconds", 0.0) for c in completed)
        total_obs_records = sum(max(0, c.get("records", -1)) for c in completed)
        total_obs_bytes = sum(max(0, c.get("bytes_est", -1)) for c in completed)

        estimates = []
        if total_obs_delete > 0 and records >= 0 and total_obs_records > 0:
            rec_rate = total_obs_records / total_obs_delete
            if rec_rate > 0:
                estimates.append(records / rec_rate)

        if total_obs_delete > 0 and bytes_est >= 0 and total_obs_bytes > 0:
            byte_rate = total_obs_bytes / total_obs_delete
            if byte_rate > 0:
                estimates.append(bytes_est / byte_rate)

        if estimates:
            return max(5.0, sum(estimates) / len(estimates))

    # Bootstrap heuristic before observed performance is available.
    baseline = 8.0
    if records >= 0:
        baseline += records / 30000.0
    if bytes_est >= 0:
        baseline += bytes_est / float(200 * 1024 * 1024)
    return max(5.0, baseline)


def estimate_combined_seconds(
    est_delete_seconds: float,
    observed_chunks: list[dict],
    default_overhead_seconds: float,
) -> float:
    completed = [c for c in observed_chunks if c.get("deleted")]
    if completed:
        overhead_samples = [
            max(0.0, c.get("combined_seconds", 0.0) - c.get("delete_seconds", 0.0))
            for c in completed
        ]
        if overhead_samples:
            avg_overhead = sum(overhead_samples) / len(overhead_samples)
            return max(est_delete_seconds, est_delete_seconds + avg_overhead)

    return est_delete_seconds + default_overhead_seconds


def print_chunk_estimate_table(chunk_metrics: list[dict]):
    print("\nChunk workload and time estimate (24h windows):")
    print("Idx | Window Start -> End      | Records    | Est. Bytes | Est. Delete | Est. Combined")
    print("----+--------------------------+------------+------------+-------------+--------------")
    for metric in chunk_metrics:
        records = metric.get("records", -1)
        bytes_est = metric.get("bytes_est", -1)
        records_str = f"{records:,}" if records >= 0 else "unknown"
        bytes_str = format_bytes(bytes_est) if bytes_est >= 0 else "unknown"
        est_delete_str = format_duration(metric.get("est_delete_seconds", -1.0))
        est_combined_str = format_duration(metric.get("est_combined_seconds", -1.0))
        window = f"{metric['start'][:10]} -> {metric['end'][:10]}"
        print(
            f"{metric['index']:>3} | {window:<24} | {records_str:>10} | {bytes_str:>10} | "
            f"{est_delete_str:>11} | {est_combined_str:>12}"
        )


def print_chunk_estimate_totals(chunk_metrics: list[dict]):
    est_total_delete = sum(m.get("est_delete_seconds", 0.0) for m in chunk_metrics)
    est_total_combined = sum(m.get("est_combined_seconds", 0.0) for m in chunk_metrics)
    print(
        "Estimated totals: "
        f"delete={format_duration(est_total_delete)}, "
        f"combined={format_duration(est_total_combined)}"
    )


def estimate_delete_query_cost(
    chunk_metrics: list[dict],
    rate_usd_per_gib: Optional[float],
) -> tuple[int, Optional[float], int, int]:
    known_scan_values = [
        int(m.get("scanned_bytes", -1))
        for m in chunk_metrics
        if int(m.get("scanned_bytes", -1)) >= 0
    ]
    total_scanned_bytes = sum(known_scan_values)
    known_chunks = len(known_scan_values)
    total_chunks = len(chunk_metrics)

    if rate_usd_per_gib is None or total_scanned_bytes <= 0:
        return total_scanned_bytes, None, known_chunks, total_chunks

    scanned_gib = total_scanned_bytes / float(1024 ** 3)
    estimated_cost = scanned_gib * rate_usd_per_gib
    return total_scanned_bytes, estimated_cost, known_chunks, total_chunks


def print_delete_query_cost_summary(
    total_scanned_bytes: int,
    estimated_cost_usd: Optional[float],
    rate_usd_per_gib: Optional[float],
    rate_source: str,
    known_chunks: int,
    total_chunks: int,
    cost_currency: str,
) -> None:
    print("\nEstimated query scan/cost for delete workflow:")
    if total_scanned_bytes > 0:
        print(
            f"  Scanned bytes (estimated from chunk count queries): "
            f"{total_scanned_bytes:,} ({format_bytes(total_scanned_bytes)})"
        )
    else:
        print("  Scanned bytes: unknown")

    print(f"  Coverage: {known_chunks}/{total_chunks} chunk(s) reported scanned_bytes")

    if rate_usd_per_gib is None:
        print(
            "  Rate card: not configured "
            "(set DT_LOG_QUERY_COST_RATE_PER_GIB in .env)"
        )
        print("  Estimated cost: unknown")
        return

    print(f"  Rate card source: {rate_source}")
    print(f"  Rate card: {format_currency(rate_usd_per_gib, cost_currency)} per GiB scanned")
    if estimated_cost_usd is None:
        print("  Estimated cost: unknown (missing scanned_bytes)")
        return

    print(f"  Estimated cost for current delete query: {format_currency(estimated_cost_usd, cost_currency)}")


def query_actual_delete_cost(
    base_url: str,
    token: str,
    delete_query: str,
    tf_start: str,
    tf_end: str,
    rate_usd_per_gib: Optional[float],
) -> tuple[Optional[int], Optional[float], Optional[str]]:
    """
    Query dt.system.events for actual billed_bytes from the delete operation.
    Returns (billed_bytes, actual_cost_usd, error_message).
    Non-blocking: returns (None, None, error_msg) if query fails.
    """
    try:
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        
        # Query dt.system.events for BILLING_USAGE_EVENT records within the delete window
        dql_query = (
            'fetch dt.system.events '
            '| filter event.kind == "BILLING_USAGE_EVENT" '
            'and matchesPhrase(event.type, "Query") '
            f'and timestamp >= toTimestamp("{tf_start}") '
            f'and timestamp < toTimestamp("{tf_end}") '
            '| summarize total_billed_bytes = sum(billed_bytes)'
        )
        
        payload = {
            "defaultTimeframeStart": tf_start,
            "defaultTimeframeEnd": tf_end,
            "enablePreview": False,
            "enforceQueryConsumptionLimit": False,
            "fetchTimeoutSeconds": 600,
            "query": dql_query,
        }
        
        # Try v2 first, fallback to v1
        query_v2_url = f"{base_url}/platform/query/v2/query:execute"
        query_v1_url = f"{base_url}/api/v2/query/queryExecutionEngine:execute"
        
        resp = None
        last_error = None
        for url in [query_v2_url, query_v1_url]:
            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=300)
                if resp.status_code == 200:
                    break
                last_error = f"status {resp.status_code}"
            except Exception as e:
                last_error = str(e)
        
        if resp is None or resp.status_code != 200:
            error_detail = last_error
            if resp is not None and resp.status_code == 403:
                try:
                    error_body = resp.text[:500]  # First 500 chars of response
                    error_detail = f"status 403 (Forbidden) - {error_body}"
                except:
                    error_detail = "status 403 (Forbidden)"
            return None, None, f"Could not query billing data: {error_detail}"
        
        result = resp.json()
        records = result.get("result", {}).get("records", [])
        
        if not records:
            return 0, 0.0, None
        
        first_record = records[0]
        billed_bytes = first_record.get("total_billed_bytes")
        
        if billed_bytes is None:
            return None, None, "Billing query did not return billed_bytes"
        
        billed_bytes = int(billed_bytes) if isinstance(billed_bytes, (int, float)) else None
        if billed_bytes is None:
            return None, None, "Could not parse billed_bytes from query result"
        
        actual_cost = None
        if rate_usd_per_gib is not None and billed_bytes > 0:
            billed_gib = billed_bytes / float(1024 ** 3)
            actual_cost = billed_gib * rate_usd_per_gib
        
        return billed_bytes, actual_cost, None
        
    except Exception as exc:
        return None, None, f"Billing query error: {str(exc)}"


def calculate_cost_variance(
    estimated_bytes: int,
    estimated_cost: Optional[float],
    actual_bytes: Optional[int],
    actual_cost: Optional[float],
) -> tuple[Optional[float], Optional[str]]:
    """
    Calculate percentage variance between estimate and actual.
    Returns (variance_percent, variance_string).
    """
    if actual_bytes is None or estimated_bytes <= 0:
        return None, "not available"
    
    variance_percent = ((actual_bytes - estimated_bytes) / estimated_bytes) * 100
    if variance_percent >= 0:
        return variance_percent, f"+{variance_percent:.1f}%"
    return variance_percent, f"-{abs(variance_percent):.1f}%"


def print_actual_delete_cost_summary(
    actual_billed_bytes: int,
    actual_cost_usd: Optional[float],
    rate_usd_per_gib: Optional[float],
    estimated_bytes: int,
    estimated_cost: Optional[float],
    cost_currency: str,
) -> None:
    """
    Display actual billing cost from dt.system.events after deletion completes.
    """
    print("\nActual query cost from dt.system.events:")
    print(
        f"  Billed bytes: {actual_billed_bytes:,} ({format_bytes(actual_billed_bytes)})"
    )
    
    variance_percent, variance_str = calculate_cost_variance(
        estimated_bytes,
        estimated_cost,
        actual_billed_bytes,
        actual_cost_usd,
    )
    
    if rate_usd_per_gib is None:
        print("  Rate card: not configured")
        print("  Actual cost: unknown")
    elif actual_cost_usd is None:
        print(f"  Rate card: {format_currency(rate_usd_per_gib, cost_currency)} per GiB scanned")
        print("  Actual cost: could not calculate")
    else:
        print(f"  Rate card: {format_currency(rate_usd_per_gib, cost_currency)} per GiB scanned")
        print(f"  Actual cost: {format_currency(actual_cost_usd, cost_currency)}")
    
    if variance_percent is not None and estimated_cost is not None:
        variance_abs = abs(estimated_cost - (actual_cost_usd or 0))
        print(
            f"  Variance from estimate: {variance_str} "
            f"({format_currency(variance_abs, cost_currency)})"
        )


def build_chunk_estimates(
    base_url: str,
    token: str,
    delete_query: str,
    chunks: list[tuple[str, str]],
    validation_interval_seconds: int = 2,
) -> list[dict]:
    chunk_metrics: list[dict] = []
    observed_chunks: list[dict] = []

    print("Analyzing chunk workload (records/bytes) for estimation...")
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        chunk_duration_hours = (parse_iso8601(chunk_end) - parse_iso8601(chunk_start)).total_seconds() / 3600
        print(f"  Chunk {i}/{len(chunks)} ({chunk_duration_hours:.1f}h): {chunk_start[:10]} to {chunk_end[:10]}")

        records_count, scanned_bytes = query_chunk_record_count(base_url, token, delete_query, chunk_start, chunk_end)
        sample_count, sample_bytes = query_chunk_size_estimate(
            base_url,
            token,
            delete_query,
            chunk_start,
            chunk_end,
            sample_limit=1000,
        )

        bytes_est = -1
        if records_count == 0:
            bytes_est = 0
        elif records_count > 0 and sample_count > 0 and sample_bytes >= 0:
            avg_record_size = sample_bytes / sample_count
            bytes_est = int(records_count * avg_record_size)

        est_delete_seconds = estimate_delete_seconds(records_count, bytes_est, observed_chunks)
        est_combined_seconds = estimate_combined_seconds(
            est_delete_seconds,
            observed_chunks,
            default_overhead_seconds=max(2.0, float(validation_interval_seconds)),
        )

        chunk_metrics.append(
            {
                "index": i,
                "start": chunk_start,
                "end": chunk_end,
                "records": records_count,
                "scanned_bytes": scanned_bytes,
                "bytes_est": bytes_est,
                "sample_count": sample_count,
                "sample_bytes": sample_bytes,
                "est_delete_seconds": est_delete_seconds,
                "est_combined_seconds": est_combined_seconds,
                "deleted": False,
                "delete_seconds": -1.0,
                "combined_seconds": -1.0,
            }
        )

    return chunk_metrics


def print_final_chunk_report(chunk_metrics: list[dict]):
    print("\nFinal chunk execution report:")
    print("Idx | Status  | Est. Delete | Real Delete | Est. Combined | Real Combined")
    print("----+---------+-------------+-------------+---------------+--------------")
    for metric in chunk_metrics:
        status = "OK" if metric.get("deleted") else "FAILED"
        est_delete = format_duration(metric.get("est_delete_seconds", -1.0))
        real_delete = format_duration(metric.get("delete_seconds", -1.0))
        est_combined = format_duration(metric.get("est_combined_seconds", -1.0))
        real_combined = format_duration(metric.get("combined_seconds", -1.0))
        print(
            f"{metric['index']:>3} | {status:<7} | {est_delete:>11} | {real_delete:>11} | "
            f"{est_combined:>13} | {real_combined:>12}"
        )


def delete_records_in_chunks(
    base_url: str,
    token: str,
    delete_query: str,
    tf_start: str,
    tf_end: str,
    validation_retries: int = 1,
    validation_interval_seconds: int = 2,
    precomputed_chunks: Optional[list[tuple[str, str]]] = None,
    precomputed_metrics: Optional[list[dict]] = None,
) -> bool:
    """
    Delete records in 24-hour chunks to work around Dynatrace deletion API limits.
    Works backwards from tf_end to tf_start.
    """
    if precomputed_chunks is not None:
        chunks = precomputed_chunks
    else:
        try:
            chunks = calculate_24h_chunks(tf_start, tf_end)
        except ValueError as e:
            print(f"Chunk calculation failed: {e}")
            return False

    print(f"\nWill delete in {len(chunks)} chunks of 24 hours each:")
    if precomputed_metrics is not None and len(precomputed_metrics) == len(chunks):
        chunk_metrics = precomputed_metrics
    else:
        chunk_metrics = build_chunk_estimates(
            base_url,
            token,
            delete_query,
            chunks,
            validation_interval_seconds=validation_interval_seconds,
        )
        print_chunk_estimate_table(chunk_metrics)
        print_chunk_estimate_totals(chunk_metrics)

    observed_chunks: list[dict] = []

    failed_chunks = []
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        metric = chunk_metrics[i - 1]
        try:
            remaining_delete_est = sum(
                m["est_delete_seconds"]
                for m in chunk_metrics[i - 1:]
            )
            remaining_combined_est = sum(
                m["est_combined_seconds"]
                for m in chunk_metrics[i - 1:]
            )
            print(
                f"\n[{i}/{len(chunks)}] Deleting chunk {i}... "
                f"(remaining est delete={format_duration(remaining_delete_est)}, "
                f"combined={format_duration(remaining_combined_est)})"
            )

            delete_started = time.monotonic()
            deleted = delete_records_in_grail(base_url, token, delete_query, chunk_start, chunk_end)
            delete_elapsed = time.monotonic() - delete_started

            post_check_started = time.monotonic()
            if deleted:
                # Quick per-chunk post-check to approximate end-to-end completion time.
                validate_records_deleted(
                    base_url,
                    token,
                    delete_query,
                    chunk_start,
                    chunk_end,
                    retries=max(1, validation_retries),
                    interval_seconds=max(1, validation_interval_seconds),
                )
            post_check_elapsed = time.monotonic() - post_check_started
            combined_elapsed = delete_elapsed + post_check_elapsed

            metric["delete_seconds"] = delete_elapsed
            metric["combined_seconds"] = combined_elapsed
            metric["deleted"] = bool(deleted)

            if deleted:
                print(
                    f"[{i}/{len(chunks)}] Chunk {i} deleted successfully. "
                    f"Observed delete={format_duration(delete_elapsed)}, "
                    f"combined={format_duration(combined_elapsed)}"
                )

                observed_chunks.append(metric)
                for future_metric in chunk_metrics[i:]:
                    future_est_delete = estimate_delete_seconds(
                        future_metric.get("records", -1),
                        future_metric.get("bytes_est", -1),
                        observed_chunks,
                    )
                    future_metric["est_delete_seconds"] = future_est_delete
                    future_metric["est_combined_seconds"] = estimate_combined_seconds(
                        future_est_delete,
                        observed_chunks,
                        default_overhead_seconds=max(2.0, float(validation_interval_seconds)),
                    )
            else:
                print(f"[{i}/{len(chunks)}] Chunk {i} delete did not complete.")
                failed_chunks.append(i)
        except KeyboardInterrupt:
            print(f"\nInterrupted by user during chunk {i}/{len(chunks)}.")
            print("Rerun with --cleanup to continue; completed chunks are safe to repeat.")
            return False
        except Exception as e:
            print(f"[{i}/{len(chunks)}] Chunk {i} delete failed: {e}")
            failed_chunks.append(i)

    successful = [m for m in chunk_metrics if m.get("deleted")]
    if successful:
        obs_delete = sum(m.get("delete_seconds", 0.0) for m in successful)
        obs_combined = sum(m.get("combined_seconds", 0.0) for m in successful)
        obs_records = sum(max(0, m.get("records", -1)) for m in successful)
        obs_bytes = sum(max(0, m.get("bytes_est", -1)) for m in successful)
        rec_rate = (obs_records / obs_delete) if obs_delete > 0 and obs_records > 0 else 0.0
        byte_rate = (obs_bytes / obs_delete) if obs_delete > 0 and obs_bytes > 0 else 0.0
        print("\nObserved performance summary:")
        print(
            f"  Successful chunks: {len(successful)}/{len(chunk_metrics)} | "
            f"Delete total: {format_duration(obs_delete)} | "
            f"Combined total: {format_duration(obs_combined)}"
        )
        if rec_rate > 0:
            print(f"  Throughput: {rec_rate:,.2f} records/s")
        if byte_rate > 0:
            print(f"  Throughput: {format_bytes(int(byte_rate))}/s")

    print_final_chunk_report(chunk_metrics)

    if failed_chunks:
        print(f"\n⚠️  {len(failed_chunks)} chunk(s) failed: {failed_chunks}")
        return False

    print(f"\n✓ All {len(chunks)} chunks deleted successfully.")
    return True


def confirm_hard_delete_chunked(
    delete_query: str,
    tf_start: str,
    tf_end: str,
    out_path: str,
    chunk_count: int,
    estimated_cost_usd: Optional[float],
    total_scanned_bytes: int,
    rate_usd_per_gib: Optional[float],
    cost_currency: str,
) -> bool:
    print("\nCSV export completed.")
    print(f"Local file retained: {out_path}")
    print("Hard delete will permanently remove matching records from Grail.")
    print(f"Delete timeframe: {tf_start} -> {tf_end}")
    print(f"Delete will proceed in {chunk_count} chunks (24 hours each) due to API limits.")
    if total_scanned_bytes > 0:
        print(f"Estimated scanned bytes for this delete workflow: {format_bytes(total_scanned_bytes)}")
    if rate_usd_per_gib is not None and estimated_cost_usd is not None:
        print(
            f"Estimated query cost at configured rate ({format_currency(rate_usd_per_gib, cost_currency)}/GiB): "
            f"{format_currency(estimated_cost_usd, cost_currency)}"
        )
    elif rate_usd_per_gib is not None:
        print("Configured rate card is present, but estimated cost is unknown (missing scanned_bytes).")
    else:
        print("No rate card configured; estimated query cost is unknown.")
    print("Review the estimate above before deciding.")
    print(f"Delete query: {delete_query}")
    try:
        answer = input("Proceed with hard delete in Grail? [y/N]: ").strip().lower()
    except EOFError:
        print("No interactive input available; skipping hard delete.")
        return False
    return answer in ("y", "yes")


def confirm_delete_window_mismatch(
    export_tf_start: str,
    export_tf_end: str,
    delete_tf_start: str,
    delete_tf_end: str,
) -> bool:
    print("\n⚠️  WARNING: Deletion window extends beyond export window!")
    print(f"   Export window: {export_tf_start} to {export_tf_end}")
    print(f"   Delete window: {delete_tf_start} to {delete_tf_end}")
    if parse_iso8601(delete_tf_start) < parse_iso8601(export_tf_start):
        print(f"   ❌ Deleting data BEFORE export start: {delete_tf_start} < {export_tf_start}")
    if parse_iso8601(delete_tf_end) > parse_iso8601(export_tf_end):
        print(f"   ❌ Deleting data AFTER export end: {delete_tf_end} > {export_tf_end}")
    print("   You will delete records that were never downloaded to CSV!")

    try:
        answer = input("Proceed anyway? Type 'yes' to continue: ").strip().lower()
    except EOFError:
        print("No interactive input available; skipping hard delete.")
        return False

    return answer in ("y", "yes")


def resolve_out_path(base_out: str, tz: tzinfo = timezone.utc) -> tuple[str, int]:
    """
    Derive a timestamped output path and detect how many previous runs exist.
    e.g. grail_logs.csv -> grail_logs_20260319_160023.csv  (run #3)
    """
    p = Path(base_out)
    stem = p.stem
    suffix = p.suffix or ".csv"
    parent = p.parent

    timestamp = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    new_name = f"{stem}_{timestamp}{suffix}"
    new_path = str(parent / new_name) if str(parent) != "." else new_name

    # Count existing files matching the base stem pattern
    pattern = f"{stem}_*{suffix}"
    existing = sorted(parent.glob(pattern)) if str(parent) != "." else sorted(Path(".").glob(pattern))
    run_number = len(existing) + 1

    return new_path, run_number


def save_json_records_to_csv(records, out_file):
    if not isinstance(records, list):
        raise RuntimeError("expected list of records")

    # unified set of header fields
    all_keys = set()
    for r in records:
        if isinstance(r, dict):
            all_keys.update(r.keys())

    headers = sorted(all_keys)
    if not headers:
        headers = ["data"]

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            if not isinstance(r, dict):
                writer.writerow({"data": json.dumps(r, ensure_ascii=False)})
                continue
            row = {}
            for k in headers:
                v = r.get(k)
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v, ensure_ascii=False)
                else:
                    row[k] = v
            writer.writerow(row)


def generate_execution_report(
    out_path: str,
    query: str,
    delete_query: str,
    tf_start: str,
    tf_end: str,
    cleanup_mode: bool,
    estimated_bytes: Optional[int] = None,
    estimated_cost_usd: Optional[float] = None,
    actual_billed_bytes: Optional[int] = None,
    actual_cost_usd: Optional[float] = None,
    cost_currency: str = "USD",
    cost_rate: Optional[float] = None,
    record_count: int = 0,
    deleted_record_count: int = 0,
    chunks_processed: int = 0,
    duration_seconds: float = 0,
) -> None:
    """Generate a timestamped execution report markdown file."""
    try:
        # Create report filename with timestamp
        report_dt = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        report_filename = f"EXECUTION_REPORT_{report_dt}.md"
        
        # Get actual CSV file size from disk
        csv_path = Path(out_path)
        download_size = csv_path.stat().st_size if csv_path.exists() else 0
        
        # Count actual records in CSV (skip header)
        actual_record_count = 0
        if csv_path.exists():
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    actual_record_count = sum(1 for _ in f) - 1  # Subtract header row
            except Exception:
                actual_record_count = record_count
        else:
            actual_record_count = record_count
        
        # Calculate GiB from bytes
        bytes_to_gib = 1024 ** 3
        estimated_gib = estimated_bytes / bytes_to_gib if estimated_bytes else 0
        actual_gib = actual_billed_bytes / bytes_to_gib if actual_billed_bytes else 0
        
        # Calculate variance if both estimates and actuals exist
        variance_pct = None
        if estimated_bytes and actual_billed_bytes:
            variance_pct = ((actual_billed_bytes - estimated_bytes) / estimated_bytes) * 100
        
        # Build report content
        report = f"""# Execution Report

**Generated:** {datetime.now(tz=timezone.utc).isoformat()}  
**Status:** ✅ Completed Successfully  

---

## Overview

| Parameter | Value |
| --- | --- |
| **Mode** | {'Export + Cleanup' if cleanup_mode else 'Export Only'} |
| **CSV File** | `{Path(out_path).name}` |
| **CSV Size** | {download_size:,} bytes ({download_size / 1024 / 1024:.2f} MB) |
| **Records Exported** | {actual_record_count:,} |
| **Records Deleted** | {deleted_record_count:,} |
| **Duration** | {duration_seconds:.1f} seconds |

---

## Query Details

**Export Query:**
```dql
{query}
```

**Delete Query:**
```dql
{delete_query if cleanup_mode else 'N/A'}
```

---

## Data Metrics

| Metric | Value |
| --- | --- |
| **Total Records** | {actual_record_count:,} |
| **Bytes Scanned** | {estimated_bytes or 'N/A':,} bytes ({estimated_gib:.5f} GiB) |
| **Bytes Deleted** | {actual_billed_bytes or estimated_bytes or 'N/A':,} bytes ({actual_gib:.5f} GiB) |

---

## Cost Analysis - Log Query

| Metric | Value |
| --- | --- |
| **Rate Card** | {cost_rate or 'N/A'} {cost_currency}/GiB |
| **Estimated Cost** | {cost_currency} {estimated_cost_usd or 0:.8f} |
| **Actual Cost** | {cost_currency} {actual_cost_usd or 'N/A'} |
| **Variance** | {f'{variance_pct:+.1f}%' if variance_pct is not None else 'N/A'} |

---

## Execution Details

- **Time Window:** {tf_start} to {tf_end}
- **Cleanup Enabled:** {'Yes' if cleanup_mode else 'No'}
- **Chunks Processed:** {chunks_processed}
- **Duration:** {duration_seconds:.1f}s

---

*Report generated automatically by grail_query_to_csv.py*
"""
        
        # Write report to file
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report)
        
        print(f"\n✅ Execution report saved: {report_filename}")
    
    except Exception as e:
        # Non-blocking: report generation failure should not fail the entire script
        print(f"\n⚠️  Could not generate execution report: {e}")


def main():
    ensure_python_version(3, 9)
    load_env(".env")
    
    # Track execution start time for report generation
    execution_start_time = time.time()

    parser = argparse.ArgumentParser(
        description="Grail logs query -> CSV with chunk deletion time estimation"
    )
    parser.add_argument("--environment", required=False, help="Tenant URL or ID (fallback: DT_ENVIRONMENT)")
    parser.add_argument("--token", required=False, help="Bearer token (fallback: DT_TOKEN)")
    parser.add_argument("--query", required=False, help="Grail query expression (fallback: DT_QUERY)")
    parser.add_argument("--delete-query", required=False,
                        help="DQL for hard delete in Grail (fallback: DT_DELETE_QUERY; defaults to --query/DT_QUERY)")
    parser.add_argument("--from", dest="from_ts", default=None,
                        help="RFC3339 UTC with Z (fallback: DT_FROM, or 7 months ago)")
    parser.add_argument("--to", dest="to_ts", default=None,
                        help="RFC3339 UTC with Z (fallback: DT_TO, or now)")
    parser.add_argument("--delete-from", dest="delete_from_ts", default=None,
                        help="RFC3339 UTC with Z for deletion window start (fallback: DT_DELETE_FROM, defaults to --from)")
    parser.add_argument("--delete-to", dest="delete_to_ts", default=None,
                        help="RFC3339 UTC with Z for deletion window end (fallback: DT_DELETE_TO, defaults to --to)")
    parser.add_argument("--out", default=os.getenv("DT_OUT", "grail_logs.csv"),
                        help="CSV output path (fallback: DT_OUT)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Prompt and hard-delete matching Grail records after CSV export (prefers DT_CLEANUP true/false)")
    parser.add_argument("--dry-run-delete", action="store_true",
                        help="Analyze delete workload and validate permissions, but do not send delete requests")
    parser.add_argument("--validate-config", action="store_true",
                        help="Run configuration and permission checks, then exit without exporting/deleting")
    args = parser.parse_args()

    environment = args.environment or os.getenv("DT_ENVIRONMENT")
    token = args.token or os.getenv("DT_TOKEN")
    query = args.query or os.getenv("DT_QUERY")
    delete_query = args.delete_query or os.getenv("DT_DELETE_QUERY") or query
    rate_usd_per_gib, rate_source = load_cost_rate_per_gib(
        os.getenv("DT_LOG_QUERY_COST_RATE_PER_GIB"),
        os.getenv("DT_LOG_QUERY_COST_RATE_USD_PER_GIB"),
    )
    cost_currency = load_cost_currency(os.getenv("DT_LOG_QUERY_COST_CURRENCY"))
    billing_validation_enabled = os.getenv(
        "DT_ENABLE_BILLING_VALIDATION",
        "false",
    ).strip().lower() in ("1", "true", "yes")
    try:
        delete_validate_retries = int(os.getenv("DT_DELETE_VALIDATE_RETRIES", "12"))
    except ValueError:
        delete_validate_retries = 12
    try:
        delete_validate_interval_seconds = int(os.getenv("DT_DELETE_VALIDATE_INTERVAL_SECONDS", "10"))
    except ValueError:
        delete_validate_interval_seconds = 10

    if not environment:
        raise SystemExit("Missing --environment or DT_ENVIRONMENT")
    if not token:
        raise SystemExit("Missing --token or DT_TOKEN")
    if not query:
        raise SystemExit("Missing --query or DT_QUERY")

    # Narrow Optional values to concrete strings for runtime and static analysis.
    environment = environment.strip()
    token = token.strip()
    query = query.strip()
    delete_query = (delete_query or "").strip()

    cleanup_env = os.getenv("DT_CLEANUP", "false").strip().lower() in ("1", "true", "yes")
    cleanup_flag = args.cleanup or cleanup_env
    dry_run_delete_flag = args.dry_run_delete

    if dry_run_delete_flag and cleanup_flag:
        print("--dry-run-delete is enabled; delete requests will not be executed.")

    # Resolve timestamp timezone for CSV filename (API timeframes always stay UTC)
    tz_name = os.getenv("DT_TIMEZONE", "").strip()
    if tz_name:
        try:
            file_tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            print(f"Warning: unknown timezone '{tz_name}', falling back to UTC. Valid example: 'America/Sao_Paulo'")
            file_tz = timezone.utc
    else:
        file_tz = datetime.now().astimezone().tzinfo or timezone.utc  # system local time
    base_url = normalize_environment(environment)
    dt_to_env = os.getenv("DT_TO")
    dt_from_env = os.getenv("DT_FROM")

    if args.to_ts:
        to_ts = parse_iso8601(args.to_ts)
    elif dt_to_env:
        to_ts = parse_iso8601(dt_to_env)
    else:
        to_ts = datetime.now(timezone.utc)

    if args.from_ts:
        from_ts = parse_iso8601(args.from_ts)
    elif dt_from_env:
        from_ts = parse_iso8601(dt_from_env)
    else:
        from_ts = to_ts - timedelta(days=int(365 * 7 / 12))  # ~7 months

    if from_ts >= to_ts:
        raise SystemExit("from must be before to")

    tf_start = from_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "000Z"
    tf_end = to_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "000Z"

    # Parse delete window (optional, defaults to export window)
    dt_delete_from_env = os.getenv("DT_DELETE_FROM")
    dt_delete_to_env = os.getenv("DT_DELETE_TO")

    if args.delete_from_ts:
        delete_from_ts = parse_iso8601(args.delete_from_ts)
    elif dt_delete_from_env:
        delete_from_ts = parse_iso8601(dt_delete_from_env)
    else:
        delete_from_ts = from_ts  # Default to export window

    if args.delete_to_ts:
        delete_to_ts = parse_iso8601(args.delete_to_ts)
    elif dt_delete_to_env:
        delete_to_ts = parse_iso8601(dt_delete_to_env)
    else:
        delete_to_ts = to_ts  # Default to export window

    if delete_from_ts >= delete_to_ts:
        raise SystemExit("delete-from must be before delete-to")

    delete_tf_start = delete_from_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "000Z"
    delete_tf_end = delete_to_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "000Z"

    delete_window_exceeds_export = delete_from_ts < from_ts or delete_to_ts > to_ts

    preflight_ok = run_preflight_checks(
        base_url,
        token,
        query,
        tf_start,
        tf_end,
        cleanup_requested=cleanup_flag,
        dry_run_delete=dry_run_delete_flag,
        delete_query=delete_query,
        delete_tf_end=delete_tf_end,
    )

    if args.validate_config:
        if preflight_ok:
            print("Configuration validation completed successfully.")
            return
        raise SystemExit("Configuration validation failed.")

    if not preflight_ok:
        raise SystemExit("Preflight checks failed.")

    print(f"Running grail query from {tf_start} to {tf_end}")

    result, request_token = run_query(base_url, token, query, tf_start, tf_end)
    records = result.get("result", {}).get("records")

    out_path, run_number = resolve_out_path(args.out, file_tz)
    if run_number > 1:
        print(f"Run #{run_number} (previous run files already exist for this base name)")

    if records is not None:
        records_payload_bytes = estimate_records_json_size_bytes(records)
        print(
            f"Got {len(records):,} records (~{records_payload_bytes:,} bytes payload, "
            f"{format_bytes(records_payload_bytes)}); writing CSV {out_path}"
        )
        save_json_records_to_csv(records, out_path)
        csv_bytes = os.path.getsize(out_path)
        print(f"CSV written: {csv_bytes:,} bytes ({format_bytes(csv_bytes)})")

    elif request_token:
        print("No in-memory records in query result; downloading from query:download endpoint")
        downloaded, downloaded_bytes = download_query_result(base_url, token, request_token, out_path)
        if not downloaded:
            raise SystemExit("Download skipped: missing request token")
        print(
            f"Downloaded CSV to {out_path} "
            f"({downloaded_bytes:,} bytes, {format_bytes(downloaded_bytes)})"
        )

    else:
        raise SystemExit("No query result records found and no request token for download")

    if cleanup_flag or dry_run_delete_flag:
        if not delete_query:
            print("Delete preparation requested, but no delete query provided (use --delete-query or DT_DELETE_QUERY).")
        elif "| limit" in delete_query.lower():
            print("Delete preparation skipped: record deletion query must not contain limit.")
        elif delete_to_ts > (datetime.now(timezone.utc) - timedelta(hours=4)):
            print("Delete preparation skipped: deletion end time must be at least 4 hours in the past.")
        else:
            # Calculate number of 24-hour chunks needed for delete window
            try:
                chunks = calculate_24h_chunks(delete_tf_start, delete_tf_end)
                chunk_count = len(chunks)
            except ValueError as e:
                print(f"Delete preparation skipped: {e}")
                chunks = []
                chunk_count = 0

            if chunk_count > 0:
                if delete_window_exceeds_export:
                    mismatch_ok = confirm_delete_window_mismatch(
                        tf_start,
                        tf_end,
                        delete_tf_start,
                        delete_tf_end,
                    )
                    if not mismatch_ok:
                        print("Remote delete canceled due to window mismatch.")
                        print(f"Local file retained: {out_path}")
                        return

                chunk_metrics = build_chunk_estimates(
                    base_url,
                    token,
                    delete_query,
                    chunks,
                    validation_interval_seconds=max(1, min(5, delete_validate_interval_seconds)),
                )
                print_chunk_estimate_table(chunk_metrics)
                print_chunk_estimate_totals(chunk_metrics)

                total_scanned_bytes, estimated_cost_usd, known_chunks, total_chunks = estimate_delete_query_cost(
                    chunk_metrics,
                    rate_usd_per_gib,
                )
                print_delete_query_cost_summary(
                    total_scanned_bytes,
                    estimated_cost_usd,
                    rate_usd_per_gib,
                    rate_source,
                    known_chunks,
                    total_chunks,
                    cost_currency,
                )

                if dry_run_delete_flag:
                    pre_count = count_records_in_grail(base_url, token, delete_query, delete_tf_start, delete_tf_end)
                    if pre_count >= 0:
                        print(f"Dry-run delete check: {pre_count:,} matching record(s) currently visible.")
                    else:
                        print("Dry-run delete check: could not determine matching record count.")

                    if billing_validation_enabled:
                        # Read-only billing check for the selected timeframe.
                        # This reports billed query usage already recorded by Dynatrace,
                        # not the future cost of the pending delete operation itself.
                        actual_billed_bytes, actual_cost_usd, billing_error = query_actual_delete_cost(
                            base_url,
                            token,
                            delete_query,
                            delete_tf_start,
                            delete_tf_end,
                            rate_usd_per_gib,
                        )
                        if billing_error:
                            print(f"\n⚠️  Dry-run billed cost validation unavailable: {billing_error}")
                        elif actual_billed_bytes is not None:
                            print("\nDry-run billed cost validation (historical):")
                            print_actual_delete_cost_summary(
                                actual_billed_bytes,
                                actual_cost_usd,
                                rate_usd_per_gib,
                                total_scanned_bytes,
                                estimated_cost_usd,
                                cost_currency,
                            )
                            print(
                                "  Note: values above reflect historical query billing events "
                                "already recorded in this timeframe."
                            )
                    else:
                        print(
                            "Dry-run billed cost validation skipped "
                            "(set DT_ENABLE_BILLING_VALIDATION=true to enable)."
                        )

                    print("Dry-run mode: no delete requests were sent.")
                    print(f"Local file retained: {out_path}")
                    return

                confirmed = confirm_hard_delete_chunked(
                    delete_query,
                    delete_tf_start,
                    delete_tf_end,
                    out_path,
                    chunk_count,
                    estimated_cost_usd,
                    total_scanned_bytes,
                    rate_usd_per_gib,
                    cost_currency,
                )
                if not confirmed:
                    print("Remote delete canceled by user.")
                else:
                    # Check how many records exist before deleting
                    pre_count = count_records_in_grail(base_url, token, delete_query, delete_tf_start, delete_tf_end)
                    if pre_count == 0:
                        print("⚠️  No matching records found in Grail for the delete window — nothing to delete.")
                        print("   Data may have already been deleted in a previous run.")
                    else:
                        deleted = delete_records_in_chunks(
                            base_url,
                            token,
                            delete_query,
                            delete_tf_start,
                            delete_tf_end,
                            validation_retries=max(1, min(3, delete_validate_retries)),
                            validation_interval_seconds=max(1, min(5, delete_validate_interval_seconds)),
                            precomputed_chunks=chunks,
                            precomputed_metrics=chunk_metrics,
                        )
                        if deleted:
                            print("Remote Grail records deleted successfully.")
                            validate_records_deleted(
                                base_url,
                                token,
                                delete_query,
                                delete_tf_start,
                                delete_tf_end,
                                retries=max(1, delete_validate_retries),
                                interval_seconds=max(1, delete_validate_interval_seconds),
                            )
                            
                            # Query actual billing cost from dt.system.events (non-blocking)
                            if billing_validation_enabled and (rate_usd_per_gib is not None or estimated_cost_usd is not None):
                                actual_billed_bytes, actual_cost_usd, billing_error = query_actual_delete_cost(
                                    base_url,
                                    token,
                                    delete_query,
                                    delete_tf_start,
                                    delete_tf_end,
                                    rate_usd_per_gib,
                                )
                                if billing_error:
                                    print(f"\n⚠️  {billing_error}")
                                elif actual_billed_bytes is not None:
                                    print_actual_delete_cost_summary(
                                        actual_billed_bytes,
                                        actual_cost_usd,
                                        rate_usd_per_gib,
                                        total_scanned_bytes,
                                        estimated_cost_usd,
                                        cost_currency,
                                    )
                            elif not billing_validation_enabled:
                                print(
                                    "Post-delete billed cost validation skipped "
                                    "(set DT_ENABLE_BILLING_VALIDATION=true to enable)."
                                )
                        else:
                            print("Remote record delete did not complete.")
    else:
        print("Remote Grail data kept (not deleted).")

    print(f"Local file retained: {out_path}")
    
    # Generate execution report
    execution_duration = time.time() - execution_start_time
    generate_execution_report(
        out_path=out_path,
        query=query,
        delete_query=delete_query,
        tf_start=tf_start,
        tf_end=tf_end,
        cleanup_mode=cleanup_flag,
        record_count=record_count if 'record_count' in dir() else 0,
        estimated_bytes=total_scanned_bytes if 'total_scanned_bytes' in dir() else None,
        estimated_cost_usd=estimated_cost_usd if 'estimated_cost_usd' in dir() else None,
        actual_billed_bytes=actual_billed_bytes if 'actual_billed_bytes' in dir() else None,
        actual_cost_usd=actual_cost_usd if 'actual_cost_usd' in dir() else None,
        cost_currency=cost_currency,
        cost_rate=rate_usd_per_gib if 'rate_usd_per_gib' in dir() else None,
        duration_seconds=execution_duration,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        raise SystemExit(130)
