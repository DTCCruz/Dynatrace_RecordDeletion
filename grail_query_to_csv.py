#!/usr/bin/env python3

import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
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


def normalize_environment(env: str) -> str:
    env = env.strip().rstrip("/")
    if env.startswith("http://") or env.startswith("https://"):
        return env

    # If the environment already looks like a full domain name (contains a dot),
    # keep it as-is. For short IDs, default to .apps.dynatrace.com.
    if "." in env:
        return "https://" + env

    return f"https://{env}.apps.dynatrace.com"


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
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    # Remove nanoseconds if present (keep only microseconds)
    if "." in ts_str:
        parts = ts_str.split(".")
        if len(parts[1]) > 6:
            # Truncate to microseconds
            ts_str = f"{parts[0]}.{parts[1][:6]}+00:00"
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


def delete_records_in_chunks(
    base_url: str, token: str, delete_query: str, tf_start: str, tf_end: str
) -> bool:
    """
    Delete records in 24-hour chunks to work around Dynatrace deletion API limits.
    Works backwards from tf_end to tf_start.
    """
    try:
        chunks = calculate_24h_chunks(tf_start, tf_end)
    except ValueError as e:
        print(f"Chunk calculation failed: {e}")
        return False

    print(f"\nWill delete in {len(chunks)} chunks of 24 hours each:")
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        chunk_duration_hours = (parse_iso8601(chunk_end) - parse_iso8601(chunk_start)).total_seconds() / 3600
        print(f"  Chunk {i}/{len(chunks)} ({chunk_duration_hours:.1f}h): {chunk_start[:10]} to {chunk_end[:10]}")

    failed_chunks = []
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        try:
            print(f"\n[{i}/{len(chunks)}] Deleting chunk {i}...")
            deleted = delete_records_in_grail(base_url, token, delete_query, chunk_start, chunk_end)
            if deleted:
                print(f"[{i}/{len(chunks)}] Chunk {i} deleted successfully.")
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

    if failed_chunks:
        print(f"\n⚠️  {len(failed_chunks)} chunk(s) failed: {failed_chunks}")
        return False

    print(f"\n✓ All {len(chunks)} chunks deleted successfully.")
    return True


def confirm_hard_delete_chunked(delete_query: str, tf_start: str, tf_end: str, out_path: str, chunk_count: int) -> bool:
    print("\nCSV export completed.")
    print(f"Local file retained: {out_path}")
    print("Hard delete will permanently remove matching records from Grail.")
    print(f"Delete timeframe: {tf_start} -> {tf_end}")
    print(f"Delete will proceed in {chunk_count} chunks (24 hours each) due to API limits.")
    print(f"Delete query: {delete_query}")
    try:
        answer = input("Proceed with hard delete in Grail? Type 'yes' to continue: ").strip().lower()
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


def main():
    load_env(".env")

    parser = argparse.ArgumentParser(description="Minimal Grail logs query -> CSV (7 months default)")
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
    args = parser.parse_args()

    environment = args.environment or os.getenv("DT_ENVIRONMENT")
    token = args.token or os.getenv("DT_TOKEN")
    query = args.query or os.getenv("DT_QUERY")
    delete_query = args.delete_query or os.getenv("DT_DELETE_QUERY") or query
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

    cleanup_env = os.getenv("DT_CLEANUP", "false").strip().lower() in ("1", "true", "yes")
    cleanup_flag = args.cleanup or cleanup_env

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
    token = token.strip()
    dt_to_env = os.getenv("DT_TO")
    dt_from_env = os.getenv("DT_FROM")

    if args.to_ts:
        to_ts = datetime.fromisoformat(args.to_ts.replace("Z", "+00:00"))
    elif dt_to_env:
        to_ts = datetime.fromisoformat(dt_to_env.replace("Z", "+00:00"))
    else:
        to_ts = datetime.now(timezone.utc)

    if args.from_ts:
        from_ts = datetime.fromisoformat(args.from_ts.replace("Z", "+00:00"))
    elif dt_from_env:
        from_ts = datetime.fromisoformat(dt_from_env.replace("Z", "+00:00"))
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
        delete_from_ts = datetime.fromisoformat(args.delete_from_ts.replace("Z", "+00:00"))
    elif dt_delete_from_env:
        delete_from_ts = datetime.fromisoformat(dt_delete_from_env.replace("Z", "+00:00"))
    else:
        delete_from_ts = from_ts  # Default to export window

    if args.delete_to_ts:
        delete_to_ts = datetime.fromisoformat(args.delete_to_ts.replace("Z", "+00:00"))
    elif dt_delete_to_env:
        delete_to_ts = datetime.fromisoformat(dt_delete_to_env.replace("Z", "+00:00"))
    else:
        delete_to_ts = to_ts  # Default to export window

    if delete_from_ts >= delete_to_ts:
        raise SystemExit("delete-from must be before delete-to")

    delete_tf_start = delete_from_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "000Z"
    delete_tf_end = delete_to_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "000Z"

    delete_window_exceeds_export = delete_from_ts < from_ts or delete_to_ts > to_ts

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

    if cleanup_flag:
        if not delete_query:
            print("Cleanup requested, but no delete query provided (use --delete-query or DT_DELETE_QUERY).")
        elif "| limit" in delete_query.lower():
            print("Cleanup skipped: record deletion query must not contain limit.")
        elif delete_to_ts > (datetime.now(timezone.utc) - timedelta(hours=4)):
            print("Cleanup skipped: deletion end time must be at least 4 hours in the past.")
        else:
            # Calculate number of 24-hour chunks needed for delete window
            try:
                chunks = calculate_24h_chunks(delete_tf_start, delete_tf_end)
                chunk_count = len(chunks)
            except ValueError as e:
                print(f"Cleanup skipped: {e}")
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

                confirmed = confirm_hard_delete_chunked(delete_query, delete_tf_start, delete_tf_end, out_path, chunk_count)
                if not confirmed:
                    print("Remote delete canceled by user.")
                else:
                    # Check how many records exist before deleting
                    pre_count = count_records_in_grail(base_url, token, delete_query, delete_tf_start, delete_tf_end)
                    if pre_count == 0:
                        print("⚠️  No matching records found in Grail for the delete window — nothing to delete.")
                        print("   Data may have already been deleted in a previous run.")
                    else:
                        deleted = delete_records_in_chunks(base_url, token, delete_query, delete_tf_start, delete_tf_end)
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
                        else:
                            print("Remote record delete did not complete.")
    else:
        print("Remote Grail data kept (not deleted).")

    print(f"Local file retained: {out_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        raise SystemExit(130)
