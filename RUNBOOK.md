# Grail Export and Cleanup User Guide

`grail_query_to_csv.py` exports Dynatrace Grail logs to CSV and can optionally delete the same records from Grail.

Use this guide as an operator manual: what to configure, what to run, what to expect, and how to recover safely.

> **IMPORTANT DISCLAIMER - LEGAL NOTICE**
>
> - This script was developed solely to assist **CIELO** with Grail data export and deletion workflows.
> - This script is provided **AS IS**, as a **BEST-EFFORT STARTING POINT**, and is **NOT A DYNATRACE PRODUCT**.
> - Dynatrace provides **NO WARRANTIES**, **NO OFFICIAL SUPPORT**, and **NO VERSIONING OR MAINTENANCE COMMITMENTS** for this script.
> - Record deletion is an existing capability of Dynatrace APIs. This script only demonstrates one possible approach for customer-managed data control in Grail.
> - **CIELO** acknowledges responsibility for installation, customization, implementation, validation, security review, and ongoing operation, with **NO FUTURE SUPPORT COMMITMENT** from Dynatrace.

---

## Table of Contents

1. [What This Tool Does](#1-what-this-tool-does)
2. [Before You Begin](#2-before-you-begin)
3. [Quick Start (Recommended First Run)](#3-quick-start-recommended-first-run)
4. [Configuration (.env)](#4-configuration-env)
5. [Step-by-Step Operating Procedures](#5-step-by-step-operating-procedures)
6. [Safety Rules (Read Before Deleting)](#6-safety-rules-read-before-deleting)
7. [Output Files and Validation](#7-output-files-and-validation)
8. [Long-Range Cleanup (Multi-Day or Multi-Month)](#8-long-range-cleanup-multi-day-or-multi-month)
9. [Console Messages and Their Meaning](#9-console-messages-and-their-meaning)
10. [Troubleshooting](#10-troubleshooting)
11. [API Reference](#11-api-reference)

---

## 1. What This Tool Does

This script has two modes:

1. Export mode: runs a DQL query and writes matching records to a timestamped CSV.
2. Cleanup mode: after export, sends hard-delete requests to Grail for matching records.

Typical safe usage is:

1. Run export-only first.
2. Check the CSV.
3. Enable cleanup only after validating the export result.

---

## 2. Before You Begin

### 2.1 Requirements

| Requirement | Details |
| --- | --- |
| Python | 3.11+ (tested on 3.13) |
| Python package | `requests` |
| Dynatrace token scopes | `storage:logs:read` and `storage:records:delete` |
| Working directory | Folder containing `.env` and `grail_query_to_csv.py` |

### 2.2 First-time setup

```text
cd /path/to/buckets
python3 -m venv .venv
.venv/bin/pip install requests
```

Always run with `.venv/bin/python` so dependencies are consistent.

---

## 3. Quick Start (Recommended First Run)

Follow this exact sequence for a safe first execution.

### Step 1: Create `.env` from `env.txt`

Use `env.txt` as the starter template and create a `.env` file in the same folder.

```text
cp env.txt .env
```

Then edit `.env` and set `DT_ENVIRONMENT`, `DT_TOKEN`, `DT_QUERY`, `DT_FROM`, `DT_TO`, and `DT_OUT`.

### Step 2: Run export only

```text
./.venv/bin/python grail_query_to_csv.py
```

### Step 3: Verify output CSV

Confirm:

1. File was created.
2. Timestamps and columns look correct.
3. Record volume is in expected range.

### Step 4: Enable cleanup only if needed

Use either:

1. `DT_CLEANUP=true` in `.env`, or
2. `--cleanup` on the command line.

### Step 5: Run cleanup

```text
./.venv/bin/python grail_query_to_csv.py --cleanup
```

---

## 4. Configuration (.env)

Use `env.txt` as the starter template to create `.env`. All settings are read from `.env`. Shell environment variables override `.env` values.

### 4.1 Full example

```text
# Required
DT_ENVIRONMENT=https://<tenant-id>.apps.dynatrace.com
DT_TOKEN=dt0s16.<token-value>

# Export query (DQL)
DT_QUERY=fetch logs | filter matchesValue(id, "your-filter")

# Delete query (DQL)
# If not set, script uses DT_QUERY
DT_DELETE_QUERY=fetch logs | filter matchesValue(id, "your-filter")

# Export window
DT_FROM=2026-03-08T00:00:00.000000000Z
DT_TO=2026-03-09T00:00:00.000000000Z

# Delete window (optional)
# If omitted, script uses DT_FROM/DT_TO
DT_DELETE_FROM=2026-03-01T00:00:00.000000000Z
DT_DELETE_TO=2026-03-09T00:00:00.000000000Z

# Output CSV base name
DT_OUT=grail_logs.csv

# Cleanup mode (optional)
DT_CLEANUP=true

# Timestamp timezone for output filename
DT_TIMEZONE=America/Sao_Paulo

# Post-delete validation tuning
DT_DELETE_VALIDATE_RETRIES=12
DT_DELETE_VALIDATE_INTERVAL_SECONDS=10
```

### 4.2 Variable guide

| Variable | Required | Purpose |
| --- | --- | --- |
| `DT_ENVIRONMENT` | Yes | Tenant URL |
| `DT_TOKEN` | Yes | API token |
| `DT_QUERY` | Yes | Export selection query |
| `DT_DELETE_QUERY` | No | Cleanup selection query |
| `DT_FROM`, `DT_TO` | Yes | Export time window |
| `DT_DELETE_FROM`, `DT_DELETE_TO` | No | Cleanup time window |
| `DT_OUT` | Yes | CSV base filename |
| `DT_CLEANUP` | No | Enable cleanup without `--cleanup` |
| `DT_TIMEZONE` | No | Timezone used in output filename |
| `DT_DELETE_VALIDATE_RETRIES` | No | Post-delete check attempts |
| `DT_DELETE_VALIDATE_INTERVAL_SECONDS` | No | Delay between validation attempts |

### 4.3 Timestamp format

All time values must be RFC3339 UTC with nanoseconds:

```text
YYYY-MM-DDTHH:MM:SS.000000000Z
```

Example: `2026-03-08T00:00:00.000000000Z`

---

## 5. Step-by-Step Operating Procedures

### 5.1 Export only (safe baseline)

Use this when validating a query or collecting data without deletion.

```text
./.venv/bin/python grail_query_to_csv.py
```

Expected result:

1. Query executes.
2. CSV file is created with a timestamp suffix.
3. Remote data remains in Grail.

### 5.2 Export and cleanup (same window)

Use this when export and delete windows should match.

```text
./.venv/bin/python grail_query_to_csv.py --cleanup
```

Expected result:

1. Export runs first.
2. Script validates delete safety conditions.
3. Matching records are deleted in 24-hour chunks.
4. Post-delete validation checks for remaining records.

### 5.3 Export one window, cleanup a different window

If delete window is wider than export window, script will warn and require explicit confirmation.

Use this only when intentional.

### 5.4 Override values from CLI

```text
--environment   Tenant URL or ID
--token         Bearer token
--query         DQL export query
--delete-query  DQL delete query (must not contain limit)
--from          Export start
--to            Export end
--delete-from   Delete start
--delete-to     Delete end
--out           CSV output path
--cleanup       Enable hard delete
```

Example:

```text
./.venv/bin/python grail_query_to_csv.py \
  --from 2026-01-01T00:00:00.000000000Z \
  --to   2026-01-02T00:00:00.000000000Z
```

### 5.5 Clear stale shell overrides

If values from previous runs are still active in your shell, clear them:

```text
unset DT_FROM DT_TO DT_DELETE_FROM DT_DELETE_TO DT_QUERY DT_DELETE_QUERY
```

---

## 6. Safety Rules (Read Before Deleting)

The script applies multiple protections to reduce accidental data loss.

### 6.1 Delete end time must be at least 4 hours in the past

If too recent, cleanup is skipped.

### 6.2 Delete query must not contain `| limit`

If found, cleanup is skipped to avoid partial deletion.

### 6.3 Window mismatch warning

If cleanup window extends outside export window, script shows a warning and asks for confirmation.

### 6.4 Pre-delete existence check

Script checks if any records still match before delete API calls.

If no records match, cleanup stops safely.

### 6.5 Post-delete validation

After all chunks complete, script re-queries with `| limit 1` for up to `DT_DELETE_VALIDATE_RETRIES` attempts.

---

## 7. Output Files and Validation

Each run creates a new timestamped CSV file.

Example sequence:

```text
grail_logs.csv
grail_logs_20260315_143012.csv
grail_logs_20260316_090511.csv
grail_logs_20260319_174822.csv
```

Validation checklist:

1. File exists and is not empty.
2. Row count is plausible.
3. Fields/columns match expected schema.
4. Time range in CSV matches selected window.
5. Reported payload/download size is within expected limits.

Note: nested JSON objects and arrays are serialized as JSON strings in CSV cells.

---

## 8. Long-Range Cleanup (Multi-Day or Multi-Month)

Delete API accepts a maximum of 24 hours per call.

For long periods, script automatically splits into 24-hour chunks and runs them sequentially.

Recommended operational pattern:

1. Export a short sample first (1-2 days).
2. Validate query correctness.
3. Expand `DT_DELETE_FROM` and `DT_DELETE_TO` to full target range.
4. Run with cleanup enabled.
5. Monitor chunk-by-chunk progress.

If interrupted (`Ctrl+C`), rerun command. Completed chunks are safe to repeat.

Resume command:

```text
./.venv/bin/python grail_query_to_csv.py
```

---

## 9. Console Messages and Their Meaning

| Message | Meaning |
| --- | --- |
| `Running grail query from ... to ...` | Export started |
| `Got N records (~X bytes payload, Y); writing CSV ...` | Records returned inline with estimated payload size |
| `No in-memory records in query result; downloading from query:download endpoint` | Large result streaming download |
| `CSV written: X bytes (Y)` | Export completed with final CSV file size |
| `Downloaded CSV to ... (X bytes, Y)` | Streaming download completed with transferred size |
| `WARNING: Deletion window extends beyond export window` | Delete range wider than export range |
| `Will delete in N chunks of 24 hours each` | Cleanup plan shown |
| `[1/3] Deleting chunk 1...` | Chunk in progress |
| `[1/3] Chunk 1 deleted successfully.` | Chunk finished |
| `No matching records found - nothing to delete.` | Nothing left to delete |
| `Post-delete validation passed on attempt N` | No matching records found |
| `Remote Grail data kept (not deleted).` | Cleanup mode not enabled |

### 9.1 Full Console Examples (Counts, Bytes, and Warning)

Example A: inline records with estimated payload bytes and final CSV size.

```text
python3 grail_query_to_csv.py --cleanup
Running grail query from 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
Run #5 (previous run files already exist for this base name)
Got 2,160 records (~1,555,200 bytes payload, 1.48 MB); writing CSV grail_logs_20260320_103910.csv
CSV written: 840,506 bytes (820.81 KB)
```

Example B: export window is smaller than delete window (warning + confirmation).

```text
python3 grail_query_to_csv.py --cleanup
Running grail query from 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
Run #5 (previous run files already exist for this base name)
Got 2,160 records (~1,555,200 bytes payload, 1.48 MB); writing CSV grail_logs_20260320_103910.csv
CSV written: 840,506 bytes (820.81 KB)

⚠️  WARNING: Deletion window extends beyond export window!
  Export window: 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
  Delete window: 2026-03-01T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
  ❌ Deleting data BEFORE export start: 2026-03-01T00:00:00.000000Z < 2026-03-02T00:00:00.000000Z
  You will delete records that were never downloaded to CSV!
Proceed anyway? Type 'yes' to continue:
```

Example C: no inline records; streamed CSV download with transferred byte count.

```text
python3 grail_query_to_csv.py
Running grail query from 2026-03-01T00:00:00.000000Z to 2026-03-02T00:00:00.000000Z
No in-memory records in query result; downloading from query:download endpoint
Downloaded CSV to grail_logs_20260320_110001.csv (145,331,002 bytes, 138.60 MB)
```

### 9.2 Decision Checklist for Warning: Deletion Window Mismatch

Use this flow when you see:
`WARNING: Deletion window extends beyond export window!`

1. Compare export and delete windows shown in the console.
2. If delete window is wider than export window, do not type `yes` yet.
3. Expand export window (`DT_FROM`/`DT_TO`) to fully cover intended delete period, then export again.
4. Or narrow delete window (`DT_DELETE_FROM`/`DT_DELETE_TO`) so it is fully inside exported period.
5. Re-run export and confirm record count and byte size are within expected limits.
6. Run cleanup again and type `yes` only when delete window is fully aligned with exported data.

---

## 10. Troubleshooting

### 10.1 `ModuleNotFoundError: No module named 'requests'`

Cause: wrong Python interpreter.

Fix:

```text
./.venv/bin/python grail_query_to_csv.py
```

Or install package:

```text
python3 -m pip install requests
```

### 10.2 `0 records returned` but records exist in Grail notebook

Check:

1. Exact UTC time window in `DT_FROM` and `DT_TO`.
2. Local timezone conversion differences.
3. Stale shell overrides.

Clear overrides if needed:

```text
unset DT_FROM DT_TO
```

### 10.3 `Cleanup skipped: deletion end time must be at least 4 hours in the past`

Set `DT_DELETE_TO` to at least 4 hours before current time.

### 10.4 `delete execute status 400: exceeds maximum duration`

Cause: delete call window exceeded 24 hours.

Fix: use exact RFC3339 boundaries with `.000000000Z`. Script chunking is designed to enforce this.

### 10.5 `Transient delete status polling error (N/5)`

Cause: temporary network issue during status polling.

Behavior: script retries automatically up to 5 times.

### 10.6 Post-delete validation does not pass

Grail replication may be delayed.

Increase validation retries and interval:

```text
DT_DELETE_VALIDATE_RETRIES=30
DT_DELETE_VALIDATE_INTERVAL_SECONDS=30
```

---

## 11. API Reference

Swagger UI reference:
Use your own tenant ID for validation and verification: `https://YOUR_TENANT_ID.apps.dynatrace.com/platform/swagger-ui/index.html?urls.primaryName=Grail+-+Storage+Record+Deletion`

| Operation | Endpoint | Method |
| --- | --- | --- |
| Execute query | `/platform/storage/query/v2/query:execute` | POST |
| Poll query | `/platform/storage/query/v2/query:poll` | GET |
| Download result | `/platform/storage/query/v1/query:download` | GET |
| Execute delete | `/platform/storage/record/v1/delete:execute` | POST |
| Poll delete status | `/platform/storage/record/v1/delete:status` | POST |

Query API notes:

1. Script tries v2 first, then falls back to v1.
2. Max result records: 50,000,000.
3. Poll interval: 2 seconds.

Delete API notes:

1. Returns HTTP 202 and `taskId` immediately.
2. Maximum delete window per call is 24 hours.
3. Delete end time must be at least 4 hours in the past.
4. Time values must use UTC and `Z` suffix.
