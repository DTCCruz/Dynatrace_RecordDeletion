# Dynatrace Grail Bucket Management

This repository contains tools and documentation for managing Dynatrace Grail data and generating customer-ready runbooks.

## Cost Disclaimer

Important: while this toolkit is provided as a best-effort helper, using capabilities that query Dynatrace Grail data can incur additional costs based on your Dynatrace consumption model.

- This impacts query-heavy flows, including export, dry-run analysis, and delete validation steps.
- Cost is primarily driven by scanned volume (GiB scanned).
- Configure `DT_LOG_QUERY_COST_RATE_PER_GIB` in `.env` to match your contract rate and review estimates before confirming deletion.
- The script shows a pre-delete estimate and, after successful delete, attempts a non-blocking post-delete comparison using billing events.

## Overview

This project provides:
- **Grail Data Export**: Python-based tool to query Dynatrace Grail and export data to CSV
- **Runbook Preparation**: Shell script to customize runbooks for customer delivery
- **Customer Documentation**: Multilingual runbooks available in English, Brazilian Portuguese, and Spanish

Dynatrace record deletion in Grail is a native platform capability available through the Dynatrace Record Deletion API. The scripts in this repository do not create a new deletion feature; they provide an operational wrapper around the existing Dynatrace API to help export, validate, and optionally delete matching records in a controlled workflow. For the native platform documentation, see [Record deletion in Grail via API](https://docs.dynatrace.com/docs/platform/grail/organize-data/record-deletion-in-grail). For API endpoint reference in Swagger UI, use your own tenant ID for validation and verification: `https://YOUR_TENANT_ID.apps.dynatrace.com/platform/swagger-ui/index.html?urls.primaryName=Grail+-+Storage+Record+Deletion`.

## Components

### Core Files

- **`grail_query_to_csv.py`** - Python script that queries Dynatrace Grail API and exports results to CSV format. Supports environment configuration via `.env` file and command-line arguments for customization.

- **`RUNBOOK.md`** - English-language runbook with operational procedures and guidelines.

- **`RUNBOOK_BR.md`** - Brazilian Portuguese version of the runbook.

- **`RUNBOOK_ES.md`** - Spanish version of the runbook.

- **`env.txt`** - Template file for environment variable configuration.

### Utility Scripts

- **`prepare_customer_runbooks.sh`** - Bash script to customize runbooks by replacing placeholders (e.g., `{{CUSTOMER_NAME}}`) with actual customer information. Supports batch processing and modular scope selection.

## Usage

### Exporting Grail Data

```bash
# Ensure dependencies are installed
python3 -m pip install requests

# Validate configuration and token permissions before execution
python3 grail_query_to_csv.py --validate-config

# Run the export script (export only, no deletion)
python3 grail_query_to_csv.py [options]
```

### Deleting Records from Grail

The script provides a `--cleanup` flag to delete matching records after exporting them to CSV. Deletion uses the native [Dynatrace Record Deletion API](https://docs.dynatrace.com/docs/platform/grail/organize-data/record-deletion-in-grail).

#### Prerequisites for Deletion

1. **API Token Permissions**: The token must have the `storage:buckets:delete` scope in addition to query permissions.
2. **Time Window Constraint**: The deletion end time (`--delete-to` or `DT_DELETE_TO`) must be at least 4 hours in the past. This is a platform constraint to prevent accidental deletion of actively ingested data.
3. **Query Requirements**: The delete query must not contain a `| limit` clause.

#### Deletion Workflow

**Step 1: Dry-run validation (recommended)**

Analyze the deletion workload without actually deleting any records:

```bash
# Estimate records to be deleted and validate permissions
python3 grail_query_to_csv.py --dry-run-delete

# With custom deletion window (different from export window)
python3 grail_query_to_csv.py --dry-run-delete \
  --delete-from "2026-01-01T00:00:00.000000000Z" \
  --delete-to "2026-03-20T00:00:00.000000000Z"
```

The dry-run will:
- Validate token permissions for both query and deletion
- Break the deletion window into 24-hour chunks (API requirement)
- Estimate the number of records and data size per chunk
- Display expected deletion time per chunk
- Report the total number of matching records currently visible

**Step 2: Export and delete**

Once validated, proceed with the actual export and deletion:

```bash
# Export to CSV, then prompt for deletion confirmation
python3 grail_query_to_csv.py --cleanup

# Non-interactive mode (use DT_CLEANUP environment variable)
DT_CLEANUP=true python3 grail_query_to_csv.py
```

**Interactive Confirmation**: When `--cleanup` is used, the script will:
1. Export data to CSV first
2. Display deletion window and chunk estimates
3. Prompt: `Proceed with hard delete in Grail? Type 'yes' to continue:`
4. Only proceed with deletion if you explicitly type `yes`

#### Deletion Process Details

- **Chunked Deletion**: Due to API limits, the script automatically breaks the deletion window into 24-hour chunks and processes them sequentially from oldest to newest.
- **Progress Tracking**: Real-time progress updates show completion status, observed deletion times, and remaining estimated time.
- **Post-deletion Validation**: After each chunk, the script queries Grail to confirm records were removed. Configurable via `DT_DELETE_VALIDATE_RETRIES` (default: 12) and `DT_DELETE_VALIDATE_INTERVAL_SECONDS` (default: 10).
- **Idempotent**: Safe to re-run if interrupted; completed chunks will show zero records and skip quickly.

#### Advanced Deletion Options

**Separate deletion query**

Use a different DQL query for deletion than for export:

```bash
# Export with one query, delete with another
python3 grail_query_to_csv.py \
  --query "fetch logs | filter loglevel == 'DEBUG'" \
  --delete-query "fetch logs | filter loglevel == 'DEBUG' and dt.entity.host == 'prod-server-1'" \
  --cleanup
```

**Custom deletion window**

Delete a different time range than the export window:

```bash
# Export last 7 months, but only delete records older than 30 days
python3 grail_query_to_csv.py \
  --from "2025-08-01T00:00:00.000000000Z" \
  --to "2026-03-25T00:00:00.000000000Z" \
  --delete-from "2025-08-01T00:00:00.000000000Z" \
  --delete-to "2026-02-23T00:00:00.000000000Z" \
  --cleanup
```

#### Environment Variables for Deletion

Set these in your `.env` file for deletion configuration:

```bash
DT_DELETE_QUERY="fetch logs | filter ..."     # Defaults to DT_QUERY if not specified
DT_DELETE_FROM="2026-01-01T00:00:00.000000000Z"  # Defaults to DT_FROM
DT_DELETE_TO="2026-03-20T00:00:00.000000000Z"    # Defaults to DT_TO (must be 4+ hours old)
DT_CLEANUP=true                                # Enable deletion without --cleanup flag
DT_DELETE_VALIDATE_RETRIES=12                  # Post-delete validation retry count
DT_DELETE_VALIDATE_INTERVAL_SECONDS=10         # Post-delete validation retry interval
```

#### Deletion Safety Features

- **Pre-flight checks**: Validates token permissions and query syntax before any operations
- **Window mismatch warning**: Alerts if deletion window extends beyond export window
- **Explicit confirmation**: Interactive prompt prevents accidental deletion
- **CSV export first**: Data is always exported before deletion begins
- **Validation**: Post-deletion queries confirm records were removed
- **Interruptible**: Press Ctrl+C to cancel; already-deleted chunks remain deleted, incomplete chunks can be retried

### Preparing Customer Runbooks

```bash
# Generate customer-ready package in an auto-created folder
./prepare_customer_runbooks.sh "Customer Name"

# Generate customer-ready package (recommended naming includes customer and date)
./prepare_customer_runbooks.sh --customer "Customer Name" --output-dir customer-ready-customer-name-YYYYMMDD

# Internal SE optional check: validate EN/BR/ES runbook section parity before packaging
./check_runbook_parity.sh
```

Packaging output notes:

- The output directory includes `RUNBOOK.md`, `RUNBOOK_BR.md`, `RUNBOOK_ES.md`, `grail_query_to_csv.py`, `env.txt`, and `MANIFEST.txt`.
- `MANIFEST.txt` includes package metadata and SHA256 hashes for delivery verification.

Integrity validation with `MANIFEST.txt`:

1. Move to the generated package directory.
2. Recalculate SHA256 for delivered files.
3. Compare each calculated hash with the `SHA256` section in `MANIFEST.txt`.
4. Any mismatch means the file changed (or was corrupted) after package generation.

```bash
cd customer-ready-customer-name-YYYYMMDD
shasum -a 256 RUNBOOK.md RUNBOOK_BR.md RUNBOOK_ES.md grail_query_to_csv.py env.txt
```

## Configuration

Set up your environment variables by creating a `.env` file based on `env.txt`. Required variables depend on the Grail query being executed.

## License

Internal Dynatrace documentation and tooling.
