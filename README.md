# Dynatrace Grail Bucket Management

This repository contains tools and documentation for managing Dynatrace Grail data and generating customer-ready runbooks.

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

# Run the export script
python3 grail_query_to_csv.py [options]

# Optional: estimate delete impact without deleting records
python3 grail_query_to_csv.py --cleanup --dry-run-delete
```

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

## Configuration

Set up your environment variables by creating a `.env` file based on `env.txt`. Required variables depend on the Grail query being executed.

## License

Internal Dynatrace documentation and tooling.
