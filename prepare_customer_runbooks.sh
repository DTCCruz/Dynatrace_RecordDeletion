#!/usr/bin/env bash
################################################################################
# INTERNAL USE ONLY - DYNATRACE CONTRIBUTORS AND EMPLOYEES
################################################################################
# This script is intended for internal use by Dynatrace contributors and
# employees to prepare customer-facing Markdown files.
#
# Use it to replace customer placeholders and adapt the runbooks to reflect the
# name of the customer or customers who will receive the scripts and documents.
#
# This helper is not intended for external or customer distribution.
################################################################################

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./prepare_customer_runbooks.sh "Customer Name"
  ./prepare_customer_runbooks.sh --customer Customer Name
  ./prepare_customer_runbooks.sh --customer Customer Name --output-dir DIR

Description:
  Replaces {{CUSTOMER_NAME}} and removes legacy internal template-note lines.

Modes:
  default      Auto-generates output directory: customer-ready-<customer>-<YYYYMMDD>
  --output-dir Creates customer-ready copies in the specified directory

Scope:
  Processes RUNBOOK.md, RUNBOOK_BR.md, and RUNBOOK_ES.md

Customer package files:
  Also copies grail_query_to_csv.py and env.txt to the output directory.

Examples:
  ./prepare_customer_runbooks.sh ACME
  ./prepare_customer_runbooks.sh --customer "ACME Corp"
  ./prepare_customer_runbooks.sh --customer "ACME Corp" --output-dir customer-ready-acme-20260324
EOF
}

slugify_customer_name() {
  local input="$1"
  echo "$input" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//; s/-+/-/g'
}

replace_customer_name() {
  local file_path="$1"
  local customer_name="$2"

  CUSTOMER_NAME="$customer_name" perl -0pi -e '
    s/\{\{CUSTOMER_NAME\}\}/$ENV{CUSTOMER_NAME}/g;

    # Also replace previously injected customer names in fixed legal disclaimer lines.
    s/(^> - This script was developed solely to assist \*\*)([^*]+)(\*\* with Grail data export and deletion workflows\.$)/$1$ENV{CUSTOMER_NAME}$3/gm;
    s/(^> - \*\*)([^*]+)(\*\* acknowledges responsibility for installation, customization, implementation, validation, security review, and ongoing operation, with \*\*NO FUTURE SUPPORT COMMITMENT\*\* from Dynatrace\.$)/$1$ENV{CUSTOMER_NAME}$3/gm;

    s/(^> - Este script foi desenvolvido exclusivamente para auxiliar \*\*)([^*]+)(\*\* nos fluxos de exportação e exclusão de dados no Grail\.$)/$1$ENV{CUSTOMER_NAME}$3/gm;
    s/(^> - \*\*)([^*]+)(\*\* reconhece responsabilidade por instalação, customização, implementação, validação, revisão de segurança e operação contínua, sem \*\*COMPROMISSO DE SUPORTE FUTURO\*\* da Dynatrace\.$)/$1$ENV{CUSTOMER_NAME}$3/gm;

    s/(^> - Este script fue desarrollado exclusivamente para auxiliar a \*\*)([^*]+)(\*\* en flujos de exportación y eliminación de datos en Grail\.$)/$1$ENV{CUSTOMER_NAME}$3/gm;
    s/(^> - \*\*)([^*]+)(\*\* reconoce responsabilidad por instalación, personalización, implementación, validación, revisión de seguridad y operación continua, sin \*\*COMPROMISO DE SOPORTE FUTURO\*\* de Dynatrace\.$)/$1$ENV{CUSTOMER_NAME}$3/gm;
  ' "$file_path"

  perl -0pi -e 's/^[ \t]*> - \*\*Template note for Dynatrace SEs:\*\*.*\n//mg; s/^[ \t]*> - \*\*Nota de template para SEs Dynatrace:\*\*.*\n//mg;' "$file_path"
}

copy_if_exists() {
  local src="$1"
  local dst="$2"

  if [[ ! -f "$src" ]]; then
    echo "Skipping missing file: $(basename "$src")" >&2
    return 1
  fi

  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
  return 0
}

generate_manifest() {
  local output_abs="$1"
  local customer_name="$2"
  shift 2
  local files=("$@")
  local manifest_path="${output_abs}/MANIFEST.txt"

  {
    echo "Customer Package Manifest"
    echo "GeneratedAtUTC: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "Customer: ${customer_name}"
    echo "SourceDirectory: ${script_dir}"
    echo
    echo "Files:"
    for rel in "${files[@]}"; do
      echo "- ${rel}"
    done
    echo "- grail_query_to_csv.py"
    echo "- env.txt"

    if command -v shasum >/dev/null 2>&1; then
      echo
      echo "SHA256:"
      for rel in "${files[@]}"; do
        if [[ -f "${output_abs}/${rel}" ]]; then
          hash_val="$(shasum -a 256 "${output_abs}/${rel}" | awk '{print $1}')"
          echo "- ${rel}: ${hash_val}"
        fi
      done
      for rel in grail_query_to_csv.py env.txt; do
        if [[ -f "${output_abs}/${rel}" ]]; then
          hash_val="$(shasum -a 256 "${output_abs}/${rel}" | awk '{print $1}')"
          echo "- ${rel}: ${hash_val}"
        fi
      done
    fi
  } >"${manifest_path}"
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

customer_name=""
output_dir=""

if [[ "$1" != --* ]]; then
  customer_name="$1"
  shift
  if [[ $# -gt 0 ]]; then
    echo "Unexpected arguments: $*" >&2
    usage
    exit 1
  fi
else
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --customer)
        shift
        if [[ $# -eq 0 || "$1" == -* ]]; then
          echo "Missing customer name after --customer" >&2
          usage
          exit 1
        fi
        name_parts=()
        while [[ $# -gt 0 && "$1" != -* ]]; do
          name_parts+=("$1")
          shift
        done
        customer_name="${name_parts[*]}"
        ;;
      --output-dir|--output)
        shift
        if [[ $# -eq 0 || "$1" == --* ]]; then
          echo "Missing output directory after --output-dir" >&2
          usage
          exit 1
        fi
        output_dir="$1"
        shift
        ;;
      --include-public|-include-public)
        # Legacy flag kept for backward compatibility (no-op).
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown option: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
fi

if [[ -z "$customer_name" ]]; then
  echo "Customer name is required." >&2
  usage
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "$output_dir" ]]; then
  customer_slug="$(slugify_customer_name "$customer_name")"
  if [[ -z "$customer_slug" ]]; then
    customer_slug="customer"
  fi
  output_dir="customer-ready-${customer_slug}-$(date +%Y%m%d)"
  echo "No --output-dir provided. Using: ${output_dir}"
fi

if [[ "$output_dir" = /* ]]; then
  output_abs="$output_dir"
else
  output_abs="${script_dir}/${output_dir}"
fi

files=(
  "RUNBOOK.md"
  "RUNBOOK_BR.md"
  "RUNBOOK_ES.md"
)

mkdir -p "$output_abs"

for rel in "${files[@]}"; do
  src="${script_dir}/${rel}"
  dst="${output_abs}/${rel}"

  if [[ ! -f "$src" ]]; then
    echo "Skipping missing template: $rel" >&2
    continue
  fi

  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"

  replace_customer_name "$dst" "$customer_name"
done

copy_if_exists "${script_dir}/grail_query_to_csv.py" "${output_abs}/grail_query_to_csv.py" && \
  echo " - ${output_dir}/grail_query_to_csv.py"
copy_if_exists "${script_dir}/env.txt" "${output_abs}/env.txt" && \
  echo " - ${output_dir}/env.txt"

generate_manifest "${output_abs}" "${customer_name}" "${files[@]}"
echo " - ${output_dir}/MANIFEST.txt"

echo "Generated customer-ready runbooks in: ${output_abs}"
for rel in "${files[@]}"; do
  echo " - ${output_dir}/${rel}"
done
