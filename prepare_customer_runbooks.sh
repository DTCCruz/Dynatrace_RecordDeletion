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
  ./prepare_customer_runbooks.sh --customer Customer Name [--include-public]
  ./prepare_customer_runbooks.sh --customer Customer Name --output-dir DIR [--include-public]

Description:
  Replaces {{CUSTOMER_NAME}} and removes legacy internal template-note lines.

Modes:
  default      Updates existing runbooks in place
  --output-dir Creates customer-ready copies in the specified directory

Scope:
  default          Processes RUNBOOK.md and RUNBOOK_BR.md
  --include-public Also processes Public/RUNBOOK.md and Public/RUNBOOK_BR.md
                   (single-dash alias -include-public is also accepted)

Examples:
  ./prepare_customer_runbooks.sh Cielo
  ./prepare_customer_runbooks.sh --customer Banco do Brasil
  ./prepare_customer_runbooks.sh --customer Banco do Brasil --include-public
  ./prepare_customer_runbooks.sh --customer Banco do Brasil --output-dir dist-bdb
EOF
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
  ' "$file_path"

  perl -0pi -e 's/^[ \t]*> - \*\*Template note for Dynatrace SEs:\*\*.*\n//mg; s/^[ \t]*> - \*\*Nota de template para SEs Dynatrace:\*\*.*\n//mg;' "$file_path"
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

customer_name=""
output_dir=""
include_public="false"

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
        include_public="true"
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
if [[ "$output_dir" = /* ]]; then
  output_abs="$output_dir"
else
  output_abs="${script_dir}/${output_dir}"
fi

files=(
  "RUNBOOK.md"
  "RUNBOOK_BR.md"
)

if [[ "$include_public" == "true" ]]; then
  files+=(
    "Public/RUNBOOK.md"
    "Public/RUNBOOK_BR.md"
  )
fi

if [[ -z "$output_dir" ]]; then
  for rel in "${files[@]}"; do
    src="${script_dir}/${rel}"

    if [[ ! -f "$src" ]]; then
      echo "Skipping missing template: $rel" >&2
      continue
    fi

    replace_customer_name "$src" "$customer_name"
    echo "Updated in place: $rel"
  done

  echo "Updated existing runbooks in place for customer: ${customer_name}"
  exit 0
fi

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

echo "Generated customer-ready runbooks in: ${output_abs}"
for rel in "${files[@]}"; do
  echo " - ${output_dir}/${rel}"
done
