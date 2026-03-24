#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runbooks=(
  "RUNBOOK.md"
  "RUNBOOK_BR.md"
  "RUNBOOK_ES.md"
)

for rb in "${runbooks[@]}"; do
  if [[ ! -f "${script_dir}/${rb}" ]]; then
    echo "Missing runbook: ${rb}" >&2
    exit 1
  fi
done

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

extract_headers() {
  local file_path="$1"
  local out_file="$2"
  grep '^## ' "$file_path" | sed -E 's/^##[[:space:]]*//' >"$out_file"
}

extract_section_numbers() {
  local in_file="$1"
  local out_file="$2"
  awk -F'.' '/^[0-9]+\./ { print $1 }' "$in_file" >"$out_file"
}

extract_headers "${script_dir}/RUNBOOK.md" "${tmp_dir}/en_headers.txt"
extract_headers "${script_dir}/RUNBOOK_BR.md" "${tmp_dir}/br_headers.txt"
extract_headers "${script_dir}/RUNBOOK_ES.md" "${tmp_dir}/es_headers.txt"

extract_section_numbers "${tmp_dir}/en_headers.txt" "${tmp_dir}/en_numbers.txt"
extract_section_numbers "${tmp_dir}/br_headers.txt" "${tmp_dir}/br_numbers.txt"
extract_section_numbers "${tmp_dir}/es_headers.txt" "${tmp_dir}/es_numbers.txt"

ok=true

if ! diff -q "${tmp_dir}/en_numbers.txt" "${tmp_dir}/br_numbers.txt" >/dev/null; then
  echo "Mismatch: section numbering differs between RUNBOOK.md and RUNBOOK_BR.md" >&2
  ok=false
fi

if ! diff -q "${tmp_dir}/en_numbers.txt" "${tmp_dir}/es_numbers.txt" >/dev/null; then
  echo "Mismatch: section numbering differs between RUNBOOK.md and RUNBOOK_ES.md" >&2
  ok=false
fi

en_count="$(wc -l <"${tmp_dir}/en_headers.txt" | tr -d '[:space:]')"
br_count="$(wc -l <"${tmp_dir}/br_headers.txt" | tr -d '[:space:]')"
es_count="$(wc -l <"${tmp_dir}/es_headers.txt" | tr -d '[:space:]')"

if [[ "${en_count}" != "${br_count}" || "${en_count}" != "${es_count}" ]]; then
  echo "Mismatch: level-2 heading count differs (EN=${en_count}, BR=${br_count}, ES=${es_count})" >&2
  ok=false
fi

if [[ "${ok}" == "false" ]]; then
  exit 1
fi

echo "Runbook parity check passed (EN/BR/ES section numbers and heading counts aligned)."
