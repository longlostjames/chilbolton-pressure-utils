#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YESTERDAY="$(date -u -d "yesterday" +%Y%m%d)"

"${SCRIPT_DIR}/process_stfc_date.sh" "${YESTERDAY}"