#!/bin/bash
# ==============================================================================
# admix_recovery_loop.sh - Automated recovery loop for postponed datasets
#
# Usage:
#   ./scripts/admix_recovery_loop.sh [--max_attempts N] [--sleep_time SECONDS]
#
# Scans path_datasets_to_fix (e.g. /home/datamanager/datasets_to_fix) and
# automatically runs recovery on postponed datasets.
# Poison pills (datasets failing repeatedly) are automatically quarantined
# to avoid infinite loops and allow subsequent datasets to proceed.
# ==============================================================================

STOP_FILE=${STOP_FILE:-/tmp/admix-stop}
ADMIX_FIX_BIN=$(command -v admix-fix 2>/dev/null || echo "$HOME/.local/bin/admix-fix")

trap "echo -e '\n[$(date \"+%Y-%m-%d %H:%M:%S\")] aDMIX Recovery loop stopped by user.'; exit 0" SIGINT SIGTERM

if [[ -e "$STOP_FILE" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stop file ${STOP_FILE} found, exiting."
    exit 0
fi

echo "===================================================================="
echo " Starting aDMIX Postponed Recovery Loop"
echo "===================================================================="

"$ADMIX_FIX_BIN" --recover_postponed --loop "$@"
