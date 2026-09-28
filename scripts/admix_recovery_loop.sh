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

trap "echo -e '\n[$(date \"+%Y-%m-%d %H:%M:%S\")] aDMIX Recovery loop stopped by user.'; exit 0" SIGINT SIGTERM

echo "===================================================================="
echo " Starting aDMIX Postponed Recovery Loop"
echo "===================================================================="

admix-fix --recover_postponed --loop "$@"
