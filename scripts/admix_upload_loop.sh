#!/bin/bash
# ==============================================================================
# admix_upload_loop.sh - Resilient loop for aDMIX Upload worker threads
#
# Usage:
#   ./scripts/admix_upload_loop.sh [SLEEP_TIME_SECONDS]
#
# Runs 'admix Upload'. When the command exits (e.g. error, network timeout),
# it immediately executes 'admix-fix --postpone' to quarantine the interrupted
# dataset and clear /tmp/admix-<screen>, preventing false-alarm crash alerts from
# admix-upload-manager. It then waits SLEEP_TIME seconds (default: 300) before
# restarting the worker.
# ==============================================================================

SLEEP_TIME=${1:-300}

trap "echo -e '\n[$(date \"+%Y-%m-%d %H:%M:%S\")] aDMIX Upload loop stopped by user.'; exit 0" SIGINT SIGTERM

echo "===================================================================="
echo " Starting aDMIX Upload Worker Loop (backoff: ${SLEEP_TIME}s)"
echo "===================================================================="

while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting admix Upload..."
    admix Upload
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] admix Upload exited with code ${EXIT_CODE}."

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Postponing any interrupted dataset (/tmp/admix-<screen>)..."
    admix-fix --postpone

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting ${SLEEP_TIME}s before restarting worker..."
    sleep "${SLEEP_TIME}"
done
