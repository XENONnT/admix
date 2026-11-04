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

STOP_FILE=${STOP_FILE:-/tmp/admix-stop}

cleanup() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Postponing any interrupted upload..."
    ~/.local/bin/admix-fix --postpone
}

trap 'echo "[$(date "+%Y-%m-%d %H:%M:%S")] aDMIX Upload loop stopped."; cleanup; exit 0' SIGINT SIGTERM

echo "Starting aDMIX Upload Worker Loop (backoff: ${SLEEP_TIME}s, stop file: ${STOP_FILE})"

while true; do
    if [[ -e "$STOP_FILE" ]]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stop file ${STOP_FILE} found, exiting."
        break
    fi

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting admix Upload..."
    ~/.local/bin/admix Upload

    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] admix Upload exited with code ${EXIT_CODE}."

    cleanup

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting ${SLEEP_TIME}s before restarting worker..."
    sleep "${SLEEP_TIME}"
done
