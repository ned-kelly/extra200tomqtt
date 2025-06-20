#!/bin/bash

# Configuration
MOUNT_POINT="/opt/extra200tomqtt/ramdisk"
SIZE_MB=50
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/extra200tomqtt.py"
PYTHON_BIN="$(which python3)"
LOG_FILE="${SCRIPT_DIR}/extra200tomqtt.log"

# Ensure log file directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to mount RAM disk
setup_ramdisk() {
    if mountpoint -q "$MOUNT_POINT"; then
        echo "$(date) - RAM disk already mounted at $MOUNT_POINT" | tee -a "$LOG_FILE"
    else
        echo "$(date) - Creating RAM disk at $MOUNT_POINT (${SIZE_MB}MB)..." | tee -a "$LOG_FILE"
        mkdir -p "$MOUNT_POINT"
        mount -t tmpfs -o size=${SIZE_MB}M tmpfs "$MOUNT_POINT"
        if mountpoint -q "$MOUNT_POINT"; then
            echo "$(date) - RAM disk successfully mounted." | tee -a "$LOG_FILE"
            # Update config.yaml log file path to use ramdisk
            sed -i "s|file_path:.*|file_path: \"${MOUNT_POINT}/battery_monitor.log\"|" "${SCRIPT_DIR}/config.yaml"
        else
            echo "$(date) - Failed to mount RAM disk." | tee -a "$LOG_FILE"
            exit 1
        fi
    fi
}

# Function to run and monitor the Python script
monitor_script() {
    while true; do
        echo "$(date) - Starting $SCRIPT_PATH" | tee -a "$LOG_FILE"
        $PYTHON_BIN "$SCRIPT_PATH" > /dev/null 2>&1
        EXIT_CODE=$?
        echo "$(date) - Script exited with code $EXIT_CODE. Restarting in 10 seconds..." | tee -a "$LOG_FILE"
        sleep 10
    done
}

# Main execution
setup_ramdisk
monitor_script
