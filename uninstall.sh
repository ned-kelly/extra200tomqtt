#!/bin/bash

# Configuration
INSTALL_DIR="/opt/extra200tomqtt"
SERVICE_NAME="extra200tomqtt.service"
LOG_FILE="/var/log/extra200tomqtt.log"

# Ensure script is run with sudo
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root or use sudo"
  exit 1
fi

# Stop and disable the service
if systemctl is-active --quiet "$SERVICE_NAME"; then
  echo "$(date) - Stopping $SERVICE_NAME" | tee -a "$LOG_FILE"
  systemctl stop "$SERVICE_NAME"
  if [ $? -ne 0 ]; then
    echo "$(date) - Failed to stop service" | tee -a "$LOG_FILE"
  fi
fi

if systemctl is-enabled --quiet "$SERVICE_NAME"; then
  echo "$(date) - Disabling $SERVICE_NAME" | tee -a "$LOG_FILE"
  systemctl disable "$SERVICE_NAME"
  if [ $? -ne 0 ]; then
    echo "$(date) - Failed to disable service" | tee -a "$LOG_FILE"
  fi
fi

# Remove service file
echo "$(date) - Removing systemd service file" | tee -a "$LOG_FILE"
rm -f "/etc/systemd/system/$SERVICE_NAME"
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to remove service file" | tee -a "$LOG_FILE"
fi

# Reload systemd
echo "$(date) - Reloading systemd" | tee -a "$LOG_FILE"
systemctl daemon-reload
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to reload systemd" | tee -a "$LOG_FILE"
fi

# Remove installation directory and log file
echo "$(date) - Removing installation directory $INSTALL_DIR" | tee -a "$LOG_FILE"
rm -rf "$INSTALL_DIR"
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to remove $INSTALL_DIR" | tee -a "$LOG_FILE"
else
  echo "$(date) - Removed $INSTALL_DIR successfully" | tee -a "$LOG_FILE"
fi

echo "$(date) - Uninstallation completed" | tee -a "$LOG_FILE"
