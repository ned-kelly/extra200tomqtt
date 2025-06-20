#!/bin/bash

# Configuration
INSTALL_DIR="/opt/extra200tomqtt"
SERVICE_NAME="extra200tomqtt.service"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/var/log"
LOG_FILE="${LOG_DIR}/extra200tomqtt.log"

# Ensure script is run with sudo
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root or use sudo"
  exit 1
fi

# Create installation directory
echo "$(date) - Creating installation directory $INSTALL_DIR" | tee -a "$LOG_FILE"
mkdir -p "$INSTALL_DIR"
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to create $INSTALL_DIR" | tee -a "$LOG_FILE"
  exit 1
fi

# Copy files
echo "$(date) - Copying files to $INSTALL_DIR" | tee -a "$LOG_FILE"
cp "$SCRIPT_DIR/extra200tomqtt.py" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/config.yaml" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/start.sh" "$INSTALL_DIR/"
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to copy files" | tee -a "$LOG_FILE"
  exit 1
fi

# Make scripts executable
chmod +x "$INSTALL_DIR/extra200tomqtt.py"
chmod +x "$INSTALL_DIR/start.sh"

# Install Python dependencies
echo "$(date) - Installing Python dependencies" | tee -a "$LOG_FILE"
pip3 install -r "$SCRIPT_DIR/requirements.txt" --break-system-packages
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to install dependencies" | tee -a "$LOG_FILE"
  exit 1
fi

# Ensure log directory exists
echo "$(date) - Ensuring log directory exists" | tee -a "$LOG_FILE"
mkdir -p "$LOG_DIR"
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to create log directory" | tee -a "$LOG_FILE"
  exit 1
fi

# Create systemd service file
echo "$(date) - Creating systemd service file" | tee -a "$LOG_FILE"
cat << EOF > /etc/systemd/system/$SERVICE_NAME
[Unit]
Description=Extra200 to MQTT Service
After=network.target

[Service]
Type=simple
ExecStart=/bin/bash $INSTALL_DIR/start.sh
Restart=always
RestartSec=10
WorkingDirectory=$INSTALL_DIR
StandardOutput=journal
StandardError=journal
User=root

[Install]
WantedBy=multi-user.target
EOF

if [ $? -ne 0 ]; then
  echo "$(date) - Failed to create systemd service file" | tee -a "$LOG_FILE"
  exit 1
fi

# Reload systemd, enable, and start the service
echo "$(date) - Reloading systemd and enabling service" | tee -a "$LOG_FILE"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl start "$SERVICE_NAME"
if [ $? -ne 0 ]; then
  echo "$(date) - Failed to start service" | tee -a "$LOG_FILE"
  exit 1
fi

echo "$(date) - Installation completed successfully" | tee -a "$LOG_FILE"
