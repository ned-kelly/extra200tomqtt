# Pylontech Extra200 to MQTT Battery Monitor for Victron

This is a python based service designed to read the data from the Pylon Extra2000 Battery (an old battery manufactuerd pre the US series) that has limited integrations to inverters via canbus etc. The program connects via the rs232 port of the master battery and collects data (e.g., voltage, current, temperature, State of Charge \[SoC\]), and publishes it to an MQTT broker. It is designed to work with the Victron ecosystem, requiring installation on a Victron inverter running Venus OS, using the [`venus-os_dbus-mqtt-battery`](https://github.com/mr-manuel/venus-os_dbus-mqtt-battery) driver to integrate the data into the Victron GX device (e.g., Cerbo GX or Raspberry Pi with Venus OS). The system includes a systemd service for automatic startup, a RAM disk for logging, and scripts for installation and uninstallation.

## Overview

- **Purpose**: Monitors battery parameters via serial communication and publishes them to an MQTT broker, enabling integration with Victron GX devices and Home Assistant.
- **Dependencies**: `python3` with the `pyyaml`, `paho-mqtt`, `pyserial` dependencies.
- **Installation Directory**: `/opt/extra200tomqtt` (default).

![victron-dashboard](./screenshots/victron-dashboard.png) ![integration-sample.png](./screenshots/integration-sample.png)



## Prerequisites

- Linux system with `systemd` (e.g., Ubuntu, Debian) or a Victron GX device running Venus OS.
- Python 3.x installed.
- `pip3` for dependency management.
- Sudo privileges for installation.
- Victron inverter with Venus OS installed (e.g., Cerbo GX). Users must install the `venus-os_dbus-mqtt-battery` driver on their Victron device to process the MQTT data. (read below for details)

## What the Script Does

The `extra200tomqtt.py` script, executed via `start.sh`, performs the following:

1. **Serial Communication**: Connects to a serial BMS (e.g., via RS232, RS485, or TTL UART) to retrieve real-time battery data such as voltage, current, temperature, and SoC.
2. **Data Processing**: Parses the raw serial data into a structured format, applying logic to calculate derived values (e.g., power = voltage × current) and handle anomalies like missing or invalid readings.
3. **MQTT Publishing**: Publishes processed data to an MQTT broker using topics defined in `config.yaml` (e.g., `/battery/data`). The data is formatted as JSON for compatibility with Victron's `venus-os_dbus-mqtt-battery` driver.
4. **Logging**: Writes operational logs to `/opt/extra200tomqtt/extra200tomqtt.log` and a RAM disk at `/opt/extra200tomqtt/ramdisk/battery_monitor.log` to minimize wear on persistent storage.

## How It Works

- **Startup**: The `start.sh` script mounts a 50MB RAM disk, ensuring temporary logs are stored in memory. It then enters a loop to monitor and restart `extra200tomqtt.py` if it fails. We use a ramdisk as logs can be verbose and if you are running this on a device with EMMC like a raspberry pi this will prolong the life of the EMMC on the device.
- **Main Loop**: `extra200tomqtt.py` continuously polls the BMS, processes the data, and publishes it to MQTT. The loop includes a delay between restarts and polling the inverter (configurable in yaml) to prevent overwhelming the system.
- **Configuration**: `config.yaml` specifies serial port, baud rate, MQTT broker details, and topic structure. The script dynamically updates this file to use the RAM disk for logging.

## Protection Measures and Logic

The script includes several safety and reliability features:

1. **RAM Disk Usage**: By mounting a tmpfs at `/opt/extra200tomqtt/ramdisk`, logs are stored in volatile memory, reducing SD card wear on Victron devices and protecting against data corruption during power loss.
2. **Restart Logic**: The `monitor_script` function in `start.sh` automatically restarts the Python script if it exits (e.g., due to a BMS disconnection), with a 10-second delay to avoid rapid cycling.
3. **Error Handling**: The script logs errors (e.g., failed RAM disk mount or serial connection issues) to `extra200tomqtt.log`, allowing users to diagnose problems without interrupting operation.
4. **Resource Limits**: The 50MB RAM disk size caps memory usage, preventing the script from consuming excessive system resources on a Victron GX device.
5. **Victron Integration**: When paired with `venus-os_dbus-mqtt-battery`, the driver on the Victron device subscribes to the MQTT topics and maps the data to the D-Bus, ensuring the inverter uses accurate battery status for charge/discharge control. This driver handles conflicts (e.g., duplicate D-Bus names) and supports the latest three stable Venus OS versions.

## Installation on Victron Inverter

To integrate with a Victron inverter:

1. Ensure your Victron GX device (e.g., Cerbo GX) is running Venus OS.
2. Install the `venus-os_dbus-mqtt-battery` driver by following its instructions (https://github.com/mr-manuel/venus-os_dbus-mqtt-battery). This typically involves SSH access, downloading the driver, and configuring it to subscribe to the MQTT topics published by this script.
3. Configure `config.yaml` in this repository to match the MQTT broker and topics expected by the Victron driver.

## Home Assistant Integration

![Integration](./homeassistant/mqtt-integration.png)

A sample Home Assistant MQTT sensor configuration has been included in the `homeassistant/` directory to import individual battery statuses. Copy the `pylon-batteries.yaml` onto your Home Assistant server add this to your `configuration.yaml` and restart Home Assistant:

```yaml
mqtt:
  sensor: !include pylon-batteries.yaml
```

Refer to the `homeassistant/` directory for further details.

![Lovelace](./homeassistant/lovelace.png)


## Installation

1. Clone the repository or download the files:

   ```bash
   git clone https://github.com/ned-kelly/extra200tomqtt.git
   cd extra200tomqtt
   ```

2. Run the installation script with sudo:

   ```bash
   sudo ./install.sh
   ```

3. Verify the service is running:

   ```bash
   systemctl status extra200tomqtt.service
   ```

## Configuration

- Edit `config.yaml` in `/opt/extra200tomqtt` to adjust MQTT settings, serial port, and battery addresses.
- The RAM disk path is set to `/opt/extra200tomqtt/ramdisk` and updated dynamically in `config.yaml`.

## Usage

- The service starts automatically on boot and restarts if the script fails.
- Monitor logs with:

  ```bash
  journalctl -u extra200tomqtt.service
  cat /opt/extra200tomqtt/extra200tomqtt.log
  ```

## Uninstallation

1. Run the uninstall script with sudo:

   ```bash
   sudo ./uninstall.sh
   ```

2. Verify removal:

   ```bash
   systemctl status extra200tomqtt.service
   ls /opt/extra200tomqtt
   ```

## Contributing

Submit issues or pull requests on GitHub with details of bugs or enhancements. But pelase note i'm pretty slack on github, and generally only reply to support requests or issues if they are genuiine and i have time (i'm time poor so dont expect much). If you get stuck try and feed this link into ChatGPT or Grok before submitting an issue. **If you don't know what MQTT is, Python, or how to install a DBUS serviece on a Victron inverter this project is not for you.**

## Version History

- **June 21, 2025**: Initial release with serial monitoring, MQTT publishing, and Victron integration.