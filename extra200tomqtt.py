#!/usr/bin/python3
import serial
import time
import json
import paho.mqtt.publish as publish
import subprocess
import logging
from logging.handlers import RotatingFileHandler
import datetime
import statistics
import yaml
import os
import sys

# Global variables for serial failure tracking
consecutive_serial_failures = 0
MAX_CONSECUTIVE_FAILURES = 10

# Default configuration file path
CONFIG_FILE = "config.yaml"

def load_config(config_file):
    """Load configuration from YAML file"""
    default_config = {
        "mqtt": {
            "active": True,
            "broker": "",
            "port": 1883,
            "username": "",
            "password": "",
            "topic_base": "victron-battery-middleware"
        },
        "serial": {
            "port": "/dev/ttyUSB0",
            "reading_freq": 2
        },
        "battery": {
            "addresses": [1, 2, 3, 4, 5],
            "num_cells": 15,
            "max_charge_voltage": 51.0,
            "max_charge_current": 50.0,
            "max_discharge_current": 50.0
        },
        "logging": {
            "level": "DEBUG",
            "file_path": "battery_monitor.log",
            "file_max_size_kb": 1000,
            "file_max_files": 5
        }
    }

    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        if not config:
            logger.warning(f"Empty configuration file {config_file}, using defaults")
            return default_config

        # Merge with defaults to handle missing keys
        def merge_dicts(default, user):
            for key, value in user.items():
                if isinstance(value, dict) and key in default:
                    default[key] = merge_dicts(default[key], value)
                else:
                    default[key] = value
            return default

        config = merge_dicts(default_config, config)

        # Validate critical fields
        required_fields = [
            ("mqtt.active", bool),
            ("mqtt.broker", str),
            ("mqtt.port", int),
            ("mqtt.topic_base", str),
            ("serial.port", str),
            ("serial.reading_freq", (int, float)),
            ("battery.addresses", list),
            ("battery.num_cells", int),
            ("battery.max_charge_voltage", (int, float)),
            ("battery.max_charge_current", (int, float)),
            ("battery.max_discharge_current", (int, float)),
            ("logging.level", str),
            ("logging.file_path", str),
            ("logging.file_max_size_kb", int),
            ("logging.file_max_files", int)
        ]

        for field, expected_type in required_fields:
            keys = field.split('.')
            value = config
            for key in keys:
                value = value.get(key, None)
                if value is None:
                    logger.error(f"Missing required configuration field: {field}")
                    sys.exit(1)
            if not isinstance(value, expected_type):
                logger.error(f"Invalid type for {field}: expected {expected_type}, got {type(value)}")
                sys.exit(1)

        # Validate battery addresses
        if not all(isinstance(addr, int) for addr in config["battery"]["addresses"]):
            logger.error("Battery addresses must be integers")
            sys.exit(1)

        # Validate logging level
        log_levels = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR}
        if config["logging"]["level"].upper() not in log_levels:
            logger.error(f"Invalid logging level: {config['logging']['level']}, using DEBUG")
            config["logging"]["level"] = logging.DEBUG
        else:
            config["logging"]["level"] = log_levels[config["logging"]["level"].upper()]

        logger.info(f"Loaded configuration from {config_file}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file {config_file} not found, using defaults")
        return default_config
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in {config_file}: {str(e)}, using defaults")
        return default_config
    except Exception as e:
        logger.error(f"Error loading configuration {config_file}: {str(e)}, using defaults")
        return default_config

# Setup logging with a temporary handler until config is loaded
logger = logging.getLogger('battery_monitor')
temp_handler = logging.StreamHandler()
temp_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y%m%d %H:%M:%S'))
logger.setLevel(logging.DEBUG)
logger.addHandler(temp_handler)

# Load configuration
config = load_config(CONFIG_FILE)

# Setup final logging with configured path
log_file_path = config["logging"]["file_path"]
log_dir = os.path.dirname(log_file_path) if os.path.dirname(log_file_path) else '.'
try:
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
        logger.info(f"Created log directory: {log_dir}")
except Exception as e:
    logger.error(f"Failed to create log directory {log_dir}: {str(e)}")
    sys.exit(1)

logger.removeHandler(temp_handler)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y%m%d %H:%M:%S')
handler = RotatingFileHandler(
    log_file_path,
    maxBytes=config["logging"]["file_max_size_kb"] * 1000,
    backupCount=config["logging"]["file_max_files"]
)
handler.setFormatter(formatter)
logger.setLevel(config["logging"]["level"])
logger.addHandler(handler)

# Configuration variables
MQTT_ACTIVE = config["mqtt"]["active"]
MQTT_BROKER = config["mqtt"]["broker"]
MQTT_PORT = config["mqtt"]["port"]
MQTT_USERNAME = config["mqtt"]["username"]
MQTT_PASSWORD = config["mqtt"]["password"]
MQTT_TOPIC_BASE = config["mqtt"]["topic_base"]
SERIAL_PORT = config["serial"]["port"]
READING_FREQ = config["serial"]["reading_freq"]
BATTERY_ADDRESSES = config["battery"]["addresses"]
NUM_CELLS = config["battery"]["num_cells"]
MAX_CHARGE_VOLTAGE = config["battery"]["max_charge_voltage"]
MAX_CHARGE_CURRENT = config["battery"]["max_charge_current"]
MAX_DISCHARGE_CURRENT = config["battery"]["max_discharge_current"]

# Event Lists
power_events_list = {
    0: ["info", "0x0", "No events"],
    1: ["warning", "0x1", "Overvoltage alarm"],
    2: ["warning", "0x2", "High voltage alarm"],
    4: ["info", "0x4", "*tbc*The voltage is normal"],
    8: ["warning", "0x8", "*tbc*Low voltage alarm"],
    16: ["warning", "0x10", "*tbc*Under voltage alarm"],
    32: ["warning", "0x20", "*tbc*Cell sleep"],
    64: ["warning", "0x40", "*tbc*Battery life alarm 1"],
    128: ["warning", "0x80", "*tbc*System startup"],
    256: ["warning", "0x100", "*tbc*Over temperature alarm"],
    512: ["warning", "0x200", "*tbc*High temperature alarm"],
    1024: ["info", "0x400", "*tbc*Temperature is normal"],
    2048: ["warning", "0x800", "*tbc*Low temperature alarm"],
    4096: ["warning", "0x1000", "*tbc*Under temperature alarm"],
    8192: ["warning", "0x2000", "*tbc*Full charge"],
    16384: ["warning", "0x4000", "*tbc*Normal power"],
    32768: ["warning", "0x8000", "*tbc*Low power"],
    65536: ["warning", "0x10000", "*tbc*Short circuit protection"],
    131072: ["warning", "0x20000", "*tbc*Discharge overcurrent protection 2"],
    262144: ["warning", "0x40000", "*tbc*Charging overcurrent protection 2"],
    524288: ["warning", "0x80000", "*tbc*Discharge overcurrent protection"],
    1048576: ["warning", "0x100000", "*tbc*Charging overcurrent protection"],
    2097152: ["info", "0x200000", "System idle"],
    4194304: ["info", "0x400000", "Charging"],
    8388608: ["info", "0x800000", "Discharging"],
    16777216: ["warning", "0x1000000", "*tbc*System power failure"],
    33554432: ["warning", "0x2000000", "*tbc*System idle"],
    67108864: ["warning", "0x4000000", "*tbc*Charging"],
    134217728: ["warning", "0x8000000", "*tbc*Discharging"],
    268435456: ["warning", "0x10000000", "*tbc*System error"],
    536870912: ["warning", "0x20000000", "*tbc*System hibernation"],
    1073741824: ["warning", "0x40000000", "*tbc*System shutdown"],
    2147483648: ["warning", "0x80000000", "*tbc*Battery life alarm 2"]
}

sys_events_list = {
    0: ["info", "0x0", "No events"],
    1: ["warning", "0x1", "Reverse connection of external power input"],
    2: ["warning", "0x2", "External power input overvoltage"],
    4: ["warning", "0x4", "Current detection error"],
    8: ["warning", "0x8", "OZ abnormal"],
    16: ["warning", "0x10", "Sleep module abnormal"],
    32: ["warning", "0x20", "temperature sensor error"],
    64: ["warning", "0x40", "Voltage detection error"],
    128: ["warning", "0x80", "I2C bus error"],
    256: ["warning", "0x100", "CAN bus address assignment error"],
    512: ["warning", "0x200", "Internal CAN bus communication error"],
    1024: ["warning", "0x400", "Charge MOS FAIL"],
    2048: ["warning", "0x800", "Discharge MOS FAIL"]
}

# Initialize variables
start_time = time.time()
uptime_start = time.time()
loops_no = 0
errors_no = 0
global_barcode = None
global_info = {}

def initialize_serial():
    """Initialize serial connection with startup mode and switch to serial mode"""
    global consecutive_serial_failures
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            subprocess.run(['stty', '-F', SERIAL_PORT, '1200', 'raw', '-echo'], check=True)
            with open(SERIAL_PORT, 'wb') as f:
                f.write(b"~20014682C0048520FCC3\r")
                f.flush()
            time.sleep(0.5)

            subprocess.run(['stty', '-F', SERIAL_PORT, '19200', 'raw', '-echo'], check=True)
            with open(SERIAL_PORT, 'wb') as f:
                f.write(b'\n')
                f.write(b'login debug\n')
                f.flush()
            time.sleep(0.5)

            ser = serial.Serial(
                port=SERIAL_PORT,
                baudrate=19200,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                bytesize=serial.EIGHTBITS,
                timeout=5
            )
            logger.info("Serial connection initialized successfully")
            print("Serial connection initialized successfully")
            consecutive_serial_failures = 0
            return ser
        except Exception as e:
            logger.error(f"Serial initialization attempt {attempt + 1} failed: {str(e)}")
            print(f"Serial initialization attempt {attempt + 1} failed: {str(e)}")
            consecutive_serial_failures += 1
            if attempt == max_attempts - 1:
                logger.error("Max serial initialization attempts reached")
                print("Max serial initialization attempts reached")
                return None
            time.sleep(2)
    return None

def serial_write(ser, req, retries=2):
    """Write to serial port with retries"""
    global consecutive_serial_failures
    for attempt in range(retries):
        try:
            if not ser.is_open:
                ser.open()
                logger.debug("Serial port opened")
                print("Serial port opened")
            ser.reset_input_buffer()
            ser.reset_output_buffer()
            ser.write(req.encode('latin-1') + b'\n')
            ser.flush()
            time.sleep(0.05)
            logger.debug(f"Serial write successful for request '{req}'")
            print(f"Serial write successful for request '{req}'")
            consecutive_serial_failures = 0
            return True
        except Exception as e:
            logger.warning(f"Serial write error for request '{req}' on attempt {attempt + 1}: {str(e)}")
            print(f"Serial write error for request '{req}' on attempt {attempt + 1}: {str(e)}")
            consecutive_serial_failures += 1
            if attempt == retries - 1:
                return False
            time.sleep(1)
    return False

def serial_read(ser, start, stop, timeout=10):
    """Read from serial port until stop condition"""
    global consecutive_serial_failures
    try:
        line_str = ""
        line_str_array = []
        start_time = time.time()
        while True:
            if ser.in_waiting > 0:
                line = ser.read()
                line_str += line.decode('latin-1', errors='ignore')
                if line == b'\n':
                    logger.debug(f"Read line: {line_str.strip()}, in_waiting: {ser.in_waiting}")
                    print(f"Read line: {line_str.strip()}")
                    if start == 'none' or start in line_str:
                        start = 'true'
                    if start == 'true':
                        line_str_array.append(line_str)
                    if start == 'true' and stop in line_str:
                        logger.debug(f"Serial read completed, lines collected: {len(line_str_array)}")
                        print(f"Serial read completed, lines collected: {len(line_str_array)}")
                        consecutive_serial_failures = 0
                        return line_str_array
                    line_str = ""
            if time.time() - start_time > timeout:
                logger.warning(f"Serial read timeout after {timeout}s, lines collected: {len(line_str_array)}")
                print(f"Serial read timeout after {timeout}s, lines collected: {len(line_str_array)}")
                consecutive_serial_failures += 1
                return line_str_array
    except Exception as e:
        logger.warning(f"Serial read error: {str(e)}")
        print(f"Serial read error: {str(e)}")
        consecutive_serial_failures += 1
        return []

def get_max_currents(ser, address):
    """Fetch max charge and discharge currents and barcode from info command"""
    global MAX_CHARGE_CURRENT, MAX_DISCHARGE_CURRENT, global_barcode, global_info
    try:
        if address != 1:
            return global_barcode
        req = f"info {address}"
        logger.debug(f"Sending request: {req}")
        print(f"Sending request: {req}")
        write_success = serial_write(ser, req)
        if not write_success:
            logger.warning(f"Serial write failed for {req}")
            print(f"Serial write failed for {req}")
            return None

        lines = serial_read(ser, req, "Command completed", timeout=10)
        if not lines:
            logger.warning(f"No data received from {req} request")
            print(f"No data received from {req} request")
            return None

        barcode = None
        for line in lines:
            logger.debug(f"Processing info line: {line.strip()}")
            print(f"  Info Line: {line.strip()}")
            try:
                parts = line.split(":")
                if len(parts) < 2:
                    continue
                key = parts[0].strip()
                value = parts[1].strip().split()[0]
                if key == "Max Charge Curr":
                    max_charge = float(value) / 1000
                    MAX_CHARGE_CURRENT = min(MAX_CHARGE_CURRENT, max_charge)
                    global_info["MaxChargeCurrent"] = MAX_CHARGE_CURRENT
                    logger.debug(f"Parsed MaxChargeCurrent: {MAX_CHARGE_CURRENT} A")
                    print(f"    Parsed MaxChargeCurrent: {MAX_CHARGE_CURRENT} A")
                elif key == "Max Dischg Curr":
                    max_dischg = abs(float(value)) / 1000
                    MAX_DISCHARGE_CURRENT = min(MAX_DISCHARGE_CURRENT, max_dischg)
                    global_info["MaxDischargeCurrent"] = MAX_DISCHARGE_CURRENT
                    logger.debug(f"Parsed MaxDischargeCurrent: {MAX_DISCHARGE_CURRENT} A")
                    print(f"    Parsed MaxDischargeCurrent: {MAX_DISCHARGE_CURRENT} A")
                elif key == "Barcode":
                    barcode = value.strip()
                    global_barcode = barcode
                    logger.debug(f"Parsed Barcode: {barcode}")
                    print(f"    Parsed Barcode: {barcode}")
                elif key == "Cell Count":
                    global_info["CellCount"] = int(value)
                    logger.debug(f"Parsed Cell Count: {global_info['CellCount']}")
                    print(f"    Parsed Cell Count: {global_info['CellCount']}")
            except Exception as e:
                logger.warning(f"Error parsing info line '{line.strip()}': {str(e)}")
                print(f"    Error parsing info line '{line.strip()}': {str(e)}")
        return barcode
    except Exception as e:
        logger.error(f"Get max currents error for address {address}: {str(e)}")
        print(f"Get max currents error for address {address}: {str(e)}")
        return None

def parse_battery_data(ser, address):
    """Parse battery data from serial for a given address"""
    global errors_no
    try:
        # Initialize JSON structure
        data = {
            "Dc": {},
            "Alarms": {
                "LowVoltage": 0,
                "HighVoltage": 0,
                "LowSoc": 0,
                "HighChargeCurrent": 0,
                "HighDischargeCurrent": 0,
                "HighCurrent": 0,
                "CellImbalance": 0,
                "HighChargeTemperature": 0,
                "LowChargeTemperature": 0,
                "LowCellVoltage": 0,
                "LowTemperature": 0,
                "HighTemperature": 0,
                "FuseBlown": 0
            },
            "Info": {
                "MaxChargeVoltage": MAX_CHARGE_VOLTAGE,
                "MaxChargeCurrent": global_info.get("MaxChargeCurrent", MAX_CHARGE_CURRENT),
                "MaxDischargeCurrent": global_info.get("MaxDischargeCurrent", MAX_DISCHARGE_CURRENT)
            },
            "History": {},
            "System": {
                "NrOfModulesOnline": 1,
                "NrOfModulesOffline": 0,
                "NrOfCellsPerBattery": global_info.get("CellCount", NUM_CELLS),
                "NrOfModulesBlockingCharge": 0,
                "NrOfModulesBlockingDischarge": 0
            },
            "Voltages": {},
            "Balances": {},
            "Io": {
                "AllowToCharge": 1,
                "AllowToDischarge": 1
            }
        }

        # Parse pwr command
        req = f"pwr {address}"
        logger.debug(f"Sending request: {req}")
        print(f"Sending request: {req}")
        write_success = serial_write(ser, req)
        if not write_success:
            logger.warning(f"Serial write failed for {req}")
            print(f"Serial write failed for {req}")
            errors_no += 1
            return None

        lines = serial_read(ser, req, "Command completed", timeout=10)
        if not lines:
            logger.warning(f"No data received from {req} request")
            print(f"No data received from {req} request")
            errors_no += 1
            return None

        logger.debug("Parsing power data")
        print("Parsing power data:")
        bat_events = 0
        power_events = 0
        sys_events = 0
        for line in lines:
            logger.debug(f"Processing line: {line.strip()}")
            print(f"  Line: {line.strip()}")
            try:
                parts = line.split(":")
                if len(parts) < 2:
                    continue
                key = parts[0].strip()
                value = parts[1].strip().split()[0]
                if key == "Voltage":
                    data["Dc"]["Voltage"] = round(float(value) / 1000, 2)  # mV to V
                    logger.debug(f"Parsed Voltage: {data['Dc']['Voltage']} V")
                    print(f"    Parsed Voltage: {data['Dc']['Voltage']} V")
                elif key == "Current":
                    data["Dc"]["Current"] = round(float(value) / 1000, 2)  # mA to A
                    logger.debug(f"Parsed Current: {data['Dc']['Current']} A")
                    print(f"    Parsed Current: {data['Dc']['Current']} A")
                elif key == "Temperature":
                    data["Dc"]["Temperature"] = round(float(value) / 1000, 1)  # mC to °C
                    data["System"]["MOSTemperature"] = data["Dc"]["Temperature"]
                    logger.debug(f"Parsed Temperature: {data['Dc']['Temperature']} °C")
                    print(f"    Parsed Temperature: {data['Dc']['Temperature']} °C")
                elif key == "Coulomb":
                    data["Soc"] = int(value)  # Percent
                    logger.debug(f"Parsed Soc: {data['Soc']} %")
                    print(f"    Parsed Soc: {data['Soc']} %")
                elif key == "Total Coulomb":
                    data["InstalledCapacity"] = round(float(value) / 1000, 1)  # mAh to Ah
                    logger.debug(f"Parsed InstalledCapacity: {data['InstalledCapacity']} Ah")
                    print(f"    Parsed InstalledCapacity: {data['InstalledCapacity']} Ah")
                elif key == "Heater Status":
                    data["Balancing"] = 1 if value.lower() == "on" else 0
                    logger.debug(f"Parsed Balancing: {data['Balancing']}")
                    print(f"    Parsed Balancing: {data['Balancing']}")
                elif key == "Charge Times":
                    data["History"]["ChargeCycles"] = int(value)
                    logger.debug(f"Parsed ChargeCycles: {data['History']['ChargeCycles']}")
                    print(f"    Parsed ChargeCycles: {data['History']['ChargeCycles']}")
                elif key == "Bat Events":
                    bat_events = int(value, 16)
                    logger.debug(f"Parsed Bat Events: {bat_events:#x}")
                    print(f"    Parsed Bat Events: {bat_events:#x}")
                elif key == "Power Events":
                    power_events = int(value, 16)
                    logger.debug(f"Parsed Power Events: {power_events:#x}")
                    print(f"    Parsed Power Events: {power_events:#x}")
                elif key == "System Fault":
                    sys_events = int(value, 16)
                    logger.debug(f"Parsed System Fault: {sys_events:#x}")
                    print(f"    Parsed System Fault: {sys_events:#x}")
            except Exception as e:
                logger.warning(f"Error parsing line '{line.strip()}': {str(e)}")
                print(f"    Error parsing line '{line.strip()}': {str(e)}")

        # Adjust MaxChargeVoltage based on SOC
        if "Soc" in data:
            if data["Soc"] < 95:
                data["Info"]["MaxChargeVoltage"] = 52.5  # Bulk charging
                logger.debug(f"Set MaxChargeVoltage to 52.5V for address {address} (SOC < 95%)")
                print(f"    Set MaxChargeVoltage to 52.5V for address {address} (SOC < 95%)")
            else:
                data["Info"]["MaxChargeVoltage"] = 51.0  # Float charging
                logger.debug(f"Set MaxChargeVoltage to 51.0V for address {address} (SOC >= 95%)")
                print(f"    Set MaxChargeVoltage to 51.0V for address {address} (SOC >= 95%)")

        # Adjust MaxChargeCurrent based on SOC and temperature
        if "Soc" in data:
            if data["Soc"] > 90:
                data["Info"]["MaxChargeCurrent"] = 7.0
                logger.debug(f"Set MaxChargeCurrent to 7A for address {address} (SOC > 90%)")
                print(f"    Set MaxChargeCurrent to 7A for address {address} (SOC > 90%)")
            else:
                data["Info"]["MaxChargeCurrent"] = round(global_info.get("MaxChargeCurrent", MAX_CHARGE_CURRENT) * 0.8, 1)
                logger.debug(f"Set MaxChargeCurrent to {data['Info']['MaxChargeCurrent']}A for address {address} (SOC <= 90%)")
                print(f"    Set MaxChargeCurrent to {data['Info']['MaxChargeCurrent']}A for address {address} (SOC <= 90%)")

        if "Temperature" in data["Dc"]:
            temp = data["Dc"]["Temperature"]
            if temp < 0:
                data["Io"]["AllowToCharge"] = 0
                data["Info"]["MaxChargeCurrent"] = 0
                data["Alarms"]["LowChargeTemperature"] = 1
                logger.debug(f"Disabled charging for address {address} (Temperature < 0°C)")
                print(f"    Disabled charging for address {address} (Temperature < 0°C)")
            elif 0 <= temp < 5:
                data["Info"]["MaxChargeCurrent"] = min(data["Info"]["MaxChargeCurrent"], 10.0)
                logger.debug(f"Limited MaxChargeCurrent to 10A for address {address} (0°C <= Temperature < 5°C)")
                print(f"    Limited MaxChargeCurrent to 10A for address {address} (0°C <= Temperature < 5°C)")
            elif temp > 45:
                data["Info"]["MaxChargeCurrent"] = min(data["Info"]["MaxChargeCurrent"], 10.0)
                data["Alarms"]["HighChargeTemperature"] = 1
                logger.debug(f"Limited MaxChargeCurrent to 10A for address {address} (Temperature > 45°C)")
                print(f"    Limited MaxChargeCurrent to 10A for address {address} (Temperature > 45°C)")

        # Limit discharge current and block discharge below 5% SOC
        if "Soc" in data:
            if data["Soc"] < 5:
                data["Io"]["AllowToDischarge"] = 0
                data["Info"]["MaxDischargeCurrent"] = 0
                data["Alarms"]["LowSoc"] = 1
                logger.debug(f"Disabled discharging for address {address} (SOC < 5%)")
                print(f"    Disabled discharging for address {address} (SOC < 5%)")
            elif data["Soc"] < 10:
                data["Info"]["MaxDischargeCurrent"] = min(data["Info"]["MaxDischargeCurrent"], 10.0)
                data["Alarms"]["LowSoc"] = 1
                logger.debug(f"Limited MaxDischargeCurrent to 10A for address {address} (SOC < 10%)")
                print(f"    Limited MaxDischargeCurrent to 10A for address {address} (SOC < 10%)")

        # Calculate derived fields
        if "Current" in data["Dc"] and "Voltage" in data["Dc"]:
            data["Dc"]["Power"] = round(data["Dc"]["Voltage"] * data["Dc"]["Current"], 1)
            logger.debug(f"Calculated Power: {data['Dc']['Power']} W")
            print(f"    Calculated Power: {data['Dc']['Power']} W")

        if "Soc" in data and "InstalledCapacity" in data:
            data["Capacity"] = round((data["Soc"] / 100.0) * data["InstalledCapacity"], 1)
            data["ConsumedAmphours"] = round(data["InstalledCapacity"] - data["Capacity"], 1)
            logger.debug(f"Calculated Capacity: {data['Capacity']} Ah, ConsumedAmphours: {data['ConsumedAmphours']} Ah")
            print(f"    Calculated Capacity: {data['Capacity']} Ah, ConsumedAmphours: {data['ConsumedAmphours']} Ah")

        if "Capacity" in data and "Current" in data["Dc"] and data["Dc"]["Current"] < 0:
            data["TimeToGo"] = int((data["Capacity"] / abs(data["Dc"]["Current"])) * 3600)
            logger.debug(f"Calculated TimeToGo: {data['TimeToGo']} s")
            print(f"    Calculated TimeToGo: {data['TimeToGo']} s")

        # Set Alarms
        if bat_events != 0 and bat_events in power_events_list:
            event = power_events_list[bat_events]
            if event[0] == "warning":
                data["Alarms"]["CellImbalance"] = 1
                logger.debug(f"Set CellImbalance for address {address}: {event[2]}")
                print(f"    Set CellImbalance for address {address}: {event[2]}")

        if power_events != 0 and power_events in power_events_list:
            event = power_events_list[power_events]
            severity = 1 if event[0] == "warning" else 0
            if power_events in [1, 2]:
                data["Alarms"]["HighVoltage"] = severity
                logger.debug(f"Set HighVoltage for address {address} to {severity}: {event[2]}")
                print(f"    Set HighVoltage for address {address} to {severity}: {event[2]}")
            elif power_events == 8:
                data["Alarms"]["LowVoltage"] = severity
                logger.debug(f"Set LowVoltage for address {address} to {severity}: {event[2]}")
                print(f"    Set LowVoltage for address {address} to {severity}: {event[2]}")
            elif power_events == 16:
                data["Alarms"]["LowCellVoltage"] = severity
                logger.debug(f"Set LowCellVoltage for address {address} to {severity}: {event[2]}")
                print(f"    Set LowCellVoltage for address {address} to {severity}: {event[2]}")
            elif power_events in [256, 512]:
                data["Alarms"]["HighTemperature"] = severity
                logger.debug(f"Set HighTemperature for address {address} to {severity}: {event[2]}")
                print(f"    Set HighTemperature for address {address} to {severity}: {event[2]}")
            elif power_events in [2048, 4096]:
                data["Alarms"]["LowTemperature"] = severity
                logger.debug(f"Set LowTemperature for address {address} to {severity}: {event[2]}")
                print(f"    Set LowTemperature for address {address} to {severity}: {event[2]}")
            elif power_events == 32768:
                data["Alarms"]["LowSoc"] = severity
                logger.debug(f"Set LowSoc for address {address} to {severity}: {event[2]}")
                print(f"    Set LowSoc for address {address} to {severity}: {event[2]}")
            elif power_events in [131072, 524288]:
                data["Alarms"]["HighDischargeCurrent"] = severity
                logger.debug(f"Set HighDischargeCurrent for address {address} to {severity}: {event[2]}")
                print(f"    Set HighDischargeCurrent for address {address} to {severity}: {event[2]}")
            elif power_events in [262144, 1048576]:
                data["Alarms"]["HighChargeCurrent"] = severity
                logger.debug(f"Set HighChargeCurrent for address {address} to {severity}: {event[2]}")
                print(f"    Set HighChargeCurrent for address {address} to {severity}: {event[2]}")
            elif power_events == 65536:
                data["Alarms"]["FuseBlown"] = severity
                logger.debug(f"Set FuseBlown for address {address} to {severity}: {event[2]}")
                print(f"    Set FuseBlown for address {address} to {severity}: {event[2]}")

        if sys_events != 0 and sys_events in sys_events_list:
            event = sys_events_list[sys_events]
            severity = 1 if event[0] == "warning" else 0
            if sys_events in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]:
                data["Alarms"]["FuseBlown"] = max(data["Alarms"]["FuseBlown"], severity)
                logger.debug(f"Set FuseBlown for address {address} to {severity}: {event[2]}")
                print(f"    Set FuseBlown for address {address} to {severity}: {event[2]}")

        # Parse bat command
        logger.debug(f"Requesting cell data for address {address}")
        print(f"Requesting cell data for address {address}:")
        req = f"bat {address}"
        write_success = serial_write(ser, req)
        if write_success:
            lines = serial_read(ser, "Battery", "Command completed", timeout=20)
            cell_voltages = []
            cell_temperatures = []
            cell_socs = []
            logger.debug(f"Cell data lines received: {len(lines)}")
            print(f"    Cell data lines received: {len(lines)}")
            for line in lines:
                logger.debug(f"Processing cell line: {line.strip()}")
                print(f"      Cell Line: {line.strip()}")
                try:
                    if not line[0].isdigit():
                        continue
                    parts = line.split()
                    if len(parts) < 10:
                        continue
                    cell_num = int(parts[0]) + 1
                    voltage = float(parts[1]) / 1000
                    temperature = float(parts[3]) / 1000
                    soc = int(parts[8].rstrip('%'))

                    data["Voltages"][f"Cell{cell_num}"] = round(voltage, 3)
                    cell_voltages.append((f"C{cell_num}", voltage))
                    cell_temperatures.append((f"C{cell_num}", temperature))
                    cell_socs.append(soc)

                    logger.debug(f"Parsed Cell{cell_num}: Voltage={voltage} V, Temperature={temperature} °C, SoC={soc} %")
                    print(f"      Parsed Cell{cell_num}: Voltage={voltage} V, Temperature={temperature} °C, SoC={soc} %")
                except Exception as e:
                    logger.warning(f"Error parsing cell line '{line.strip()}': {str(e)}")
                    print(f"      Error parsing cell line '{line.strip()}': {str(e)}")

            if cell_voltages:
                num_cells = len(cell_voltages)
                data["System"]["NrOfCellsPerBattery"] = num_cells

                min_cell = min(cell_voltages, key=lambda x: x[1])
                max_cell = max(cell_voltages, key=lambda x: x[1])
                data["System"]["MinVoltageCellId"] = min_cell[0]
                data["System"]["MinCellVoltage"] = round(min_cell[1], 3)
                data["System"]["MaxVoltageCellId"] = max_cell[0]
                data["System"]["MaxCellVoltage"] = round(max_cell[1], 3)
                logger.debug(f"Min Cell: {min_cell[0]} ({min_cell[1]}V), Max Cell: {max_cell[0]} ({max_cell[1]}V)")
                print(f"    Min Cell: {min_cell[0]} ({min_cell[1]}V), Max Cell: {max_cell[0]} ({max_cell[1]}V)")

                # Cell voltage protection
                if max_cell[1] > 3.65:
                    data["Io"]["AllowToCharge"] = 0
                    data["Alarms"]["HighVoltage"] = 1
                    logger.debug(f"Disabled charging for address {address} (Cell voltage > 3.65V)")
                    print(f"    Disabled charging for address {address} (Cell voltage > 3.65V)")
                if min_cell[1] < 2.5:
                    data["Io"]["AllowToDischarge"] = 0
                    data["Alarms"]["LowCellVoltage"] = 1
                    logger.debug(f"Disabled discharging for address {address} (Cell voltage < 2.5V)")
                    print(f"    Disabled discharging for address {address} (Cell voltage < 2.5V)")

                # Balancing logic
                if max_cell[1] - min_cell[1] > 0.05 and max_cell[1] > 3.4:
                    data["Balances"][f"Cell{max_cell[0][1:]}"] = 1
                    logger.debug(f"Enabled balancing for {max_cell[0]} at address {address} (Voltage diff > 50mV and max > 3.4V)")
                    print(f"    Enabled balancing for {max_cell[0]} at address {address} (Voltage diff > 50mV and max > 3.4V)")
                else:
                    for cell_num in range(1, num_cells + 1):
                        data["Balances"][f"Cell{cell_num}"] = 0

                if max_cell[1] - min_cell[1] > 0.1:
                    data["Alarms"]["CellImbalance"] = 1
                    logger.debug(f"Set CellImbalance for address {address} to 1 due to voltage difference > 0.1V")
                    print(f"    Set CellImbalance for address {address} to 1 due to voltage difference > 0.1V")

                min_temp = min(cell_temperatures, key=lambda x: x[1])
                max_temp = max(cell_temperatures, key=lambda x: x[1])
                data["System"]["MinTemperatureCellId"] = min_temp[0]
                data["System"]["MinCellTemperature"] = round(min_temp[1], 1)
                data["System"]["MaxTemperatureCellId"] = max_temp[0]
                data["System"]["MaxCellTemperature"] = round(max_temp[1], 1)
                logger.debug(f"Min Temp: {min_temp[0]} ({min_temp[1]}°C), Max Temp: {max_temp[0]} ({max_temp[1]}°C)")
                print(f"    Min Temp: {min_temp[0]} ({min_temp[1]}°C), Max Temp: {max_temp[0]} ({max_temp[1]}°C)")

                if min_temp[1] < 0:
                    data["Alarms"]["LowTemperature"] = 1
                    logger.debug(f"Set LowTemperature for address {address} to 1 due to cell temperature < 0°C")
                    print(f"    Set LowTemperature for address {address} to 1 due to cell temperature < 0°C")
                if max_temp[1] > 45:
                    data["Alarms"]["HighTemperature"] = 1
                    logger.debug(f"Set HighTemperature for address {address} to 1 due to cell temperature > 45°C")
                    print(f"    Set HighTemperature for address {address} to 1 due to cell temperature > 45°C")

        logger.debug(f"Parsed data for address {address}: {json.dumps(data, indent=2)}")
        print(f"Parsed data for address {address}:\n{json.dumps(data, indent=2)}")
        return data
    except Exception as e:
        logger.error(f"Parse battery data error for address {address}: {str(e)}")
        print(f"Parse battery data error for address {address}: {str(e)}")
        errors_no += 1
        return None

def create_consolidated_view(battery_data_list):
    """Create a consolidated view of all battery data"""
    if not battery_data_list:
        return None

    consolidated = {
        "Dc": {},
        "Alarms": {
            "LowVoltage": 0,
            "HighVoltage": 0,
            "LowSoc": 0,
            "HighChargeCurrent": 0,
            "HighDischargeCurrent": 0,
            "HighCurrent": 0,
            "CellImbalance": 0,
            "HighChargeTemperature": 0,
            "LowChargeTemperature": 0,
            "LowCellVoltage": 0,
            "LowTemperature": 0,
            "HighTemperature": 0,
            "FuseBlown": 0
        },
        "Info": {
            "MaxChargeVoltage": MAX_CHARGE_VOLTAGE,
            "MaxChargeCurrent": 0,
            "MaxDischargeCurrent": 0
        },
        "History": {},
        "System": {
            "NrOfModulesOnline": len(battery_data_list),
            "NrOfModulesOffline": len(BATTERY_ADDRESSES) - len(battery_data_list),
            "NrOfCellsPerBattery": global_info.get("CellCount", NUM_CELLS),
            "NrOfModulesBlockingCharge": 0,
            "NrOfModulesBlockingDischarge": 0
        },
        "Voltages": {},
        "Balances": {},
        "Io": {
            "AllowToCharge": 1,
            "AllowToDischarge": 1
        }
    }

    # Aggregate alarms and Io states
    for data in battery_data_list:
        for alarm, value in data["Alarms"].items():
            consolidated["Alarms"][alarm] = max(consolidated["Alarms"][alarm], value)
        consolidated["Io"]["AllowToCharge"] = min(consolidated["Io"]["AllowToCharge"], data["Io"]["AllowToCharge"])
        consolidated["Io"]["AllowToDischarge"] = min(consolidated["Io"]["AllowToDischarge"], data["Io"]["AllowToDischarge"])
        if data["Io"]["AllowToCharge"] == 0:
            consolidated["System"]["NrOfModulesBlockingCharge"] += 1
        if data["Io"]["AllowToDischarge"] == 0:
            consolidated["System"]["NrOfModulesBlockingDischarge"] += 1

    # Calculate average voltages and balances per cell
    cell_voltages = {f"Cell{i}": [] for i in range(1, global_info.get("CellCount", NUM_CELLS) + 1)}
    cell_balances = {f"Cell{i}": [] for i in range(1, global_info.get("CellCount", NUM_CELLS) + 1)}
    for data in battery_data_list:
        for cell, voltage in data["Voltages"].items():
            cell_voltages[cell].append(voltage)
        for cell, balance in data["Balances"].items():
            cell_balances[cell].append(balance)

    for cell in cell_voltages:
        if cell_voltages[cell]:
            consolidated["Voltages"][cell] = round(statistics.mean(cell_voltages[cell]), 3)
            consolidated["Balances"][cell] = 1 if any(cell_balances[cell]) else 0

    # Calculate system-level min/max voltages
    all_voltages = []
    for data in battery_data_list:
        for cell, voltage in data["Voltages"].items():
            all_voltages.append((cell, voltage))

    if all_voltages:
        min_cell = min(all_voltages, key=lambda x: x[1])
        max_cell = max(all_voltages, key=lambda x: x[1])
        consolidated["System"]["MinVoltageCellId"] = min_cell[0]
        consolidated["System"]["MinCellVoltage"] = round(min_cell[1], 3)
        consolidated["System"]["MaxVoltageCellId"] = max_cell[0]
        consolidated["System"]["MaxCellVoltage"] = round(max_cell[1], 3)
        if max_cell[1] - min_cell[1] > 0.1:
            consolidated["Alarms"]["CellImbalance"] = 1

    # Aggregate other system metrics
    voltages = [data["Dc"].get("Voltage", 0) for data in battery_data_list if "Voltage" in data["Dc"]]
    currents = [data["Dc"].get("Current", 0) for data in battery_data_list if "Current" in data["Dc"]]
    powers = [data["Dc"].get("Power", 0) for data in battery_data_list if "Power" in data["Dc"]]
    temperatures = [data["Dc"].get("Temperature", 0) for data in battery_data_list if "Temperature" in data["Dc"]]
    socs = [data.get("Soc", 0) for data in battery_data_list if "Soc" in data]
    installed_capacities = [data.get("InstalledCapacity", 0) for data in battery_data_list if "InstalledCapacity" in data]
    capacities = [data.get("Capacity", 0) for data in battery_data_list if "Capacity" in data]
    consumed_amphours = [data.get("ConsumedAmphours", 0) for data in battery_data_list if "ConsumedAmphours" in data]
    charge_cycles = [data["History"].get("ChargeCycles", 0) for data in battery_data_list if "ChargeCycles" in data["History"]]
    max_charge_voltages = [data["Info"].get("MaxChargeVoltage", MAX_CHARGE_VOLTAGE) for data in battery_data_list if "MaxChargeVoltage" in data["Info"]]
    max_charge_currents = [data["Info"].get("MaxChargeCurrent", 0) for data in battery_data_list if "MaxChargeCurrent" in data["Info"]]
    max_discharge_currents = [data["Info"].get("MaxDischargeCurrent", 0) for data in battery_data_list if "MaxDischargeCurrent" in data["Info"]]

    if voltages:
        consolidated["Dc"]["Voltage"] = round(statistics.mean(voltages), 2)
    if currents:
        consolidated["Dc"]["Current"] = round(sum(currents), 2)
    if powers:
        consolidated["Dc"]["Power"] = round(sum(powers), 1)
    if temperatures:
        consolidated["Dc"]["Temperature"] = round(statistics.mean(temperatures), 1)
        consolidated["System"]["MOSTemperature"] = consolidated["Dc"]["Temperature"]
    if socs:
        consolidated["Soc"] = round(statistics.mean(socs))
    if installed_capacities:
        consolidated["InstalledCapacity"] = round(sum(installed_capacities), 1)
    if capacities:
        consolidated["Capacity"] = round(sum(capacities), 1)
    if consumed_amphours:
        consolidated["ConsumedAmphours"] = round(sum(consumed_amphours), 1)
    if charge_cycles:
        consolidated["History"]["ChargeCycles"] = round(statistics.mean(charge_cycles))
    if max_charge_voltages:
        consolidated["Info"]["MaxChargeVoltage"] = min(max_charge_voltages)
        logger.debug(f"Consolidated MaxChargeVoltage: {consolidated['Info']['MaxChargeVoltage']}V")
        print(f"    Consolidated MaxChargeVoltage: {consolidated['Info']['MaxChargeVoltage']}V")
    if max_charge_currents:
        consolidated["Info"]["MaxChargeCurrent"] = round(min(max_charge_currents) * len(battery_data_list), 1)
        logger.debug(f"Consolidated MaxChargeCurrent: {consolidated['Info']['MaxChargeCurrent']}A")
        print(f"    Consolidated MaxChargeCurrent: {consolidated['Info']['MaxChargeCurrent']}A")
    if max_discharge_currents:
        consolidated["Info"]["MaxDischargeCurrent"] = round(min(max_discharge_currents) * len(battery_data_list), 1)
        logger.debug(f"Consolidated MaxDischargeCurrent: {consolidated['Info']['MaxDischargeCurrent']}A")
        print(f"    Consolidated MaxDischargeCurrent: {consolidated['Info']['MaxDischargeCurrent']}A")

    if "Capacity" in consolidated and "Current" in consolidated["Dc"] and consolidated["Dc"]["Current"] < 0:
        consolidated["TimeToGo"] = int((consolidated["Capacity"] / abs(consolidated["Dc"]["Current"])) * 3600)

    return consolidated

def mqtt_publish(data, address, barcode):
    """Publish data to MQTT broker with dynamic topic based on address and barcode"""
    try:
        topic = f"{MQTT_TOPIC_BASE}/battery_{address}"
        auth = {'username': MQTT_USERNAME, 'password': MQTT_PASSWORD} if MQTT_USERNAME else None
        message = json.dumps(data)
        publish.single(topic, message, hostname=MQTT_BROKER, port=MQTT_PORT, auth=auth)
        logger.info(f"MQTT publish successful to topic {topic}")
        print(f"MQTT publish successful to topic {topic}")
    except Exception as e:
        logger.warning(f"MQTT publish error to topic {topic}: {str(e)}")
        print(f"MQTT publish error to topic {topic}: {str(e)}")

def mqtt_publish_consolidated(data, barcode):
    """Publish consolidated data to MQTT broker"""
    try:
        topic = f"{MQTT_TOPIC_BASE}/pylon"
        auth = {'username': MQTT_USERNAME, 'password': MQTT_PASSWORD} if MQTT_USERNAME else None
        message = json.dumps(data)
        publish.single(topic, message, hostname=MQTT_BROKER, port=MQTT_PORT, auth=auth)
        logger.info(f"MQTT publish successful to topic {topic}")
        print(f"MQTT publish successful to topic {topic}")
    except Exception as e:
        logger.warning(f"MQTT publish error to topic {topic}: {str(e)}")
        print(f"MQTT publish error to topic {topic}: {str(e)}")

def main():
    global loops_no, errors_no, start_time, consecutive_serial_failures
    ser = initialize_serial()
    if not ser:
        logger.error("Failed to initialize serial connection. Exiting.")
        sys.exit(1)

    # Fetch max currents and barcodes
    barcodes = {}
    for address in BATTERY_ADDRESSES:
        barcode = get_max_currents(ser, address)
        if barcode:
            barcodes[address] = barcode
        else:
            logger.warning(f"Using default MaxChargeCurrent and MaxDischargeCurrent for address {address}")
            print(f"Using default MaxChargeCurrent and MaxDischargeCurrent for address {address}")
            barcodes[address] = f"UNKNOWN_{address}"

    logger.info("Program initialization completed. Starting main loop.")
    print("Program initialization completed. Starting main loop.")

    while True:
        try:
            if time.time() - start_time > READING_FREQ:
                loops_no += 1
                now = datetime.datetime.now()
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                uptime = round((time.time() - uptime_start) / 86400, 3)

                logger.info(f"Loop {loops_no} - Timestamp: {timestamp}, Uptime: {uptime} days")
                print(f"Loop {loops_no} - Timestamp: {timestamp}, Uptime: {uptime} days")

                if consecutive_serial_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"Reached {MAX_CONSECUTIVE_FAILURES} consecutive serial failures. Exiting.")
                    print(f"Reached {MAX_CONSECUTIVE_FAILURES} consecutive serial failures. Exiting.")
                    if ser and ser.is_open:
                        ser.close()
                        logger.info("Serial port closed")
                        print("Serial port closed")
                    sys.exit(1)

                battery_data_list = []
                for address in BATTERY_ADDRESSES:
                    print(f"Processing battery address {address}")
                    data = parse_battery_data(ser, address)
                    if data:
                        battery_data_list.append(data)
                        if MQTT_ACTIVE:
                            mqtt_publish(data, address, barcodes[address])
                    else:
                        # Fallback for offline batteries
                        offline_data = {
                            "Dc": {},
                            "Alarms": {
                                "LowVoltage": 0,
                                "HighVoltage": 0,
                                "LowSoc": 1,
                                "HighChargeCurrent": 0,
                                "HighDischargeCurrent": 0,
                                "HighCurrent": 0,
                                "CellImbalance": 0,
                                "HighChargeTemperature": 0,
                                "LowChargeTemperature": 0,
                                "LowCellVoltage": 0,
                                "LowTemperature": 0,
                                "HighTemperature": 0,
                                "FuseBlown": 0
                            },
                            "Info": {
                                "MaxChargeVoltage": MAX_CHARGE_VOLTAGE,
                                "MaxChargeCurrent": 0,
                                "MaxDischargeCurrent": 0
                            },
                            "History": {},
                            "System": {
                                "NrOfModulesOnline": 0,
                                "NrOfModulesOffline": 1,
                                "NrOfCellsPerBattery": global_info.get("CellCount", NUM_CELLS),
                                "NrOfModulesBlockingCharge": 1,
                                "NrOfModulesBlockingDischarge": 1
                            },
                            "Voltages": {},
                            "Balances": {},
                            "Io": {
                                "AllowToCharge": 0,
                                "AllowToDischarge": 0
                            }
                        }
                        battery_data_list.append(offline_data)
                        if MQTT_ACTIVE:
                            mqtt_publish(offline_data, address, barcodes[address])
                        print(f"No data to publish for address {address} due to parsing error, published offline data")

                if battery_data_list and MQTT_ACTIVE:
                    consolidated_data = create_consolidated_view(battery_data_list)
                    if consolidated_data:
                        mqtt_publish_consolidated(consolidated_data, global_barcode)

                print(f"Stats - Loops: {loops_no}, Errors: {errors_no}, Efficiency: {round((1 - (errors_no / loops_no)) * 100, 2) if loops_no > 0 else 100}%")
                print("-" * 50)
                start_time = time.time()
            time.sleep(0.1)
        except KeyboardInterrupt:
            if ser and ser.is_open:
                ser.close()
                logger.info("Serial port closed")
                print("Serial port closed")
            sys.exit(0)

if __name__ == "__main__":
    main()
