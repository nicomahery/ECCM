import os.path
from threading import Thread
import socket
import socketserver
import datetime
import obd
from obd import utils
from gps3 import gps3
import configparser
import minio
import platform
import subprocess
import time

config = configparser.ConfigParser()
config.read('config.ini')
DATETIME_FORMAT = "%Y%m%d%H%M%S%f"
DEVICE_TIME_LABEL = 'DEVICE_TIME'
S3_ROOT_DIRECTORY = 'car-logs'
SECONDS_BETWEEN_PING = 60
MESSAGE_RETRY_INTERVAL = 50
RECORD_DIRECTORY_LOCATION = config.get('DEFAULT', 'RECORD_DIRECTORY_LOCATION', fallback='.')
CAR_IDENTIFIER = config.get('DEFAULT', 'CAR_IDENTIFIER', fallback=None)
if CAR_IDENTIFIER is None:
    print('NO CAR_IDENTIFIER FOUND')
    print('SHUTDOWN PROGRAM')
    exit(0)
OBD_INTERFACE = config.get('DEFAULT', 'OBD_INTERFACE', fallback=None)
FILE_RECORDING = config['DEFAULT'].getboolean('FILE_RECORDING', fallback=False)
GPS_POSITION_MONITORING = config['DEFAULT'].getboolean('GPS_POSITION_MONITORING', fallback=False)
GEOPOSITION_SERVER_LOCATION = config['DEFAULT'].get('GEOPOSITION_SERVER_LOCATION', fallback=None)
GEOPOSITION_SERVER_ACCESS_KEY = config['DEFAULT'].get('GEOPOSITION_SERVER_ACCESS_KEY', fallback=None)
GEOPOSITION_SERVER_UPDATE_PERIOD = config['DEFAULT'].getfloat('GEOPOSITION_SERVER_ACCESS_KEY', fallback=None)
MONITORING_FILE_EXTENSION = config.get('DEFAULT', 'MONITORING_FILE_EXTENSION', fallback='.tsv')
MONITORING_FILE_SEPARATION_CHARACTER_FALLBACK = '\t' if MONITORING_FILE_EXTENSION == '.tsv' else ',' \
    if MONITORING_FILE_EXTENSION == '.csv' else '|'
MONITORING_FILE_SEPARATION_CHARACTER = config.get('DEFAULT', 'MONITORING_FILE_SEPARATION_CHARACTER',
                                                  fallback=MONITORING_FILE_SEPARATION_CHARACTER_FALLBACK)
S3_SERVER_ENDPOINT = config.get('DEFAULT', 'S3_SERVER_ENDPOINT', fallback=None)
S3_SERVER_AK = config.get('DEFAULT', 'S3_SERVER_AK', fallback=None)
S3_SERVER_SK = config.get('DEFAULT', 'S3_SERVER_SK', fallback=None)
S3_SERVER_BUCKET = config.get('DEFAULT', 'S3_SERVER_BUCKET', fallback=None)
S3_SERVER_REGION = config.get('DEFAULT', 'S3_SERVER_REGION', fallback=None)
ENABLE_SOCKET_SERVER = config['DEFAULT'].getboolean('ENABLE_SOCKET_SERVER', fallback=False)
SOCKET_SERVER_PORT = config.getint('DEFAULT', 'SOCKET_SERVER_PORT', fallback=80)
SOCKET_SERVER_SECRET = config['DEFAULT'].get('SOCKET_SERVER_SECRET', fallback=None)


def ping(host):
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    command = ['ping', param, '1', host]
    return subprocess.call(command) == 0


class SocketTCPHandler(socketserver.BaseRequestHandler):
    data_manager_not_available_message = 'DATA MANAGER NOT AVAILABLE'
    main_manager_not_available_message = 'MAIN MANAGER NOT AVAILABLE'
    authenticated = (SOCKET_SERVER_SECRET is None)

    """
        The request handler class for our server.

        It is instantiated once per connection to the server, and must
        override the handle() method to implement communication to the
        client.
        """

    def query_command(self, command):
        if self.server.main_manager is None or self.server.main_manager.data_manager is None:
            return self.data_manager_not_available_message
        return self.server.main_manager.data_manager.query_command(command)

    def query_status(self, status_type):
        if self.server.main_manager is None or self.server.main_manager.data_manager is None:
            return self.data_manager_not_available_message
        return self.server.main_manager.data_manager.query_status(status_type)

    def get_dtc(self):
        if self.server.main_manager is None or self.server.main_manager.data_manager is None:
            return self.data_manager_not_available_message
        return self.server.main_manager.data_manager.query_dtc()

    def clear_dtc(self):
        if self.server.main_manager is None or self.server.main_manager.data_manager is None:
            return self.data_manager_not_available_message
        return self.server.main_manager.data_manager.clear_dtc()

    def restart_data_manager(self):
        if self.server.main_manager is None:
            return self.main_manager_not_available_message
        self.server.main_manager.restart_data_manager()
        return 'RESTART IN PROGRESS'

    def get_command_list(self):
        if self.server.main_manager is None or self.server.main_manager.data_manager is None \
                or not self.server.main_manager.data_manager.running:
            return self.data_manager_not_available_message
        return ','.join(self.server.main_manager.data_manager.get_command_list())

    def handle(self):
        # self.request is the TCP socket connected to the client
        while True:
            self.data = self.request.recv(1024).decode()
            command = None
            parameter = None
            if not self.data:
                # if data is not received break
                break
            data = str(self.data)
            print("{} wrote:{}".format(self.client_address[0], data))
            if self.authenticated:
                if data.__contains__('>>'):
                    command, parameter = str(self.data).split('>>')
                else:
                    command = data
                switch = {
                    'QUERY_COMMAND': self.query_command(parameter),
                    'QUERY_STATUS': self.query_status(parameter),
                    'GET_DTC': self.get_dtc(),
                    'CLEAR_DTC': self.clear_dtc(),
                    'RESTART_DATA_MANAGER': self.restart_data_manager(),
                    'GET_COMMAND_LIST': self.get_command_list()
                }

                self.request.sendall(str(switch.get(command, 'INVALID COMMAND')).encode())
            else:
                if data == SOCKET_SERVER_SECRET:
                    self.authenticated = True
                else:
                    print('WRONG SECRET')
                    break


class FileSyncManager(Thread):
    excluded_sync_filename = None
    running = False

    def __init__(self, excluded_sync_filename):
        self.excluded_sync_filename = excluded_sync_filename
        super().__init__()

    def run(self):
        self.running = True
        retry_count = 0
        print('FILE SYNC MANAGER STARTED')
        while not ping(S3_SERVER_ENDPOINT):
            retry_count = retry_count + 1
            time.sleep(SECONDS_BETWEEN_PING)
            if retry_count % MESSAGE_RETRY_INTERVAL == 0:
                print(f'S3 SERVER PING RETRY COUNT {retry_count}')
        client = minio.Minio(endpoint=S3_SERVER_ENDPOINT, access_key=S3_SERVER_AK, secret_key=S3_SERVER_SK,
                             region=S3_SERVER_REGION)
        for filename in os.listdir(RECORD_DIRECTORY_LOCATION):
            if filename.endswith(MONITORING_FILE_EXTENSION) and filename != self.excluded_sync_filename:
                client.fput_object(bucket_name=S3_SERVER_BUCKET,
                                   object_name=f'{S3_ROOT_DIRECTORY}/{CAR_IDENTIFIER}/{filename}',
                                   file_path=os.path.join(RECORD_DIRECTORY_LOCATION, filename)
                                   )
                os.remove(os.path.join(RECORD_DIRECTORY_LOCATION, filename))
                print(f'FILE {filename} SYNCED TO S3')
        print('FILE SYNC MANAGER ENDED')
        self.running = False


class GNSSManager(Thread):
    gpsd_socket = None
    data_stream = None
    latitude = None
    longitude = None
    altitude = None
    track = None
    speed = None

    def __init__(self):
        super().__init__()
        if GPS_POSITION_MONITORING:
            self.gpsd_socket = gps3.GPSDSocket()
            self.data_stream = gps3.DataStream()
            self.gpsd_socket.connect()
            self.gpsd_socket.watch()

    def run(self):
        if GPS_POSITION_MONITORING:
            for new_data in self.gpsd_socket:
                if new_data:
                    self.data_stream.unpack(new_data)
                    self.altitude = self.data_stream.TPV['alt']
                    self.longitude = self.data_stream.TPV['lon']
                    self.latitude = self.data_stream.TPV['lat']
                    self.track = self.data_stream.TPV['track']
                    self.speed = self.data_stream.TPV['speed']

    def get_data_for_header_name(self, header):
        switch = {
            'GPS LATITUDE (degree)': self.latitude,
            'GPS LONGITUDE (degree)': self.longitude,
            'GPS ALTITUDE (meter)': self.altitude,
            'GPS SPEED (meter per second)': self.speed,
            'GPS TRACK (degree)': self.track
        }
        return switch.get(header, None)


class DataManager(Thread):
    obd_connection = None
    gnss_manager = None
    deviceTime = None
    running = False
    header_line_written = False

    command_value_dict = {}

    string_to_command_dict = {
        'ENGINE_LOAD': obd.commands.ENGINE_LOAD,
        'COOLANT_TEMP': obd.commands.COOLANT_TEMP,
        'SHORT_FUEL_TRIM_1': obd.commands.SHORT_FUEL_TRIM_1,
        'LONG_FUEL_TRIM_1': obd.commands.LONG_FUEL_TRIM_1,
        'SHORT_FUEL_TRIM_2': obd.commands.SHORT_FUEL_TRIM_2,
        'LONG_FUEL_TRIM_2': obd.commands.LONG_FUEL_TRIM_2,
        'FUEL_PRESSURE': obd.commands.FUEL_PRESSURE,
        'INTAKE_PRESSURE': obd.commands.INTAKE_PRESSURE,
        'RPM': obd.commands.RPM,
        'SPEED': obd.commands.SPEED,
        'TIMING_ADVANCE': obd.commands.TIMING_ADVANCE,
        'INTAKE_TEMP': obd.commands.INTAKE_TEMP,
        'MAF': obd.commands.MAF,
        'THROTTLE_POS': obd.commands.THROTTLE_POS,
        'AIR_STATUS': obd.commands.AIR_STATUS,
        'O2_SENSORS': obd.commands.O2_SENSORS,
        'O2_B1S1': obd.commands.O2_B1S1,
        'O2_B1S2': obd.commands.O2_B1S2,
        'O2_B1S3': obd.commands.O2_B1S3,
        'O2_B1S4': obd.commands.O2_B1S4,
        'O2_B2S1': obd.commands.O2_B2S1,
        'O2_B2S2': obd.commands.O2_B2S2,
        'O2_B2S3': obd.commands.O2_B2S3,
        'O2_B2S4': obd.commands.O2_B2S4,
        'OBD_COMPLIANCE': obd.commands.OBD_COMPLIANCE,
        'O2_SENSORS_ALT': obd.commands.O2_SENSORS_ALT,
        'AUX_INPUT_STATUS': obd.commands.AUX_INPUT_STATUS,
        'RUN_TIME': obd.commands.RUN_TIME,
        'DISTANCE_W_MIL': obd.commands.DISTANCE_W_MIL,
        'FUEL_RAIL_PRESSURE_VAC': obd.commands.FUEL_RAIL_PRESSURE_VAC,
        'FUEL_RAIL_PRESSURE_DIRECT': obd.commands.FUEL_RAIL_PRESSURE_DIRECT,
        'O2_S1_WR_VOLTAGE': obd.commands.O2_S1_WR_VOLTAGE,
        'O2_S2_WR_VOLTAGE': obd.commands.O2_S2_WR_VOLTAGE,
        'O2_S3_WR_VOLTAGE': obd.commands.O2_S3_WR_VOLTAGE,
        'O2_S4_WR_VOLTAGE': obd.commands.O2_S4_WR_VOLTAGE,
        'O2_S5_WR_VOLTAGE': obd.commands.O2_S5_WR_VOLTAGE,
        'O2_S6_WR_VOLTAGE': obd.commands.O2_S6_WR_VOLTAGE,
        'O2_S7_WR_VOLTAGE': obd.commands.O2_S7_WR_VOLTAGE,
        'O2_S8_WR_VOLTAGE': obd.commands.O2_S8_WR_VOLTAGE,
        'COMMANDED_EGR': obd.commands.COMMANDED_EGR,
        'EGR_ERROR': obd.commands.EGR_ERROR,
        'EVAPORATIVE_PURGE': obd.commands.EVAPORATIVE_PURGE,
        'FUEL_LEVEL': obd.commands.FUEL_LEVEL,
        'WARMUPS_SINCE_DTC_CLEAR': obd.commands.WARMUPS_SINCE_DTC_CLEAR,
        'DISTANCE_SINCE_DTC_CLEAR': obd.commands.DISTANCE_SINCE_DTC_CLEAR,
        'EVAP_VAPOR_PRESSURE': obd.commands.EVAP_VAPOR_PRESSURE,
        'BAROMETRIC_PRESSURE': obd.commands.BAROMETRIC_PRESSURE,
        'O2_S1_WR_CURRENT': obd.commands.O2_S1_WR_CURRENT,
        'O2_S2_WR_CURRENT': obd.commands.O2_S2_WR_CURRENT,
        'O2_S3_WR_CURRENT': obd.commands.O2_S3_WR_CURRENT,
        'O2_S4_WR_CURRENT': obd.commands.O2_S4_WR_CURRENT,
        'O2_S5_WR_CURRENT': obd.commands.O2_S5_WR_CURRENT,
        'O2_S6_WR_CURRENT': obd.commands.O2_S6_WR_CURRENT,
        'O2_S7_WR_CURRENT': obd.commands.O2_S7_WR_CURRENT,
        'O2_S8_WR_CURRENT': obd.commands.O2_S8_WR_CURRENT,
        'CATALYST_TEMP_B1S1': obd.commands.CATALYST_TEMP_B1S1,
        'CATALYST_TEMP_B2S1': obd.commands.CATALYST_TEMP_B2S1,
        'CATALYST_TEMP_B1S2': obd.commands.CATALYST_TEMP_B1S2,
        'CATALYST_TEMP_B2S2': obd.commands.CATALYST_TEMP_B2S2,
        'CONTROL_MODULE_VOLTAGE': obd.commands.CONTROL_MODULE_VOLTAGE,
        'ABSOLUTE_LOAD': obd.commands.ABSOLUTE_LOAD,
        'COMMANDED_EQUIV_RATIO': obd.commands.COMMANDED_EQUIV_RATIO,
        'RELATIVE_THROTTLE_POS': obd.commands.RELATIVE_THROTTLE_POS,
        'AMBIANT_AIR_TEMP': obd.commands.AMBIANT_AIR_TEMP,
        'THROTTLE_POS_B': obd.commands.THROTTLE_POS_B,
        'THROTTLE_POS_C': obd.commands.THROTTLE_POS_C,
        'ACCELERATOR_POS_D': obd.commands.ACCELERATOR_POS_D,
        'ACCELERATOR_POS_E': obd.commands.ACCELERATOR_POS_E,
        'ACCELERATOR_POS_F': obd.commands.ACCELERATOR_POS_F,
        'THROTTLE_ACTUATOR': obd.commands.THROTTLE_ACTUATOR,
        'RUN_TIME_MIL': obd.commands.RUN_TIME_MIL,
        'TIME_SINCE_DTC_CLEARED': obd.commands.TIME_SINCE_DTC_CLEARED,
        'MAX_MAF': obd.commands.MAX_MAF,
        'FUEL_TYPE': obd.commands.FUEL_TYPE,
        'ETHANOL_PERCENT': obd.commands.ETHANOL_PERCENT,
        'EVAP_VAPOR_PRESSURE_ABS': obd.commands.EVAP_VAPOR_PRESSURE_ABS,
        'EVAP_VAPOR_PRESSURE_ALT': obd.commands.EVAP_VAPOR_PRESSURE_ALT,
        'SHORT_O2_TRIM_B1': obd.commands.SHORT_O2_TRIM_B1,
        'LONG_O2_TRIM_B1': obd.commands.LONG_O2_TRIM_B1,
        'SHORT_O2_TRIM_B2': obd.commands.SHORT_O2_TRIM_B2,
        'LONG_O2_TRIM_B2': obd.commands.LONG_O2_TRIM_B2,
        'FUEL_RAIL_PRESSURE_ABS': obd.commands.FUEL_RAIL_PRESSURE_ABS,
        'RELATIVE_ACCEL_POS': obd.commands.RELATIVE_ACCEL_POS,
        'HYBRID_BATTERY_REMAINING': obd.commands.HYBRID_BATTERY_REMAINING,
        'OIL_TEMP': obd.commands.OIL_TEMP,
        'FUEL_INJECT_TIMING': obd.commands.FUEL_INJECT_TIMING,
        'FUEL_RATE': obd.commands.FUEL_RATE,
    }

    command_to_string_header_dict = {
        obd.commands.ENGINE_LOAD: "ENGINE LOAD (percent)",
        obd.commands.COOLANT_TEMP: "COOLANT TEMPERATURE",
        obd.commands.SHORT_FUEL_TRIM_1: "SHORT FUEL TRIM 1 (percent)",
        obd.commands.LONG_FUEL_TRIM_1: "LONG FUEL TRIM 1 (percent)",
        obd.commands.SHORT_FUEL_TRIM_2: "SHORT FUEL TRIM 2 (percent)",
        obd.commands.LONG_FUEL_TRIM_2: "LONG FUEL TRIM 2 (percent)",
        obd.commands.FUEL_PRESSURE: "FUEL PRESSURE (kilopascal)",
        obd.commands.INTAKE_PRESSURE: "INTAKE_PRESSURE (kilopascal)",
        obd.commands.RPM: "RPM",
        obd.commands.SPEED: "SPEED (kilometers per hour)",
        obd.commands.TIMING_ADVANCE: "TIMING ADVANCE (degree)",
        obd.commands.INTAKE_TEMP: "INTAKE TEMPERATURE (degree celsius)",
        obd.commands.MAF: "Air Flow Rate (grams per second)",
        obd.commands.THROTTLE_POS: "THROTTLE POSITION (percent)",
        obd.commands.AIR_STATUS: "AIR STATUS (status)",
        obd.commands.O2_SENSORS: "O2 SENSORS (sensors)",
        obd.commands.O2_B1S1: "O2 BANK1 SENSOR1 (volt)",
        obd.commands.O2_B1S2: "O2 BANK1 SENSOR2 (volt)",
        obd.commands.O2_B1S3: "O2 BANK1 SENSOR3 (volt)",
        obd.commands.O2_B1S4: "O2 BANK1 SENSOR4 (volt)",
        obd.commands.O2_B2S1: "O2 BANK2 SENSOR1 (volt)",
        obd.commands.O2_B2S2: "O2 BANK2 SENSOR2 (volt)",
        obd.commands.O2_B2S3: "O2 BANK2 SENSOR3 (volt)",
        obd.commands.O2_B2S4: "O2 BANK2 SENSOR4 (volt)",
        obd.commands.OBD_COMPLIANCE: "OBD COMPLIANCE",
        obd.commands.O2_SENSORS_ALT: "O2 SENSORS PRESENT ALTERNATE",
        obd.commands.AUX_INPUT_STATUS: "AUX INPUT STATUS (boolean)",
        obd.commands.RUN_TIME: "ENGINE RUN TIME (second)",
        obd.commands.DISTANCE_W_MIL: "DISTANCE TRAVELED WITH MIL on (kilometer)",
        obd.commands.FUEL_RAIL_PRESSURE_VAC: "FUEL RAIL PRESSURE RELATIVE TO VACUUM (kilopascal)",
        obd.commands.FUEL_RAIL_PRESSURE_DIRECT: "FUEL RAIL PRESSURE DIRECT INJECTION (kilopascal)",
        obd.commands.O2_S1_WR_VOLTAGE: "O2 SENSOR1 WR LAMBDA VOLTAGE (volt)",
        obd.commands.O2_S2_WR_VOLTAGE: "O2 SENSOR2 WR LAMBDA VOLTAGE (volt)",
        obd.commands.O2_S3_WR_VOLTAGE: "O2 SENSOR3 WR LAMBDA VOLTAGE (volt)",
        obd.commands.O2_S4_WR_VOLTAGE: "O2 SENSOR4 WR LAMBDA VOLTAGE (volt)",
        obd.commands.O2_S5_WR_VOLTAGE: "O2 SENSOR5 WR LAMBDA VOLTAGE (volt)",
        obd.commands.O2_S6_WR_VOLTAGE: "O2 SENSOR6 WR LAMBDA VOLTAGE (volt)",
        obd.commands.O2_S7_WR_VOLTAGE: "O2 SENSOR7 WR LAMBDA VOLTAGE (volt)",
        obd.commands.O2_S8_WR_VOLTAGE: "O2 SENSOR8 WR LAMBDA VOLTAGE (volt)",
        obd.commands.COMMANDED_EGR: "COMMANDED EGR (percent)",
        obd.commands.EGR_ERROR: "EGR ERROR (percent)",
        obd.commands.EVAPORATIVE_PURGE: "EVAPORATIVE PURGE (percent)",
        obd.commands.FUEL_LEVEL: "FUEL LEVEL (percent)",
        obd.commands.WARMUPS_SINCE_DTC_CLEAR: "WARMUPS SINCE DTC CLEAR (count)",
        obd.commands.DISTANCE_SINCE_DTC_CLEAR: "DISTANCE SINCE DTC CLEAR (kilometer)",
        obd.commands.EVAP_VAPOR_PRESSURE: "EVAPORATIVE VAPOR PRESSURE (pascal)",
        obd.commands.BAROMETRIC_PRESSURE: "BAROMETRIC PRESSURE (kilopascal)",
        obd.commands.O2_S1_WR_CURRENT: "O2 SENSOR1 WR CURRENT (milliampere)",
        obd.commands.O2_S2_WR_CURRENT: "O2 SENSOR2 WR CURRENT (milliampere)",
        obd.commands.O2_S3_WR_CURRENT: "O2 SENSOR3 WR CURRENT (milliampere)",
        obd.commands.O2_S4_WR_CURRENT: "O2 SENSOR4 WR CURRENT (milliampere)",
        obd.commands.O2_S5_WR_CURRENT: "O2 SENSOR5 WR CURRENT (milliampere)",
        obd.commands.O2_S6_WR_CURRENT: "O2 SENSOR6 WR CURRENT (milliampere)",
        obd.commands.O2_S7_WR_CURRENT: "O2 SENSOR7 WR CURRENT (milliampere)",
        obd.commands.O2_S8_WR_CURRENT: "O2 SENSOR8 WR CURRENT (milliampere)",
        obd.commands.CATALYST_TEMP_B1S1: "CATALYST TEMPERATURE BANK1 SENSOR1 (celsius)",
        obd.commands.CATALYST_TEMP_B2S1: "CATALYST TEMPERATURE BANK2 SENSOR1 (celsius)",
        obd.commands.CATALYST_TEMP_B1S2: "CATALYST TEMPERATURE BANK1 SENSOR2 (celsius)",
        obd.commands.CATALYST_TEMP_B2S2: "CATALYST TEMPERATURE BANK2 SENSOR2 (celsius)",
        obd.commands.CONTROL_MODULE_VOLTAGE: "CONTROL MODULE VOLTAGE (volt)",
        obd.commands.ABSOLUTE_LOAD: "ABSOLUTE LOAD (percent)",
        obd.commands.COMMANDED_EQUIV_RATIO: "COMMANDED EQUIVALENCE RATIO (ratio)",
        obd.commands.RELATIVE_THROTTLE_POS: "RELATIVE THROTTLE POSITION (percent)",
        obd.commands.AMBIANT_AIR_TEMP: "AMBIANT AIR TEMPERATURE (degree celsius)",
        obd.commands.THROTTLE_POS_B: "ABSOLUTE THROTTLE POSITION B (percent)",
        obd.commands.THROTTLE_POS_C: "ABSOLUTE THROTTLE POSITION C (percent)",
        obd.commands.ACCELERATOR_POS_D: "ACCELERATOR PEDAL POSITION D (percent)",
        obd.commands.ACCELERATOR_POS_E: "ACCELERATOR PEDAL POSITION E (percent)",
        obd.commands.ACCELERATOR_POS_F: "ACCELERATOR PEDAL POSITION F (percent)",
        obd.commands.THROTTLE_ACTUATOR: "THROTTLE ACTUATOR (percent)",
        obd.commands.RUN_TIME_MIL: "RUN TIME MIL (minute)",
        obd.commands.TIME_SINCE_DTC_CLEARED: "TIME SINCE DTC CLEARED (minute)",
        obd.commands.MAX_MAF: "MAX MASS AIR FLOW (grams per second)",
        obd.commands.FUEL_TYPE: "FUEL TYPE",
        obd.commands.ETHANOL_PERCENT: "ETHANOL PERCENT (percent)",
        obd.commands.EVAP_VAPOR_PRESSURE_ABS: "ABSOLUTE EVAPORATIVE VAPOR PRESSURE (kilopascal)",
        obd.commands.EVAP_VAPOR_PRESSURE_ALT: "EVAPORATIVE VAPOR PRESSURE (pascal)",
        obd.commands.SHORT_O2_TRIM_B1: "SHORT O2 TRIM BANK1 (percent)",
        obd.commands.LONG_O2_TRIM_B1: "LONG O2 TRIM BANK1 (percent)",
        obd.commands.SHORT_O2_TRIM_B2: "SHORT O2 TRIM BANK2 (percent)",
        obd.commands.LONG_O2_TRIM_B2: "LONG O2 TRIM BANK2 (percent)",
        obd.commands.FUEL_RAIL_PRESSURE_ABS: "ABSOLUTE FUEL RAIL PRESSURE (kilopascal)",
        obd.commands.RELATIVE_ACCEL_POS: "RELATIVE ACCELELERATOR PEDAL POSITION (percent)",
        obd.commands.HYBRID_BATTERY_REMAINING: "HYBRID BATTERY REMAINING (percent)",
        obd.commands.OIL_TEMP: "OIL TEMPERATURE (degree celsius)",
        obd.commands.FUEL_INJECT_TIMING: "FUEL INJECTION TIMING (degree)",
        obd.commands.FUEL_RATE: "FUEL RATE (liters per hour)"
    }

    command_list = [obd.commands.ENGINE_LOAD,
                    obd.commands.COOLANT_TEMP,
                    obd.commands.SHORT_FUEL_TRIM_1,
                    obd.commands.LONG_FUEL_TRIM_1,
                    obd.commands.SHORT_FUEL_TRIM_2,
                    obd.commands.LONG_FUEL_TRIM_2,
                    obd.commands.FUEL_PRESSURE,
                    obd.commands.INTAKE_PRESSURE,
                    obd.commands.RPM,
                    obd.commands.SPEED,
                    obd.commands.TIMING_ADVANCE,
                    obd.commands.INTAKE_TEMP,
                    obd.commands.MAF,
                    obd.commands.THROTTLE_POS,
                    obd.commands.AIR_STATUS,
                    obd.commands.O2_SENSORS,
                    obd.commands.O2_B1S1,
                    obd.commands.O2_B1S2,
                    obd.commands.O2_B1S3,
                    obd.commands.O2_B1S4,
                    obd.commands.O2_B2S1,
                    obd.commands.O2_B2S2,
                    obd.commands.O2_B2S3,
                    obd.commands.O2_B2S4,
                    obd.commands.OBD_COMPLIANCE,
                    obd.commands.O2_SENSORS_ALT,
                    obd.commands.AUX_INPUT_STATUS,
                    obd.commands.RUN_TIME,
                    obd.commands.DISTANCE_W_MIL,
                    obd.commands.FUEL_RAIL_PRESSURE_VAC,
                    obd.commands.FUEL_RAIL_PRESSURE_DIRECT,
                    obd.commands.O2_S1_WR_VOLTAGE,
                    obd.commands.O2_S2_WR_VOLTAGE,
                    obd.commands.O2_S3_WR_VOLTAGE,
                    obd.commands.O2_S4_WR_VOLTAGE,
                    obd.commands.O2_S5_WR_VOLTAGE,
                    obd.commands.O2_S6_WR_VOLTAGE,
                    obd.commands.O2_S7_WR_VOLTAGE,
                    obd.commands.O2_S8_WR_VOLTAGE,
                    obd.commands.COMMANDED_EGR,
                    obd.commands.EGR_ERROR,
                    obd.commands.EVAPORATIVE_PURGE,
                    obd.commands.FUEL_LEVEL,
                    obd.commands.WARMUPS_SINCE_DTC_CLEAR,
                    obd.commands.DISTANCE_SINCE_DTC_CLEAR,
                    obd.commands.EVAP_VAPOR_PRESSURE,
                    obd.commands.BAROMETRIC_PRESSURE,
                    obd.commands.O2_S1_WR_CURRENT,
                    obd.commands.O2_S2_WR_CURRENT,
                    obd.commands.O2_S3_WR_CURRENT,
                    obd.commands.O2_S4_WR_CURRENT,
                    obd.commands.O2_S5_WR_CURRENT,
                    obd.commands.O2_S6_WR_CURRENT,
                    obd.commands.O2_S7_WR_CURRENT,
                    obd.commands.O2_S8_WR_CURRENT,
                    obd.commands.CATALYST_TEMP_B1S1,
                    obd.commands.CATALYST_TEMP_B2S1,
                    obd.commands.CATALYST_TEMP_B1S2,
                    obd.commands.CATALYST_TEMP_B2S2,
                    obd.commands.CONTROL_MODULE_VOLTAGE,
                    obd.commands.ABSOLUTE_LOAD,
                    obd.commands.COMMANDED_EQUIV_RATIO,
                    obd.commands.RELATIVE_THROTTLE_POS,
                    obd.commands.AMBIANT_AIR_TEMP,
                    obd.commands.THROTTLE_POS_B,
                    obd.commands.THROTTLE_POS_C,
                    obd.commands.ACCELERATOR_POS_D,
                    obd.commands.ACCELERATOR_POS_E,
                    obd.commands.ACCELERATOR_POS_F,
                    obd.commands.THROTTLE_ACTUATOR,
                    obd.commands.RUN_TIME_MIL,
                    obd.commands.TIME_SINCE_DTC_CLEARED,
                    obd.commands.MAX_MAF,
                    obd.commands.FUEL_TYPE,
                    obd.commands.ETHANOL_PERCENT,
                    obd.commands.EVAP_VAPOR_PRESSURE_ABS,
                    obd.commands.EVAP_VAPOR_PRESSURE_ALT,
                    obd.commands.SHORT_O2_TRIM_B1,
                    obd.commands.LONG_O2_TRIM_B1,
                    obd.commands.SHORT_O2_TRIM_B2,
                    obd.commands.LONG_O2_TRIM_B2,
                    obd.commands.FUEL_RAIL_PRESSURE_ABS,
                    obd.commands.RELATIVE_ACCEL_POS,
                    obd.commands.HYBRID_BATTERY_REMAINING,
                    obd.commands.OIL_TEMP,
                    obd.commands.FUEL_INJECT_TIMING,
                    obd.commands.FUEL_RATE]

    gps_data_label_list = [
        'GPS LATITUDE (degree)',
        'GPS LONGITUDE (degree)',
        'GPS ALTITUDE (meter)',
        'GPS SPEED (meter per second)',
        'GPS TRACK (degree)'
    ]

    string_to_status_dict = {
        'FUEL_STATUS': obd.commands.FUEL_STATUS,
        'AIR_STATUS': obd.commands.AIR_STATUS,
        'AUX_INPUT_STATUS': obd.commands.AUX_INPUT_STATUS,
    }

    status_list = [
        obd.commands.FUEL_STATUS,
        obd.commands.AIR_STATUS,
        obd.commands.AUX_INPUT_STATUS,
    ]

    def __init__(self, gnss_manager):
        super().__init__()
        self.gnss_manager = gnss_manager

    def value_callback(self, response):
        self.command_value_dict[response.command] = response.value

    def run(self):
        self.running = False
        print('START NEW OBD CONNECTION')
        self.obd_connection = obd.Async(OBD_INTERFACE)
        time.sleep(2)
        wait_count = 1
        filename = os.path.join(RECORD_DIRECTORY_LOCATION, self.get_device_time_string() + MONITORING_FILE_EXTENSION)
        if S3_SERVER_ENDPOINT is not None and S3_SERVER_AK is not None and S3_SERVER_SK is not None \
                and S3_SERVER_BUCKET is not None and S3_SERVER_REGION is not None:
            file_sync_manager = FileSyncManager(excluded_sync_filename=filename)
            file_sync_manager.start()

        while self.obd_connection.status() == utils.OBDStatus.NOT_CONNECTED:
            print('OBD NOT CONNECTED')
            if wait_count > 60:
                self.obd_connection.close()
                print('FAILED TO CREATE OBD CONNECTION')
                return
            print('RETRY CONNECTION N°' + str(wait_count))
            wait_count = wait_count + 1
            time.sleep(1)

            if not self.obd_connection.is_connected():
                print('CLOSE PREVIOUS OBD CONNECTION')
                self.obd_connection.close()
                time.sleep(7)
                print('RETRY STARTING NEW OBD CONNECTION')
                self.obd_connection = obd.Async(OBD_INTERFACE)
                time.sleep(2)

        print('TESTING OBD CONNECTION')
        print(str(self.obd_connection.supported_commands))

        for status in self.status_list:
            print('TESTING OBD STATUS: ' + str(status))
            status_supported = self.obd_connection.supports(status)
            print(str(status_supported))

            if not status_supported:
                print('UNSUPPORTED OBD COMMAND: ' + str(status))
                print(str(self.obd_connection.query(status, force=True).value))
                self.status_list.remove(status)

        for command in self.command_list:
            print('TESTING OBD COMMAND: ' + str(command))
            command_supported = self.obd_connection.supports(command)
            print(str(command_supported))
            if not command_supported:
                print('UNSUPPORTED OBD COMMAND: ' + str(command))
                print(str(self.obd_connection.query(command, force=True).value))
                self.command_list.remove(command)
                # connection_complete = False
                # print('CLOSE PREVIOUS OBD CONNECTION')
                # self.obd_connection.close()
                # time.sleep(4)
                # break
        if self.command_list.__len__() < 1:
            print('NO OBD COMMAND SUPPORTED')
            print('CLOSE PREVIOUS OBD CONNECTION')
            self.obd_connection.close()
        else:
            for command in self.command_list:
                self.obd_connection.watch(command)  # , callback=self.value_callback)

            for status in self.status_list:
                self.obd_connection.watch(status)

            self.obd_connection.start()
            print(f'CONNECTED TO ECU')
            print(f'NUMBER OF COMMAND MONITORED:{self.command_list.__len__()}')
            print('START MONITORING ECU DATA')
            self.running = True

            if FILE_RECORDING:
                if not os.path.exists(RECORD_DIRECTORY_LOCATION):
                    os.makedirs(RECORD_DIRECTORY_LOCATION)

                self.write_record_line_to_file(filename, True)
                while self.running:
                    if self.header_line_written:
                        self.write_record_line_to_file(filename, False)
                        time.sleep(0.5)
                    else:
                        time.sleep(0.1)

            else:
                while self.running:
                    time.sleep(0.5)

            self.obd_connection.stop()
            self.obd_connection.unwatch_all()
            print('CLOSE USED OBD CONNECTION')
            self.obd_connection.close()

    def write_record_line_to_file(self, filename, write_header):
        file = open(filename, 'a')
        file.write(self.get_command_record(write_header))
        file.close()
        self.header_line_written = (self.header_line_written or write_header)

    def terminate(self):
        self.running = False

    def query_command(self, string_command):
        command = self.string_to_command_dict.get(string_command)
        if command is None or not self.command_list.__contains__(command) or not self.running:
            return None
        return self.obd_connection.query(command)

    def query_status(self, string_status):
        status = self.string_to_status_dict.get(string_status)
        if status is None or not self.running:
            return None
        return self.obd_connection.query(status)

    def query_dtc(self):
        if not self.running:
            return None
        return self.obd_connection.query(obd.commands.GET_DTC)

    def clear_dtc(self):
        if not self.running:
            return None
        return self.obd_connection.query(obd.commands.CLEAR_DTC)

    def get_command_record(self, write_header):
        if not self.running or write_header is None:
            return None
        ret = ''
        if write_header:
            ret = DEVICE_TIME_LABEL
            for command in self.command_list:
                ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{self.command_to_string_header_dict.get(command)}'

            if GPS_POSITION_MONITORING:
                for gps_label in self.gps_data_label_list:
                    ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{gps_label}'
            return ret + '\n'
        else:
            ret = self.get_device_time_string()
            for command in self.command_list:
                ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{self.obd_connection.query(command).value}'

            if GPS_POSITION_MONITORING:
                for gps_label in self.gps_data_label_list:
                    ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{self.gnss_manager.get_data_for_header_name(gps_label)}'
            return ret + '\n'

    def get_command_list(self):
        return [self.command_to_string_header_dict[command] for command in self.command_list]

    @staticmethod
    def get_device_time_string():
        return datetime.datetime.now().strftime(DATETIME_FORMAT)


class MainManager(Thread):
    running = False
    gnss_manager = None
    data_manager = None

    def __init__(self):
        super().__init__()

    def run(self):
        self.running = True
        self.start_gnss_manager()
        self.restart_data_manager()

    def start_gnss_manager(self):
        self.gnss_manager = GNSSManager()
        self.gnss_manager.start()

    def restart_data_manager(self):
        if self.data_manager is not None:
            self.data_manager.terminate()
            self.data_manager.join()
        self.data_manager = DataManager(self.gnss_manager)
        self.data_manager.start()


if __name__ == '__main__':
    main_manager = MainManager()
    main_manager.start()
    if ENABLE_SOCKET_SERVER and SOCKET_SERVER_PORT is not None:
        with socketserver.TCPServer((socket.gethostname(), SOCKET_SERVER_PORT), SocketTCPHandler) as server:
            server.main_manager = main_manager
            server.serve_forever()
