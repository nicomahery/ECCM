import os.path
from threading import Thread
import queue
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
import requests
import logging
import socketio
from aiohttp import web

config = configparser.ConfigParser()
config.read('config.ini')
DATETIME_FORMAT = "%Y%m%d%H%M%S%f"
DEVICE_TIME_LABEL = 'DEVICE_TIME'
S3_ROOT_DIRECTORY = 'car-logs'
POST_TRIP_LOCATION = 'api/carloguploads/'
SECONDS_BETWEEN_PING = 60
MESSAGE_RETRY_INTERVAL = 50
RECORD_DIRECTORY_LOCATION = config.get('DEFAULT', 'RECORD_DIRECTORY_LOCATION', fallback='.')
CAR_IDENTIFIER = config.get('DEFAULT', 'CAR_IDENTIFIER', fallback=None)
if CAR_IDENTIFIER is None:
    logging.error('NO CAR_IDENTIFIER FOUND')
    logging.info('SHUTDOWN PROGRAM')
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
ECCM_SERVER_LOCATION = config['DEFAULT'].get('ECCM_SERVER_LOCATION', fallback=None)
ECCM_SECRET_HEADER = config['DEFAULT'].get('ECCM_SECRET_HEADER', fallback=None)
ECCM_SECRET_VALUE = config['DEFAULT'].get('ECCM_SECRET_VALUE', fallback=None)


def ping(host):
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    command = ['ping', param, '1', host]
    return subprocess.call(command) == 0


class SocketServer:
    main_manager = None
    socket_io_server = None
    data_manager_not_available_message = 'DATA MANAGER NOT AVAILABLE'
    main_manager_not_available_message = 'MAIN MANAGER NOT AVAILABLE'

    def __init__(self, manager):
        self.main_manager = manager
        self.socket_io_server = socketio.AsyncServer()
        app = web.Application()
        self.socket_io_server.attach(app)

        @self.socket_io_server.event
        async def connect(sid, environ, auth):
            print("connected ", sid)
            print("auth", auth)
            if not auth['secret'] == SOCKET_SERVER_SECRET:
                await self.socket_io_server.disconnect(sid)

        @self.socket_io_server.event
        async def disconnect(sid):
            print("disconnected ", sid)

        @self.socket_io_server.event
        async def query_command(sid, data):
            response = self.query_command(data)
            if response is not None:
                response = response.value
            return response

        @self.socket_io_server.event
        async def query_status(sid, data):
            response = self.query_status(data)
            if response is not None:
                response = response.value
            return response

        @self.socket_io_server.event
        async def get_dtc(sid, data):
            response = self.get_dtc()
            if response is not None:
                response = response.value
            return response

        @self.socket_io_server.event
        async def clear_dtc(sid, data):
            self.clear_dtc()
            return "OK"

        web.run_app(app, host='0.0.0.0', port=SOCKET_SERVER_PORT)

    def query_command(self, command):
        return self.main_manager.query_command(command)

    def query_status(self, status_type):
        return self.main_manager.query_status(status_type)

    def get_dtc(self):
        return self.main_manager.query_dtc()

    def clear_dtc(self):
        return self.main_manager.clear_dtc()

    def restart_data_manager(self, sync=False):
        self.main_manager.restart_data_manager(sync)
        return 'RESTART IN PROGRESS'

    def stop_data_manager(self, sync=False):
        self.main_manager.stop_data_manager(sync)
        return 'RESTART IN PROGRESS'

    def get_command_list(self):
        return ','.join(self.main_manager.get_command_list())

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
                    'STOP_DATA_MANAGER': self.stop_data_manager(sync=(parameter == 'SYNC')),
                    'RESTART_DATA_MANAGER': self.restart_data_manager(sync=(parameter == 'SYNC')),
                    'GET_COMMAND_LIST': self.get_command_list()
                }

                self.request.sendall(str(switch.get(command, 'INVALID COMMAND')).encode())
            else:
                if data == SOCKET_SERVER_SECRET:
                    self.authenticated = True
                else:
                    logging.warning('WRONG SECRET')
                    break


class FileSyncManager(Thread):
    excluded_sync_filename = None
    running = False

    def __init__(self, excluded_sync_filename=None):
        self.excluded_sync_filename = excluded_sync_filename
        super().__init__()

    def run(self):
        self.running = True
        retry_count = 0
        logging.info('FILE SYNC MANAGER STARTED')
        while not ping(S3_SERVER_ENDPOINT):
            retry_count = retry_count + 1
            time.sleep(SECONDS_BETWEEN_PING)
            if retry_count % MESSAGE_RETRY_INTERVAL == 0:
                logging.warning(f'S3 SERVER PING RETRY COUNT {retry_count}')
        s3_file_location_list = []
        client = minio.Minio(endpoint=S3_SERVER_ENDPOINT, access_key=S3_SERVER_AK, secret_key=S3_SERVER_SK,
                             region=S3_SERVER_REGION)
        for filename in os.listdir(RECORD_DIRECTORY_LOCATION):
            if filename.endswith(MONITORING_FILE_EXTENSION) and filename != self.excluded_sync_filename:
                try:
                    result = client.fput_object(bucket_name=S3_SERVER_BUCKET,
                                                object_name=f'{S3_ROOT_DIRECTORY}/{CAR_IDENTIFIER}/{filename}',
                                                file_path=os.path.join(RECORD_DIRECTORY_LOCATION, filename)
                                                )
                    os.remove(os.path.join(RECORD_DIRECTORY_LOCATION, filename))
                    logging.info(f'FILE {filename} SYNCED TO S3')
                    s3_file_location_list.append(result.object_name)
                except Exception as exc:
                    logging.error(f'FILE {filename} SYNC FAILED')
                    logging.error(f'Error: {exc}')
        thread_list = []
        if ECCM_SERVER_LOCATION is not None and ECCM_SECRET_VALUE is not None and ECCM_SECRET_HEADER is not None:
            for location in s3_file_location_list:
                data = {"objectLocation": location, "uploadDate": "2022-05-05T07:17:00"}
                thread = Thread(target=post_to_eccm_server, args=(POST_TRIP_LOCATION, data, location,))
                thread.start()
                thread_list.append(thread)

            for thread in thread_list:
                thread.join()

        logging.info('FILE SYNC MANAGER ENDED')
        self.running = False


def post_to_eccm_server(url, data, location):
    response = requests.Response()
    while response.status_code != 200:
        response = requests.post(f'{ECCM_SERVER_LOCATION}/{url}',
                                 headers={ECCM_SECRET_HEADER: ECCM_SECRET_VALUE,
                                          "Content-Type": "application/json; charset=utf-8"},
                                 json=data)
        if response.status_code != 200:
            logging.warning(f'FILE {location} IMPORTATION FAILED RETRY IN 2.5 SECONDS')
            time.sleep(2.5)

    logging.info(f'FILE {location} IMPORTED INTO ECCM SERVER')


class FileUpdateManager(Thread):
    filename = None
    running = False
    first_line_written = False
    header_overwritten = False
    q = None
    header = None

    def __init__(self, filename, q, header):
        self.filename = filename
        self.q = q
        self.header = header
        super().__init__()

    def run(self):
        self.running = True
        while self.running:
            time.sleep(2)
            if self.first_line_written and not self.header_overwritten:
                self.override_header()
                self.header_overwritten = True
            if not self.q.qsize() == 0:
                file = open(self.filename, 'a')
                while not self.q.qsize() == 0:
                    try:
                        file.write(self.q.get_nowait())
                        self.q.task_done()
                        if not self.first_line_written:
                            self.first_line_written = True

                    except queue.Empty:
                        logging.info('file queue already empty')

                file.close()

    def override_header(self):
        with open(self.filename, 'r+') as f:
            if not f.readline().rstrip('\r\n') == self.header.rstrip('\r\n'):
                f.seek(0, 0)
                content = f.read()
                f.seek(0, 0)
                f.write(self.header.rstrip('\r\n') + '\n' + content)

    def terminate(self):
        self.running = False


class GNSSManager(Thread):
    gpsd_socket = None
    data_stream = None
    latitude = None
    longitude = None
    altitude = None
    track = None
    speed = None
    stop_flag = False

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
                if self.stop_flag:
                    break

    def get_data_for_header_name(self, header):
        switch = {
            'GPS LATITUDE (degree)': self.latitude,
            'GPS LONGITUDE (degree)': self.longitude,
            'GPS ALTITUDE (meter)': self.altitude,
            'GPS SPEED (meter per second)': self.speed,
            'GPS TRACK (degree)': self.track
        }
        return switch.get(header, None)

    def terminate(self):
        self.stop_flag = True
        self.gpsd_socket.close()


class DataManager(Thread):
    obd_connection = None
    gnss_manager = None
    deviceTime = None
    running = False
    sync_before_terminate = False
    header_line_written = False
    q = None
    fileUpdateManager = None
    car_id = None
    trip_id = None
    command_list = None
    string_to_command_dict = None
    string_to_status_dict = None
    global_label_list = None
    command_to_string_header_dict = None
    gps_data_label_list = None

    def __init__(self, gnss_manager, car_id, obd_connection, command_list, string_to_command_dict,
                 string_to_status_dict,
                 global_label_list, command_to_string_header_dict, gps_data_label_list):
        super().__init__()
        self.car_id = car_id
        self.gnss_manager = gnss_manager
        self.obd_connection = obd_connection
        self.command_list = command_list
        self.string_to_command_dict = string_to_command_dict
        self.string_to_status_dict = string_to_status_dict
        self.global_label_list = global_label_list
        self.command_to_string_header_dict = command_to_string_header_dict
        self.gps_data_label_list = gps_data_label_list

    def run(self):
        file_sync_manager = None
        self.running = False
        self.trip_id = self.get_device_time_string()
        filename = os.path.join(RECORD_DIRECTORY_LOCATION, self.trip_id + MONITORING_FILE_EXTENSION)
        if S3_SERVER_ENDPOINT is not None and S3_SERVER_AK is not None and S3_SERVER_SK is not None \
                and S3_SERVER_BUCKET is not None and S3_SERVER_REGION is not None:
            file_sync_manager = FileSyncManager(excluded_sync_filename=filename)
            file_sync_manager.start()

        self.running = True

        if FILE_RECORDING:
            if not os.path.exists(RECORD_DIRECTORY_LOCATION):
                os.makedirs(RECORD_DIRECTORY_LOCATION)
            self.q = queue.Queue()
            self.fileUpdateManager = FileUpdateManager(filename, self.q, self.get_command_record(True))
            self.fileUpdateManager.start()
            self.write_record_line_to_file(True)

            while self.running:
                self.write_record_line_to_file(False)
                time.sleep(0.5)

            self.fileUpdateManager.terminate()
            self.fileUpdateManager.join()

            if self.sync_before_terminate and S3_SERVER_ENDPOINT is not None and S3_SERVER_AK is not None and \
                    S3_SERVER_SK is not None and S3_SERVER_BUCKET is not None and S3_SERVER_REGION is not None:
                if file_sync_manager is not None:
                    file_sync_manager.join()

                logging.info('START SYNC BEFORE DATA MANAGER SHUTDOWN')
                file_sync_manager = FileSyncManager()
                file_sync_manager.start()
                file_sync_manager.join()
            logging.info('CARLOG FILE CLOSED')

        else:
            while self.running:
                time.sleep(0.5)

    def write_record_line_to_file(self, write_header):
        try:
            self.q.put_nowait(self.get_command_record(write_header))
        except queue.Full:
            logging.warning('file queue full')

    def terminate(self, sync):
        self.sync_before_terminate = sync
        self.running = False

    def get_command_record(self, write_header):
        if not self.running or write_header is None:
            return None

        if write_header:
            ret = DEVICE_TIME_LABEL
            for header in self.global_label_list:
                ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{header}'

            for command in self.command_list:
                ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{self.command_to_string_header_dict.get(command)}'

            if GPS_POSITION_MONITORING:
                for gps_label in self.gps_data_label_list:
                    ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{gps_label}'
            return ret + '\n'
        else:
            ret = self.get_device_time_string()
            for header in self.global_label_list:
                ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{self.get_global_value_for_header(header)}'

            for command in self.command_list:
                ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{self.obd_connection.query(command).value}'

            if GPS_POSITION_MONITORING:
                for gps_label in self.gps_data_label_list:
                    ret += f'{MONITORING_FILE_SEPARATION_CHARACTER}{self.gnss_manager.get_data_for_header_name(gps_label)}'
            return ret + '\n'

    def get_command_list(self):
        return [self.command_to_string_header_dict[command] for command in self.command_list]

    def get_global_value_for_header(self, header):
        switch = {
            'CAR ID': self.car_id,
            'TRIP ID': self.trip_id
        }
        return switch.get(header, None)

    @staticmethod
    def get_device_time_string():
        return datetime.datetime.now().strftime(DATETIME_FORMAT)


class MainManager(Thread):
    running = False
    obd_connection = None
    gnss_manager = None
    data_manager = None
    socket_server = None

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
        obd.commands.EVAP_VAPOR_PRESSURE_ALT: "ALT EVAPORATIVE VAPOR PRESSURE (pascal)",
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

    global_label_list = [
        'CAR ID',
        'TRIP ID'
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

    def __init__(self):
        super().__init__()

    def run(self):
        self.running = True
        self.start_gnss_manager()
        self.start_obd_connection()
        if self.obd_connection is not None:
            self.restart_data_manager()
            if ENABLE_SOCKET_SERVER and SOCKET_SERVER_PORT is not None:
                self.socket_server = SocketServer(self)
        else:
            logging.error('UNABLE TO HAVE OBD CONNECTION: NOT STARTING DATA MANAGER')

    def start_gnss_manager(self):
        self.gnss_manager = GNSSManager()
        self.gnss_manager.start()

    def stop_data_manager(self, sync=False):
        self.data_manager.terminate(sync)
        self.data_manager.join()
        self.data_manager = None

    def terminate(self):
        self.stop_data_manager()
        self.gnss_manager.terminate()
        self.gnss_manager.join()
        self.stop_obd_connection()

    def restart_data_manager(self, sync=False):
        if self.data_manager is not None:
            self.stop_data_manager(sync)
        self.data_manager = DataManager(self.gnss_manager, CAR_IDENTIFIER, self.obd_connection, self.command_list,
                                        self.string_to_command_dict, self.string_to_status_dict, self.global_label_list,
                                        self.command_to_string_header_dict, self.gps_data_label_list)
        self.data_manager.start()

    def stop_obd_connection(self):
        self.obd_connection.stop()
        self.obd_connection.unwatch_all()
        logging.info('CLOSE USED OBD CONNECTION')
        self.obd_connection.close()

    def query_command(self, string_command):
        command = self.string_to_command_dict.get(string_command)
        if command is None or not self.command_list.__contains__(command):
            return None
        return self.obd_connection.query(command)

    def query_status(self, string_status):
        status = self.string_to_status_dict.get(string_status)
        if status is None:
            return None
        return self.obd_connection.query(status)

    def query_dtc(self):
        return self.obd_connection.query(obd.commands.GET_DTC)

    def clear_dtc(self):
        return self.obd_connection.query(obd.commands.CLEAR_DTC)

    def start_obd_connection(self):
        logging.info('START NEW OBD CONNECTION')
        self.obd_connection = obd.Async(OBD_INTERFACE)
        time.sleep(2)
        wait_count = 1

        while self.obd_connection.status() == utils.OBDStatus.NOT_CONNECTED:
            logging.warning('OBD NOT CONNECTED')
            if wait_count > 500:
                self.obd_connection.close()
                self.obd_connection = None
                logging.error('FAILED TO CREATE OBD CONNECTION')
                return
            logging.warning('RETRY CONNECTION N°' + str(wait_count))
            wait_count = wait_count + 1
            time.sleep(1)

            if not self.obd_connection.is_connected():
                logging.info('CLOSE PREVIOUS OBD CONNECTION')
                self.obd_connection.close()
                time.sleep(7)
                logging.warning('RETRY STARTING NEW OBD CONNECTION')
                self.obd_connection = obd.Async(OBD_INTERFACE)
                time.sleep(2)

        logging.info('TESTING OBD CONNECTION')
        logging.info(str(self.obd_connection.supported_commands))

        for status in self.status_list:
            logging.info('TESTING OBD STATUS: ' + str(status))
            status_supported = self.obd_connection.supports(status)
            logging.info(str(status_supported))

            if not status_supported:
                logging.warning('UNSUPPORTED OBD COMMAND: ' + str(status))
                logging.warning(str(self.obd_connection.query(status, force=True).value))
                self.status_list.remove(status)

        for command in self.command_list:
            logging.info('TESTING OBD COMMAND: ' + str(command))
            command_supported = self.obd_connection.supports(command)
            logging.info(str(command_supported))
            if not command_supported:
                logging.warning('UNSUPPORTED OBD COMMAND: ' + str(command))
                logging.warning(str(self.obd_connection.query(command, force=True).value))
                self.command_list.remove(command)

        if self.command_list.__len__() < 1:
            logging.error('NO OBD COMMAND SUPPORTED')
            logging.info('CLOSE PREVIOUS OBD CONNECTION')
            self.obd_connection.close()
        else:
            for command in self.command_list:
                self.obd_connection.watch(command)  # , callback=self.value_callback)

            for status in self.status_list:
                self.obd_connection.watch(status)

            self.obd_connection.start()
            logging.info(f'CONNECTED TO ECU')
            logging.info(f'NUMBER OF COMMAND MONITORED:{self.command_list.__len__()}')
            logging.info('START MONITORING ECU DATA')


if __name__ == '__main__':
    main_manager = MainManager()
    main_manager.start()
