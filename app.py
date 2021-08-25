import os.path
from flask import Flask, jsonify
from threading import Thread
import datetime
import obd
from obd import utils
from gps import *
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
DATETIME_FORMAT = "%Y%m%d_%H%M%S-%f"
DEVICE_TIME_LABEL = 'DEVICE_TIME'
RECORD_DIRECTORY = 'recordings'
RECORD_DIRECTORY_LOCATION = os.path.join('.', RECORD_DIRECTORY)
OBD_INTERFACE = config.get('DEFAULT', 'OBD_INTERFACE', fallback=None)
SERVER_LOCATION = config.get('DEFAULT', 'SERVER_LOCATION', fallback=None)
FILE_RECORDING = config['DEFAULT'].getboolean('FILE_RECORDING', fallback=False)


class DataManager(Thread):
    obd_connection = None
    gpsd = None
    deviceTime = None
    running = False

    command_value_dict = {}

    string_to_command_dict = {
        'ENGINE_LOAD': obd.commands.ENGINE_LOAD,
        'COOLANT_TEMP': obd.commands.COOLANT_TEMP,
        'SHORT_FUEL_TRIM_1': obd.commands.SHORT_FUEL_TRIM_1,
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

    command_list = [obd.commands.ENGINE_LOAD,
                    obd.commands.COOLANT_TEMP,
                    obd.commands.SHORT_FUEL_TRIM_1,
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
        self.gpsd = gps(mode=WATCH_ENABLE)

    def value_callback(self, response):
        self.command_value_dict[response.command] = response.value

    def run(self):
        self.running = False
        print('START NEW OBD CONNECTION')
        self.obd_connection = obd.Async(OBD_INTERFACE)
        time.sleep(2)
        wait_count = 1

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
                if RECORD_DIRECTORY not in os.listdir('.'):
                    os.mkdir(RECORD_DIRECTORY_LOCATION)
                filename = os.path.join(RECORD_DIRECTORY_LOCATION, self.get_device_time_string() + '.csv')

                with open(filename, 'a') as file:
                    file.write(self.get_command_record(header=True))
                    while self.running:
                        file.write(self.get_command_record(header=False))
                        time.sleep(0.5)

            else:
                while self.running:
                    time.sleep(0.5)

            self.obd_connection.stop()
            self.obd_connection.unwatch_all()
            print('CLOSE USED OBD CONNECTION')
            self.obd_connection.close()

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

    def query_position(self):
        if not self.running:
            return None
        return self.gpsd.fix

    def clear_dtc(self):
        if not self.running:
            return None
        return self.obd_connection.query(obd.commands.CLEAR_DTC)

    def get_command_record(self, header=False):
        if not self.running:
            return None
        ret = ''
        if header:
            ret = f'{DEVICE_TIME_LABEL},'
            for command in self.command_list:
                ret += f',{command}'
            return ret + '\n'
        else:
            ret = f'{self.get_device_time_string(),}'
            for command in self.command_list:
                ret += f',{self.obd_connection.query(command).value}'
            return ret + '\n'

    @staticmethod
    def get_device_time_string():
        return str(datetime.datetime.now().strftime(DATETIME_FORMAT))


app = Flask(__name__)
data_manager = DataManager()


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


@app.route('/command/<command>')
def get_command(command):
    response = data_manager.query_command(command)
    return jsonify(
        value=response.value.magnitude,
        unit=response.unit
    )


@app.route('/status/<status>')
def get_status(status):
    return data_manager.query_status(status)


@app.route('/dtc')
def get_dtc():
    return data_manager.query_dtc()


@app.route('/position')
def get_position():
    gps_data = data_manager.query_position()
    return jsonify(
        latitude=gps_data.latitude,
        longitude=gps_data.longitude,
        altitude=gps_data.altitude,
        speed=gps_data.speed,
        track=gps_data.track,
    )


if __name__ == '__main__':
    data_manager.start()
    app.run()
