import paho.mqtt.client as mqtt
import ssl
import json
import logging
import sys
import configparser
import csv
import threading
import time
import random

logFormatter = logging.Formatter(
    "%(asctime)s [%(threadName)s] [%(module)s:%(funcName)s():%(lineno)d] [%(levelname)s] - %(message)s")

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(
    "{0}/{1}.log".format('data', 'testlog'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

class MyMQTT(mqtt.Client):

    BROKER_URL = 'vsb.nrt.unabdev.sprint.com'
    IMA_PORT = 8333
    CA_CERT_PATH = 'certs/vsbnrtunabdevsprintcom.crt'
    USER_NAME = 'IMA_OAUTH_ACCESS_TOKEN'
    PASSWORD = 'ca3e42a6-e782-400f-8622-1b60a3eda59f'
    PUBLISH_PERIOD = 10  # in seconds
    PUBLISH_WAIT_PERIOD = 2  # in minutes

    def on_connect(self, mqttc, obj, flags, rc):
        logger.debug('Connection status: [' + str(rc) + ']')
        self.msg_publisher = MessagePublisher(self)
        self.msg_publisher.publish_tdr()

    def on_message(self, mqttc, obj, msg):
        logger.debug('Received msg [' +
                     str(msg.payload) + '] on topic[' + msg.topic)
        MessageHandler(msg.topic, msg.payload.decode(
            'utf-8'), self).route_message()

    def on_publish(self, mqttc, obj, mid):
        logger.debug('Publish success - mid: [' + str(mid) + ']')

    def on_subscribe(self, mqttc, obj, mid, granted_qos):
        logger.debug('Subscribed')

    def on_log(self, mqttc, obj, level, string):
        # logger.debug(string)
        pass

    def run(self):
        # self.tls_set(ca_certs=self.CA_CERT_PATH)
        self.vehicle_dict = VehicleData().get_config_data()
        self.serial = self.vehicle_dict['device_info']['serial']
        self.SUBSCRIBE_TOPIC = '/vc/v1/devices/' + self.serial + '/rpc/request'
        self.RESPONSE_TOPIC = '/vc/v1/devices/' + self.serial + '/rpc/response'
        self.PUBLISH_TOPIC = '/vc/v1/devices/' + self.serial + '/telemetry'
        self.CONTROL_TOPIC = '/vc/v1/devices/' + self.serial + '/control'
        self.CONTROL_RESP_TOPIC = '/vc/v1/devices/' + self.serial + '/control/response'
        self.tls_set(ca_certs=self.CA_CERT_PATH, cert_reqs=ssl.CERT_NONE)
        self.username_pw_set(self.USER_NAME, self.PASSWORD)
        self.connect(self.BROKER_URL, self.IMA_PORT, 60)
        self.subscribe(self.SUBSCRIBE_TOPIC, 2)
        self.subscribe(self.CONTROL_TOPIC, 2)
        rc = 0
        while rc == 0:
            rc = self.loop()
        return rc


class MessageHandler:
    def __init__(self, topic, message, mqttc):
        self.topic = topic
        self.message = message
        self.mqttc = mqttc
        self.vehicle_data = self.mqttc.vehicle_dict

    def route_message(self):
        logger.debug('Received message on topic [' + self.topic + ']')
        if self.mqttc.SUBSCRIBE_TOPIC in self.topic:
            self.handle_server_msg()
        elif self.mqttc.CONTROL_TOPIC in self.topic:
            self.handle_control_message()

    def handle_control_message(self):
        try:
            json_msg = json.loads(self.message)
        except json.JSONDecodeError:
            logger.error('Received payload is not a valid json')
            # publish_dict['additionalInfo'] = 'Received payload is not a valid json'
        else:
            cmd = json_msg.get('cmd', None)
            if cmd is None:
                logger.debug('Received json is missing [cmd] key')
            else:
                if cmd == 'ignOff':
                    logger.debug('Received [ignOff] cmd')
                    self.mqttc.msg_publisher.publish_trip_info()  # todo: remove later
                if cmd == 'onlineStatus':
                    logger.debug('Received [onlineStatus] cmd')
                    self.mqttc.msg_publisher.publish_online_status()
                else:
                    logger.debug('Command [' + cmd + '] is not supported')

    def handle_server_msg(self):
        publish_dict = {'results': '0'}
        try:
            json_msg = json.loads(self.message)
        except json.JSONDecodeError:
            logger.error('Received payload is not a valid json')
            publish_dict['additionalInfo'] = 'Received payload is not a valid json'
        else:
            method = json_msg.get('method', None)
            if method is None:
                logger.debug('Received json is missing [method] key')
                publish_dict['additionalInfo'] = 'Received json is missing [method] key'
            else:
                try:
                    add_info_dict = {}
                    if method == 'getGPSKey':
                        # publish_json = '{"additionalInfo": {"gpskey": "AXSDM4354545DFDFDFDFDFDFDF"}, "results": "0"}'
                        add_info_dict['gpskey'] = self.vehicle_data['device_info']['gps_key']
                    elif method == 'getDeviceInfo':
                        # publish_json = '{"additionalInfo": {"device": {"network_id": 0, "complied_version": 0, "cardb_name": 0, "fw_version": 0, "obd2_supported_result": 0}}, "results": "0"}'
                        device_dict = {}
                        device_dict['network_id'] = self.vehicle_data['device_info']['network_id']
                        device_dict['complied_version'] = self.vehicle_data['fw_info']['complied_version']
                        device_dict['cardb_name'] = self.vehicle_data['fw_info']['cardb_name']
                        device_dict['fw_version'] = self.vehicle_data['fw_info']['fw_version']
                        device_dict['obd2_supported_result'] = self.vehicle_data['fw_info']['obd2_supported_result']
                        add_info_dict['device'] = device_dict
                    elif method == 'serial':
                        # publish_json = '{"additionalInfo": {"serial": "A00001"}, "results": "0"}'
                        add_info_dict['serial'] = self.vehicle_data['device_info']['serial']
                    elif method == 'getVinfo':
                        # publish_json = '{"additionalInfo": {"vinfo": {"cylNo": 0, "vol": 0, "vin": "", "hybrid": 0, "fuel": 0, "modelcode": 0}}, "results": "0"}'
                        vehicle_info_dict = {}
                        vehicle_info_dict['cylNo'] = self.vehicle_data['vehicle_info']['cylNo']
                        vehicle_info_dict['vol'] = self.vehicle_data['vehicle_info']['vol']
                        vehicle_info_dict['vin'] = self.vehicle_data['vehicle_info']['vin']
                        vehicle_info_dict['hybrid'] = self.vehicle_data['vehicle_info']['hybrid']
                        vehicle_info_dict['fuel'] = self.vehicle_data['vehicle_info']['fuel']
                        vehicle_info_dict['modelcode'] = self.vehicle_data['vehicle_info']['model_code']
                        add_info_dict['vinfo'] = vehicle_info_dict
                    else:
                        # publish_json = '{"additionalInfo": "", "results": "0"}'
                        logger.debug(
                            'Key [' + method + "] not expected in the json payload")
                except KeyError:
                    logger.debug('Key not found in dict')
                except:
                    logger.debug('Exception while retrieving information')
                finally:
                    publish_dict['additionalInfo'] = add_info_dict
                    print(publish_dict)
        finally:
            self.mqttc.msg_publisher.publish_data(self.mqttc.RESPONSE_TOPIC,
                                                  json.dumps(publish_dict, ensure_ascii=False))


class VehicleData:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('data/jastec_config.ini')

    def get_config_data(self):
        return self.config


class MessagePublisher:
    def __init__(self, mqttc):
        self.mqttc = mqttc
        self.online = True
        self.row_no = 0
        self.distance_seed = random.randint(2000, 10000)
        self.fuel_level_seed = random.randint(90, 100)
        self.speed_seed = random.randint(5, 10)
        self.SPEED_LIMIT = 60 #mph
        self.increase_speed = True
        with open('data/gps_track_data.csv', newline='') as gps_track_file:
            gps_track_reader = csv.reader(gps_track_file)
            self.gps_track_data = list(gps_track_reader)
            self.row_count = len(self.gps_track_data)

    def start_publishing(self):
        self.online = True
        logger.debug('Starting publishing')

    def stop_publishing(self):
        self.online = False
        logger.debug('Stopping publishing')

    def publish_data(self, topic, message):
        if self.online or topic == self.mqttc.CONTROL_RESP_TOPIC:
            self.mqttc.publish(topic, message)
        else:
            logger.debug('Offline, do not publish')

    def publish_telemetry(self, message):
        self.publish_data(self.mqttc.PUBLISH_TOPIC,
                          json.dumps(message, ensure_ascii=False))

    def publish_online_status(self):
        if self.online is True:
            status = '{status: online}'
        else:
            status = '{status: offline}'
        self.publish_data(self.mqttc.CONTROL_RESP_TOPIC, status)

    def publish_tdr(self):
        if self.online is True:
            # publish fq updates here
            # {"type": 1, "payload": {"distance": 1043, "bVolt": 1, "yaw_rot": 0, "course": 10, "engTemp": 99, "engload": 10, "lon": 12688242, "fuelS": 100, "ts": 1501746441,
            #                         "gVal": 1, "acc_xy": 0, "fc": 61, "tps": 10, "lat": 3755148, "encoded": 0, "tid": 37, "rpm": 883, "speed": 10, "s_mark": 0}, "ts": 1501746441}

            # reset the row count
            if self.row_no == self.row_count:
                self.row_no = 0
                self.publish_trip_info()
                self.stop_publishing()
                time.sleep(self.mqttc.PUBLISH_WAIT_PERIOD * 60)  # 10 minutes
                self.start_publishing()

            threading.Timer(self.mqttc.PUBLISH_PERIOD,
                            self.publish_tdr).start()

            # logger.debug('row_no[' + str(self.row_no) + '] lat[' +
            #              str(self.gps_track_data[self.row_no][0]) + '] lon[' + str(self.gps_track_data[self.row_no][1]) + ']')

            tdr_dict = {'type': 1}
            payload_dict = {}
            # distance
            payload_dict['distance'] = self.distance_seed
            self.distance_seed += random.randint(5, 20)
            # bVolt
            payload_dict['bVolt'] = round(
                random.uniform(14.0, 15.0), 2)
            # yaw_rot
            payload_dict['yaw_rot'] = random.randint(0, 360)
            # course
            payload_dict['course'] = random.randint(0, 360)
            # engTemp
            payload_dict['engTemp'] = random.randint(50, 120)
            # engLoad
            payload_dict['engload'] = random.randint(5, 95)

            # fuelS
            payload_dict['fuelS'] = self.fuel_level_seed
            if self.fuel_level_seed <= 5:
                # driver refuelled 
                self.fuel_level_seed = random.randint(98, 100)
            else:    
                self.fuel_level_seed -= random.randint(1, 5)
                # fuel level cannot be negative
                if self.fuel_level_seed < 0:
                    self.fuel_level_seed = 0
            # gVal
            payload_dict['gVal'] = round(
                random.uniform(-4.0, 4.0), 2)
            # acc_xy
            payload_dict['acc_xy'] = round(
                random.uniform(-4.0, 4.0), 2)
            # fc
            payload_dict['fc'] = random.randint(0, 100)
            # tps
            payload_dict['tps'] = random.randint(0, 100)
            # lat
            # payload_dict['lat'] = round(
            #     random.uniform(38.91, 38.92), 7)
            payload_dict['lat'] = self.gps_track_data[self.row_no][0]
            # lon
            # payload_dict['lon'] = round(
            #     random.uniform(-94.65, -94.66), 7)
            payload_dict['lon'] = self.gps_track_data[self.row_no][1]
            self.row_no += 1
            # encoded
            payload_dict['encoded'] = random.randint(0, 100)
            # tid
            payload_dict['tid'] = random.randint(0, 100)
            # rpm
            payload_dict['rpm'] = random.randint(500, 5000)
            # speed
            payload_dict['speed'] = self.speed_seed
            if self.speed_seed >= self.SPEED_LIMIT + 10:
                self.increase_speed = False
            elif self.speed_seed <= 5:
                self.increase_speed = True
            
            if self.increase_speed is True:
                self.speed_seed += random.randint(5, 15)
            else:
                self.speed_seed -= random.randint(5, 15)
                # speed cannot be negative
                if self.speed_seed < 0:
                    self.speed_seed = 0

            # s_mark
            payload_dict['s_mark'] = random.randint(0, 100)

            # mpg
            payload_dict['mileage'] = round(random.uniform(21.0, 23.0), 2)

            # ts
            ts = int(time.time())
            payload_dict['ts'] = ts

            tdr_dict['payload'] = payload_dict
            tdr_dict['ts'] = ts

            logger.debug('Publishing tdr data')
            self.publish_telemetry(tdr_dict)

    def publish_trip_info(self):
        # {"type": 2, "payload": {"sudden_mark_decel": 0, "co2PerKm": 0, "t_lon": 12688915, "dtcType": "e", "max_accel": 0.939372713, "ecoTime": 0, "t_lat": 3754732, "iTime": 0, "max_rot": 0.479425539,
        #  "engTempMax": 0, "etime": 1501115390, "max_decel": 0, "accelTime": 0, "tpsMax": 10, "co2Mass": 0, "tid": 37, "dtcCode": "P0101", "rTime": 0, "oSpeedTime": 0, "warmTime": 0, "h_lon": 12688242,
        #  "avgSpeed": 146.0, "h_lat": 3755148, "fcEffi": 0, "fcMass": 27.083333333333332, "stime": 1501115378, "sudden_mark_rot": 0, "distance": 0, "maxSpeed": 63, "sudden_mark_accel": 0,
        # "fCutTime": 0}, "ts": 1501746453}

        trip_info_dict = {'ts': int(time.time()), 'type': 2}
        payload_dict = {}
        payload_dict['sudden_mark_decel'] = 0
        payload_dict['co2PerKm'] = 0
        payload_dict['t_lon'] = self.gps_track_data[self.row_count - 1][1]
        payload_dict['dtcType'] = 'e'
        payload_dict['max_accel'] = 0.939372713
        payload_dict['ecoTime'] = 0
        payload_dict['t_lat'] = self.gps_track_data[self.row_count - 1][0]
        payload_dict['iTime'] = 0
        payload_dict['max_rot'] = 0.479425539
        payload_dict['engTempMax'] = random.randint(50, 100)
        payload_dict['etime'] = 1501115390
        payload_dict['max_decel'] = 0
        payload_dict['accelTime'] = 0
        payload_dict['tpsMax'] = 10
        payload_dict['co2Mass'] = 0
        payload_dict['tid'] = 37
        payload_dict['dtcCode'] = 'P0101'
        payload_dict['rTime'] = 0
        payload_dict['oSpeedTime'] = 0
        payload_dict['warmTime'] = 0
        payload_dict['h_lon'] = self.gps_track_data[0][1]
        payload_dict['avgSpeed'] = random.randint(5, 120)
        payload_dict['h_lat'] = self.gps_track_data[0][0]
        payload_dict['fcEffi'] = 0
        payload_dict['fcMass'] = 27.083333333333332
        payload_dict['stime'] = 1501115378
        payload_dict['sudden_mark_rot'] = 0
        payload_dict['distance'] = self.distance_seed
        payload_dict['maxSpeed'] = random.randint(90, 120)
        payload_dict['sudden_mark_accel'] = 0

        trip_info_dict['payload'] = payload_dict
        logger.debug('Publishing trip info data')
        self.publish_telemetry(trip_info_dict)
