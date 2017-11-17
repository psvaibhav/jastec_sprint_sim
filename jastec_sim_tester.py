import jastec_device_sim_mqtt
import random
import time

print(150 * '-')
mqttClient = jastec_device_sim_mqtt.MyMQTT(
    'jastec_sim_v01_' + str(random.randint(0, 10000)) + '_' + str(int(time.time())))
rc = mqttClient.run()

print("rc: " + str(rc))
