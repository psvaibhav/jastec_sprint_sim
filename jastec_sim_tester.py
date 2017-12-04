import jastec_device_sim_mqtt
import random
import time
import sys

print(150 * '-')

if len(sys.argv) < 2:
    print('Zero arguments')
else:
    device_path = sys.argv[1:][0]
    print('Device path is [' + device_path + ']')
    mqtt_client = jastec_device_sim_mqtt.MyMQTT(
        'jastec_sim_v01_' + str(random.randint(0, 10000)) + '_' + str(int(time.time())), device_path)
    rc=mqtt_client.run()
    print("rc: " + str(rc))
