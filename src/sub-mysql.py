#!/usr/local/bin/python3

import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import time

import login_details

def on_connect(client, userdata, flags, rc):
    print("MQTT Client Connected")
    client.subscribe("sensor/#")

def on_message(client, userdata, msg):
    #print("Transmission received")

    value = float(str(msg.payload.decode("utf-8")).strip())

    root_sensors,sensor_hex,item = str(msg.topic).split('/')

    sensor_id = int("0x"+sensor_hex,16)

    now = time.strftime('%Y-%m-%d %H:%M:%S')

    # print(now)
    # print(sensor_hex)
    # print(sensor_id)
    # print(item)
    # print(item[0])
    # print(value)

    try:
        db = pymysql.connect(host=login_details.mysqlHost, user=login_details.mysqlUser, password=login_details.mysqlPassword, db=login_details.dbName, 
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        #print("MySQL Client Connected")
    except pymysql.Error as e:
        print(e)
        sys.exit()
    
    #finally:
    #    print('Connection opened successfully.')   
    
    #
    # sensor_update(db,payload)
    #

    with db.cursor() as cursor:
        try:
            sql = "INSERT INTO `indoor_temperature` (`sensor_id`, `date_time`, `value`, `vtype`) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (sensor_id, now, value, item[0]))
        except pymysql.Error as e:
            print(e)
            sys.exit()
        finally:
            db.commit()


    db.close()


# Connect the MQTT Client
client = mqtt.Client("sensors2mysql-1")
client.on_connect = on_connect
client.on_message = on_message
# client.username_pw_set(username=mqttUser, password=mqttPassword)
try:
    client.connect(login_details.mqttBroker, login_details.mqttBrokerPort, 60)
except:
    sys.exit("Connection to MQTT Broker failed")
# Stay connected to the MQTT Broker indefinitely
client.loop_forever()

# client.loop_start() #start the loop
# time.sleep(10) # wait
# client.loop_stop()
