#!/usr/local/bin/python3

##  rtl_433 -F "mqtt://mqtt_bus:1883,retain=60,events=rtl_433/[model],devices=rtl_433/[model]/[id]"


import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import time

import requests
import json
import math
import re

import login_details

DEBUG=1

sensor_model="Fineoffset-WHx080"

software_name = "Custom_RTL433"

# approximation valid for
# 0 degC < T < 60 degC
# 1% < RH < 100%
# 0 degC < Td < 50 degC 
# constants
a = 17.271
b = 237.7 # degC
def dewpoint_approximation(T,RH):
    Td = (b * gamma(T,RH)) / (a - gamma(T,RH))
    return Td
def gamma(T,RH):
    g = (a * T / (b + T)) + math.log(RH/100.0)
    return g


def hpa_to_inches(pressure_in_hpa):
    pressure_in_inches_of_m = pressure_in_hpa * 0.02953
    return pressure_in_inches_of_m


def on_connect(client, userdata, flags, rc):
    print("MQTT Client Connected")

    # have to use events topic to get the raw json
    client.subscribe("rtl_433/events/"+sensor_model)

def on_message(client, userdata, msg):
    #print("Transmission received")

    value = str(msg.payload.decode("utf-8")).strip() # payload is the value, in this case the json

    # TODO: pull air quality stats from mySQL

    # TODO: save all data back to old weewx DB

    if DEBUG==1:
        print(value)

    j = json.loads(value)

    T=float(j['temperature_C'])
    RH=float(j['humidity'])
    Td = dewpoint_approximation(T,RH)

    # build URL
    # https://kapuablog.wordpress.com/2021/02/07/pws-weather-underground-upload/

    url = "https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"
    url += "?action=updateraw"
    url += "&ID=" + login_details.wu_id
    url += "&PASSWORD="  + login_details.wu_password
    url += "&softwaretype=" + software_name
    url += "&dateutc=" + j['time']
    url += "&dailyrainin=" + "{0:.2f}".format(j['rain_mm'] * 0.0393701)
    url += "&dewptf=" + "{0:.2f}".format((Td * 9.0 / 5.0) + 32.0)
    #url += "&rainin=" + j['rain_mm']
    url += "&humidity=" + str(j['humidity'])
    url += "&tempf=" + "{0:.2f}".format((T * 9.0 / 5.0) + 32.0)
    url += "&winddir=" + str(j['wind_dir_deg'])
    url += "&windgustmph=" + "{0:.2f}".format(j['wind_max_km_h'] * 0.62137119)
    url += "&windspeedmph=" + "{0:.2f}".format(j['wind_avg_km_h'] * 0.62137119)

    # PressureHpa * 0.0295299830714,
    # RainToday * 0.0393701,
    # RainLastHour * 0.0393701

    print(re.sub("PASSWORD=.*?\&","PASSWORD=XXXXXXX&",url))

    # send to WU
    r= requests.get(url)
    if DEBUG==1:
        print("Received " + str(r.status_code) + " " + str(r.text))

# Connect the MQTT Client
client = mqtt.Client("weatherunderground2mysql-1")
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
