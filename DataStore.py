#!/bin/usr/python3

#Read Data from MQTT messages and store in DB tale every 60 sec

import paho.mqtt.client as mqtt #import the client1
import time
import traceback
import psycopg2

conn = psycopg2.connect('dbname=Farm2021')
cur = conn.cursor()

broker_address="192.168.50.201" 

FlowSensor = mqtt.Client("FlowSensor") #create new instance
FlowSensor.connect(broker_address) #connect to broker
Radon = mqtt.Client("Radon")
Radon.connect(broker_address)
Server = mqtt.Client("Server")
Server.connect(broker_address)
WeatherStation = mqtt.Client("WeatherStation")
WeatherStation.connect(broker_address)

data = ""
array = []
testMessage = 0
pumpWh = 0
maxFlowRate = 0
waterVolume = 0
powerMax = 0
houseWh = 0
HVACWh = 0
Lwater = 0
Toutside =0
Houtside=0
Tinside=0
Hinside=0
radon=0
windSpeed=0 #PH1
windDirection=0 #PH2
cloudCover=0 #PH3
dewPoint=0 #PH4
precipIntensity = 0 #PH5=0
PH6=0

def getCurrentTime():
    #timeNow = time.localtime()
    year = time.localtime().tm_year
    month = time.localtime().tm_mon
    day = time.localtime().tm_mday
    hour = time.localtime().tm_hour
    minute = time.localtime().tm_min
    #second = time.localtime().tm_sec
    return year,month,day,hour,minute

def on_msgFlowSensor(client, userdata, message):
    global waterVolume, maxFlowRate, pumpWh, PH1
    data = str(message.payload.decode("utf-8"))
    array = data.split(",")
    waterVolume = int(float(array[1])*10) #Water Volume for 60 sec in [L/100]
    maxFlowRate = int(float(array[2])*10) #Flow rate in [L/10min]
    pumpWh=int(float(array[3])*10) #pump power for 60 sec [Wh/10]
    PH1=int(array[4]) #1 for Arduino connected
    print("WV ",waterVolume,",","Flow ",maxFlowRate,",","Wp ",pumpWh,",","ArduinoConnected",PH1)
    
def on_msgServer(client, userdata, message):
    global houseWh, powerMax
    data=str(message.payload.decode("utf-8"))
    array = data.split(",")
    if len(array)== 2:
        houseWh = int(float(array[0])*10) #[wh/10]
        powerMax = int(array[1]) #[W]
        print("WhH ",houseWh,", ","WH ",powerMax)
    else:
        print(len(array), " is not required length of 2.  Message skipped")
    
def on_msgRadon(client,userdata,message):
    global Tinside,Hinside,radon
    data=str(message.payload.decode("utf-8"))
    array = data.split(",")
    Tinside = int(float(array[1])*10)
    Hinside = int(float(array[2]))
    radon = int(float(array[3])*10)
    print("Ti ",Tinside,"Hi ",Hinside,"R ",radon)

def on_msgWeatherData(client,userdata,message):
    global Toutside,Houtside,windSpeed,windDirection,cloudCover,dewPoint,precipIntensity
    data=str(message.payload.decode("utf-8"))
    array=data.split(",")
    Toutside=int(float(array[1])*10)
    Houtside=int(float(array[2]))
    windSpeed=int(float(array[3])*10)
    windDirection=int(float(array[4]))
    cloudCover=int(float(array[5]))
    dewPoint=int(float(array[6])*10)
    precipIntensity=int(float(array[7])*100)

def saveData(yr,mn,dy,hr,mi):
    global houseWh,powerMax,waterVolume,maxFlowRate,pumpWh,Lwater,HVACWh,Toutside,Houtside
    global Tinside,Hinside,radon,windSpeed,windDirection,cloudCover,dewPoint
    global precipIntensity,PH6
    query="""INSERT INTO data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    values = [yr,mn,dy,hr,mi,Lwater,maxFlowRate,houseWh,pumpWh,waterVolume,HVACWh,Toutside,Houtside,Tinside,Hinside,radon,powerMax,windSpeed,windDirection,cloudCover,dewPoint,precipIntensity,PH6]
    print(values)
    cur.execute(query, values)
    #Reset values to ensure only fresh readings are added to DB next round
    waterVolume=0
    maxFlowRate = 0
    pumpWh = 0
    radon = 0
    Tinside = 0
    Hinside = 0
    conn.commit()
    print(yr,",",mn,",",dy,",",hr,",",(mi),",","Data Saved")
        
FlowSensor.on_message=on_msgFlowSensor #Attach message to callback
Server.on_message=on_msgServer
Radon.on_message=on_msgRadon
WeatherStation.on_message=on_msgWeatherData

yr,mn,dy,hr,mi = getCurrentTime()
miPast = mi

try:
    FlowSensor.loop_start()
    Server.loop_start()
    Radon.loop_start()
    WeatherStation.loop_start()
    
    while True:
        yr,mn,dy,hr,mi = getCurrentTime()
        
        if mi != miPast:
            saveData(yr,mn,dy,hr,mi)
            miPast = mi  
        
        FlowSensor.subscribe("FlowSensorPi/WaterVolume")
        Server.subscribe("ServerPi/Energy")
        Radon.subscribe("FlowSensorPi/Radon")
        WeatherStation.subscribe("WeatherStation/WeatherData")
        
        time.sleep(1)
except:
    FlowSensor.loop_stop()
    Server.loop_stop()
    Radon.loop_stop()
    WeatherStation.loop_stop()
    traceback.print_exc()