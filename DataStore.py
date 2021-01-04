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
PH1=0
PH2=0
PH3=0
PH4=0
PH5=0
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
    global waterVolume, maxFlowRate, pumpWh
    data = str(message.payload.decode("utf-8"))
    array = data.split(",")
    waterVolume = int(float(array[1])*100) #Water Volume for 60 sec in [L/100]
    maxFlowRate = int(float(array[2])*10) #Flow rate in [L/10min]
    pumpWh=int(float(array[3])*10) #pump power for 60 sec [Wh/10]
    
    print(waterVolume,",",maxFlowRate,",",pumpWh)
    
def on_msgServer(client, userdata, message):
    global houseWh, powerMax
    data=str(message.payload.decode("utf-8"))
    array = data.split(",")
    houseWh = int(float(array[1])*10) #[wh/10]
    powerMax = int(array[2]) #[W]
    
    print(round(houseWh,1),", ",powerMax)

def on_msgRadon(client,userdata,message):
    global Tinside,Hinside,radon
    data=str(message.payload.decode("utf-8"))
    array = data.split(",")
    Tinside = int(float(array[1])*10)
    Hinside = int(float(array[2])*10)
    radon = int(float(array[3])*10)
    
    print(Tinside,Hinside,radon)


def saveData(yr,mn,dy,hr,mi):
    global houseWh,powerMax,waterVolume,maxFlowRate,pumpWh,Lwater,HVACWh,Toutside,Houtside
    global Tinside,Hinside,radon,PH1,PH2,PH3,PH4,PH5,PH6
    query="""INSERT INTO data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    values = [yr,mn,dy,hr,mi,Lwater,maxFlowRate,houseWh,pumpWh,waterVolume,HVACWh,Toutside,Houtside,
              Tinside,Hinside,radon,powerMax,PH1,PH2,PH3,PH4,PH5,PH6]
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

yr,mn,dy,hr,mi = getCurrentTime()
miPast = mi

try:
    FlowSensor.loop_start()
    Server.loop_start()
    Radon.loop_start()
    
    while True:
        yr,mn,dy,hr,mi = getCurrentTime()
        
        if mi != miPast:
            saveData(yr,mn,dy,hr,mi)
            miPast = mi  
        
        FlowSensor.subscribe("FlowSensorPi/WaterVolume")
        Server.subscribe("ServerPi/Energy")
        Radon.subscribe("FlowSensorPi/Radon")
        
        time.sleep(1)
except:
    FlowSensor.loop_stop()
    Server.loop_stop()
    Radon.loop_stop()
    traceback.print_exc()