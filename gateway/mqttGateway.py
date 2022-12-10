from loraE32 import ebyteE32
import time
import paho.mqtt.client as mqtt 
from loguru import logger
import weatherstation

M0pin = 25
M1pin = 26
AUXpin = 27

e32 = ebyteE32(M0pin, M1pin, AUXpin, Address=0x0000, Channel=0x06, Platform='raspberry_pi', DTU=True, debug=False)

e32.start()

from_address = 0x0000
from_channel = 0x06

to_address=0x0000
to_channel = 0x06

did=100


# connect to mqtt broker
#broker_address = "192.168.3.63"
broker_address= "broker.hivemq.com"
port=1883
client = mqtt.Client("v_beacon")      # create new instance
client.connect(broker_address, port=port)
#client.loop_start()


def ack(to_did):
    global to_address
    message={ 'did':did, 'to':to_did ,'ack': 1 }
    e32.sendMessage(to_address, to_channel, message, useChecksum=True)
    logger.info(f"ACK to {to_did}")



while True:

    message = e32.recvMessage(from_address, from_channel, useChecksum=True)
    #if message:
    #    logger.info(message)
        
    if 'mqtt' in message:
        from_did=message['did']
        logger.info(f"Received MQTT request from {from_did}")
        #time.sleep(2)
        ack(from_did)
        client.publish("loraE32/{}".format(from_did), message['mqtt'],qos=2)
        client.on_publish = logger.info("Published message to MQTT broker")
    
    if 'wth' in message:
        weatherstation.get_weather_data(message['wth'])
    
    client.loop(.1)
    weatherstation.local_client.loop(.1)
     
    time.sleep(1)

e32.stop()