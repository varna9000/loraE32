import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from notifiers import notify
import paho.mqtt.client as mqtt
from loguru import logger
# make sure you rename the sample file and put your credentials in it
import tokens

org = "home"
url = "http://192.168.3.63:8086"
client = influxdb_client.InfluxDBClient(url=url, token=influx_token, org=org)
bucket = "weatherstation"

write_api = client.write_api(write_options=SYNCHRONOUS)

# connect to mqtt broker
local_broker_address = "192.168.3.63"
local_client = mqtt.Client("P1")      # create new instance
local_client.connect(local_broker_address)  # connect to broker

def get_weather_data(msg):
    
    if msg is not None:
    
        try:
            if len(msg) > 0:

                logger.info(f"Weather data: {msg}")

                # 18.51, 975.2288, 118.9924, 56.7, 1.8, 0, 0.0, 12.316
                # FORMAT: temperature, pressure/100, humidity, degrees_to_cardinal(wind_direction),
                # wind_speed_mean, wind_gust, rainfall, voltage

                temperature = msg.split(',')[0]
                pressure = msg.split(',')[1]
                humidity = msg.split(',')[2]
                winddirection = msg.split(',')[3]
                windspeed = msg.split(',')[4]
                # windgust=message['msg'].split(',')[5]
                rainfall = msg.split(',')[6]
                battery = msg.split(',')[7]

                logger.info("Publishing to MQTT broker")
                local_client.publish("weatherstation/temperature", temperature)
                local_client.publish("weatherstation/pressure", pressure)
                local_client.publish("weatherstation/humidity", humidity)
                local_client.publish("weatherstation/battery", battery)
                local_client.publish("weatherstation/rainfall", rainfall)
                local_client.publish("weatherstation/winddirection", winddirection)
                local_client.publish("weatherstation/windspeed", windspeed)

                points = [
                {"measurement": "temperature", "fields": {"value": float(temperature)}},
                {"measurement": "humidity", "fields": {"value": round(float(humidity))}},
                {"measurement": "pressure", "fields": {"value": round(float(pressure))}},
                {"measurement": "rainfall", "fields": {"value": float(rainfall)}},
                {"measurement": "windspeed", "fields": {"value": float(windspeed)}},
                {"measurement": "winddirection", "fields": {"value": float(winddirection)}},
                {"measurement": "battery", "fields": {"value": float(battery)}}
                ]

            logger.info("Saving to Influxdb")
            write_api.write(bucket=bucket, org=org, record=points)
        
        except IndexError as e:
            logger.error(f"CRC failed! {e}")
            notify('pushover', user=push_user, token=push_token, message=f'{e}')

        except Exception as e:
            logger.error(f"Something unexpected happened: {e}")
            notify('pushover', user=push_user, token=push_token, message=f'{e}')