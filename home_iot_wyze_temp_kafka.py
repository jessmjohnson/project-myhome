import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError


"""
    This function authenticates to the Wyze API.
"""
def wyze_authentication(wyze_email, wyze_password):
    try: 
        # authentication
        wyze_response = Client().login(email=wyze_email, password=wyze_password)   # Create a new client and Login to the client 
        wyze_access_token = wyze_response['access_token'] # Get the access token
        client = Client(token=wyze_access_token)

        return client # Return the Wyze API client

    except WyzeApiError as e:
        # You will get a WyzeApiError if the request failed
        print(f"Got an error: {e}")
        exit() # Exit the program

"""
    This function gets the temperature data from the Wyze API.
"""
def get_wyze_temperatures(wyze_client, kafka_producer, kafka_topic):

    # Get the list of all Wyze devices associated with your account
    try:
        # Get the list of all Wyze devices associated with your account
        my_thermostat = wyze_client.thermostats.list()[0]  #get_user_devices()

        # Get a list of all Room Sensors for Thermostat devices
        room_sensors = wyze_client.thermostats.get_sensors(device_mac=my_thermostat.mac, device_model='CO_EA1')

        # Loop through the temperature sensors and insert readings into the database
        for r in room_sensors:
                try:
                    sensor_name = r.nickname # Get the sensor name
                    device_id = r.did # Get the device ID
                    mac_address = r.mac # Get the MAC address
                    product_model = 'WS01C' if r.product.model is None else r.product.model # Get the product model
                    temperature = r.temperature # Get the temperature
                except WyzeApiError as e:
                    print('Failed to get temperature data:', e)
                    exit(1) # Exit the program
                    #continue

                try:
                    # Publish the sensor mac address, temperature, and timestamp data to Kafka
                    kafka_producer.send(kafka_topic, str([mac_address, temperature, datetime.now(timezone.utc)]).encode('utf-8'))
                except Exception as e:
                    print("Error when publishing to kafka: " + str(e))
                    #logger.error("Error when connecting with kafka consumer: " + str(e))
                    exit() # Exit the program

    except WyzeApiError as e:
        print(e) # Print the error
        exit() # Exit the program


#def main(storage_option, zip_code):

# Wyze credentials
email = os.environ.get('WYZE_USER')
password = os.environ.get('WYZE_PSWD')
kafka_server_address = os.environ.get('KAFKA_SERVER_ADDRESS')

# Get the Wyze API client
wyze_client = wyze_authentication(email, password)

# Create a Kafka producer
try:
    producer = KafkaProducer(bootstrap_servers=kafka_server_address)
except Exception as e:
    print('Failed to create Kafka producer:', e)
    exit(1)

# Define the topic to publish to
topic = 'wyze-room-sensor-temp'

# Loop to collect and publish data every 30 minutes
while True:
    # Get the temperature data from the Wyze Room Sensor
    temperature = get_wyze_temperatures(wyze_client, producer, topic)
    
    # Wait for 15 minutes before collecting data again
    time.sleep(15 * 60)
