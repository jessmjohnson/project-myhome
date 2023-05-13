import os
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError

logging.basicConfig(level=logging.INFO)

"""
This function authenticates to the Wyze API.
"""
def wyze_authentication(wyze_email: str, wyze_password: str) -> Client:
    try: 
        wyze_response = Client().login(email=wyze_email, password=wyze_password)
        wyze_access_token = wyze_response['access_token']
        client = Client(token=wyze_access_token)

        return client

    except WyzeApiError as e:
        logging.error(f"Failed to authenticate to Wyze API. Error: {e}")
        raise SystemExit(1)


def publish_motion_sensor_data(kafka_producer, kafka_topic, sensor_data):
    """Publishes the motion sensor data to Kafka topic."""
    try:
        # Convert sensor data to bytes
        sensor_data_bytes = str(sensor_data).encode('utf-8')
        
        # Publish data to Kafka topic
        kafka_producer.send(kafka_topic, sensor_data_bytes)
        kafka_producer.flush()
        
        logger.info("Motion sensor data published successfully")
    except Exception as e:
        logger.error("Failed to publish motion sensor data to Kafka: %s", str(e))

def collect_motion_sensor_data(wyze_client, kafka_producers, kafka_topic):
    """Collects motion sensor data using Wyze-SDK."""
    try:
        # Get list of motion sensors
        sensors = wyze_client.devices_list_by_category('MotionSensor')
        
        # Collect motion sensor data
        for sensor in sensors:
            sensor_data = sensor.info
            publish_motion_sensor_data(KafkaProducer, kafka_topic,  sensor_data)
        
        logger.info("Motion sensor data collection completed")
    except Exception as e:
        logger.error("Failed to collect motion sensor data: %s", str(e))


"""
This function gets the temperature data from the Wyze API.
"""
def get_wyze_temperatures(wyze_client: Client, kafka_producer: KafkaProducer, kafka_topic: str) -> None:
    try:
        my_thermostat = wyze_client.thermostats.list()[0]
        room_sensors = wyze_client.thermostats.get_sensors(device_mac=my_thermostat.mac, device_model='CO_EA1')

        for r in room_sensors:
            try:
                sensor_name = r.nickname
                device_id = r.did
                mac_address = r.mac
                product_model = 'WS01C' if r.product.model is None else r.product.model
                temperature = r.temperature
                humidity = r.humidity
                now_utc = datetime.now(timezone.utc)
                str_now_utc = now_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                message = str([mac_address, temperature, humidity, str_now_utc])

            except WyzeApiError as e:
                logging.error(f"Failed to get temperature data for device {r.nickname}. Error: {e}")
                continue

            try:
                kafka_producer.send(kafka_topic, message.encode('utf-8'))
                kafka_producer.flush()

            except Exception as e:
                logging.error(f"Failed to publish to Kafka. Error: {e}")
                raise SystemExit(1)

    except WyzeApiError as e:
        logging.error(f"Failed to get list of room sensors. Error: {e}")
        raise SystemExit(1)


if __name__ == '__main__':
    print("Starting Wyze Room Sensor to Kafka program @ " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # Wyze credentials
    email = os.environ.get('WYZE_USER')
    password = os.environ.get('WYZE_PSWD')
    kafka_server_address = os.environ.get('KAFKA_SERVER_ADDRESS')

    # Get the Wyze API client
    try:
        wyze_client = wyze_authentication(email, password)
    except SystemExit as e:
        logging.error("Exiting program due to authentication failure.")
        raise e

    # Create a Kafka producer
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_server_address)
    except Exception as e:
        logging.error(f"Failed to create Kafka producer. Error: {e}")
        raise SystemExit(1)

    # Define the topic to publish to
    temp_topic = 'wyze-room-sensor-temp'
    motion_topic = 'wyze-motion-sensor'

    # Get the temperature data from the Wyze Room Sensor
    try:
        get_wyze_temperatures(wyze_client, producer, temp_topic)
        collect_motion_sensor_data(wyze_client, producer, motion_topic)
        logging.info("Data published to Kafka.")
    except Exception as e:
        logging.error(f"An error occurred while getting temperature data or publishing to Kafka. Error: {e}")
        raise SystemExit(1)
        
    # Logout from Wyze account
    wyze_client.logout()

    print("Finished Wyze Room Sensor to Kafka program @ " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print("Exiting program.")
