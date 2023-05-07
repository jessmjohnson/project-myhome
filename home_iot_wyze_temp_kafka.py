import os
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError


def setup_logging(level: int = logging.INFO) -> None:
    """Set up logging configuration"""
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=level
    )


def create_kafka_producer(bootstrap_server_address: str) -> KafkaProducer:
    """Set up and return a KafkaProducer instance using the given bootstrap servers address.

    Args:
        bootstrap_server_address (str): The Kafka bootstrap servers address.

    Returns:
        KafkaProducer: A KafkaProducer instance.
    """
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_server_address)
    except Exception as e:
        logging.error(f'Failed to create Kafka producer: {e}')
        raise

    return producer


def authenticate_wyze_api(email: str, password: str) -> Client:
    """
    Authenticate to the Wyze API and return a client object.

    Args:
        email (str): Wyze account email.
        password (str): Wyze account password.

    Returns:
        Client: Wyze API client object.
    """
    try:
        client = Client().login(email=email, password=password)
        return client
    except WyzeApiError as e:
        logging.error(f"Wyze API authentication failed with error: {e}")
        raise


def get_wyze_temperatures(wyze_client: Client, kafka_producer: KafkaProducer, topic: str) -> None:
    """
    Get the temperature data from the Wyze API and publish it to Kafka.

    Args:
        wyze_client (Client): Wyze API client object.
        kafka_producer (KafkaProducer): Kafka producer object.
        topic (str): Kafka topic to publish the data to.
    """
    try:
        my_thermostat = None
        for device in wyze_client.devices_list:
            if device.product_type == 'THERMOSTAT':
                my_thermostat = device
                break

        if my_thermostat is None:
            logging.error('No thermostat found in Wyze account')
            raise

        #thermostat = wyze_client.thermostats.list()[0]
        room_sensors = wyze_client.thermostats.get_sensors(device_mac=my_thermostat.mac, device_model='CO_EA1')
        for r in room_sensors:
            try:
                sensor_name = r.nickname
                device_id = r.did
                mac_address = r.mac
                product_model = 'WS01C' if r.product.model is None else r.product.model
                temperature = r.temperature
                humidity = r.humidity if r.humidity is not None else 0
                timestamp = datetime.now(timezone.utc)
            except WyzeApiError as e:
                logging.error(f"Failed to get temperature data for {r.nickname} with error: {e}")
                continue

            try:
                message = [mac_address, temperature, humidity, timestamp]
                kafka_producer.send(topic, str(message).encode('utf-8'))
            except Exception as e:
                logging.error(f"Failed to publish to Kafka with error: {e}")
                raise
    except WyzeApiError as e:
        logging.error(f"Failed to get Wyze temperature data with error: {e}")
        raise


def main() -> None:
    # logging setup
    setup_logging()

    # get credentials
    email = os.environ.get('WYZE_USER')
    password = os.environ.get('WYZE_PSWD')
    kafka_server_address = os.environ.get('KAFKA_SERVER_ADDRESS')

    # create Wyze client object
    try:
        wyze_client = authenticate_wyze_api(email, password)
    except:
        logging.error("Wyze API authentication failed")
        raise
    
    # create Kafka producer object
    producer = create_kafka_producer(kafka_server_address)
    
    # get temperature data and publish to Kafka
    try:
        get_wyze_temperatures(wyze_client, producer, 'wyze-room-sensor-temp')
    except:
        logging.error("Failed to get and publish temperature data")
        raise

if __name__ == '__main__':
    main()