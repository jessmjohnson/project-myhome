import os
from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError

email = os.environ.get('WYZE_USER')
password = os.environ.get('WYZE_PSWD')

try:
    # authentication
    wyze_response = Client().login(email=email, password=password)   # Create a new client and Login to the client 
    wyze_access_token = wyze_response['access_token'] # Get the access token
    wyze_refresh_token = wyze_response['refresh_token'] # Get the refresh token
    client = Client(token=wyze_access_token)

    # Get a list of all Room Sensors for Thermostat devices
    room_sensors = client.thermostats.get_sensors(device_mac='CO_EA1_52324837043835324e325cac', device_model='CO_EA1')
    for s in room_sensors:
        sensor_mac = s.mac
        sensor_name = s.nickname
        sensor_is_online = s.is_online
        sensor_product_model = s.product.model
        sensor_temperature = s.temperature

        print(f'{sensor_name} is {sensor_temperature} degrees and is {sensor_is_online} online.')

except WyzeApiError as e:
    # You will get a WyzeApiError if the request failed
    print(f"Got an error: {e}")