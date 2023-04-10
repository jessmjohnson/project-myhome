from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError
from datetime import datetime, timezone
import os
import psycopg2

email = os.environ.get('WYZE_USER')
password = os.environ.get('WYZE_PSWD')

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database="myhome_iot",
        user='ETL_USER',
        password='[ETL PASSWORD]', 
        host="localhost"
    )

    # Create a cursor to execute SQL queries
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")

try: 
    # authentication
    wyze_response = Client().login(email=email, password=password)   # Create a new client and Login to the client 
    wyze_access_token = wyze_response['access_token'] # Get the access token
    client = Client(token=wyze_access_token)

    # Get the list of all Wyze devices associated with your account
    my_thermostat =client.thermostats.list()[0]  #get_user_devices()

    # Get a list of all Room Sensors for Thermostat devices
    room_sensors = client.thermostats.get_sensors(device_mac=my_thermostat.mac, device_model='CO_EA1')

except WyzeApiError as e:
    # You will get a WyzeApiError if the request failed
    print(f"Got an error: {e}")
    conn.close() # Close communication with the database
    exit() # Exit the program

# Loop through the temperature sensors and insert readings into the database
for r in room_sensors:
    try:
        sensor_name = r.nickname # Get the sensor name
        device_id = r.did # Get the device ID
        mac_address = r.mac # Get the MAC address
        product_model = 'WS01C' if r.product.model is None else r.product.model # Get the product model
        temperature = r.temperature # Get the temperature
        humidity = r.humidity # Get the humidity
        create_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M") # Get the current date and time
        is_online = r.is_online # Get the online status
        battery_level = int(r.battery.name.split('_')[1]) # Extract the integer value of the battery level

    except WyzeApiError as e:
        print(e) # Print the error
        conn.close() # Close communication with the database
        exit() # Exit the program
    
    try:
        cur.execute("INSERT INTO wyze_temperature (sensor_name, device_id, mac_address, product_model, temperature, humidity, battery_level, is_online, create_dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (sensor_name, device_id, mac_address, product_model, temperature, humidity, battery_level, is_online, create_dt))
    except psycopg2.Error as e:
        print(f"Error: Could not insert record into temperature table: {e}")

# Commit the changes to the database and close the connection
cur.close()
conn.commit()
conn.close()

"""
# Create a Wyze client with your access token
wyze_client = Client(token=wyze_access_token)

# Get a list of devices and filter for temperature sensors
try:
    devices = wyze_client.devices_list()
    sensors = [d for d in devices if d is not None and d.product.model == 'WS01C' and d.product.definition is not None]
except WyzeApiError as e:
    print(e)
    conn.close()
    exit()

# Loop through the temperature sensors and insert readings into the database
for sensor in sensors:
    try:
        temperature = sensor.sensor.temperature.get_reading().value
        humidity = sensor.sensor.humidity.get_reading().value
        cur.execute("INSERT INTO temperature_readings (device_id, temperature, humidity) VALUES (%s, %s, %s)", (sensor.mac, temperature, humidity))
    except WyzeApiError as e:
        print(e)

# Commit the changes to the database and close the connection
conn.commit()
conn.close()

"""
"""
if my_thermostat:
    # Get the list of room sensors associated with the thermostat
    room_sensors = client.thermostats.get_sensors(device_mac=my_thermostat.mac, device_model=my_thermostat.product.model)

    # Set the date range for the data you want to export
    start_time = datetime.datetime(2022, 3, 1, 0, 0, 0)
    end_time = datetime.datetime(2022, 4, 1, 0, 0, 0)

    # Retrieve the temperature and humidity data for the date range for each room sensor
    for room_sensor in room_sensors:
        #temperature_data = client.get_device_history(room_sensor.mac, start_time, end_time, room_sensor.DeviceEventType.TEMPERATURE)
        #humidity_data = client.get_device_history(room_sensor.mac, start_time, end_time, room_sensor.DeviceEventType.HUMIDITY)
        
        print(client.events.get_events(start_time, end_time, room_sensor.mac))
        
        # Print out the temperature and humidity readings for the room sensor
        #for reading in temperature_data:
        #    print(f'Room Sensor: {room_sensor.nickname}, Temperature: {reading.value}°F, Timestamp: {reading.created_at}')
        #for reading in humidity_data:
        #    print(f'Room Sensor: {room_sensor.nickname}, Humidity: {reading.value}%, Timestamp: {reading.created_at}')
else:
    print('No Wyze thermostats found.')
"""

"""
# If we found a thermostat, retrieve the historical data for the associated room sensor(s)
if thermostat:
    # Get the list of room sensors associated with the thermostat
    room_sensors = client.thermostats.get_sensors(device_mac='CO_EA1_52324837043835324e325cac', device_model='CO_EA1')

    # Set the date range for the data you want to export
    start_time = datetime.datetime(2022, 3, 1, 0, 0, 0)
    end_time = datetime.datetime(2022, 4, 1, 0, 0, 0)

    # Retrieve the temperature and humidity data for the date range for each room sensor
    for room_sensor in room_sensors:
        temperature_data = client.get_device_history(room_sensor.mac, start_time, end_time, room_sensor.DeviceEventType.TEMPERATURE)
        humidity_data = client.get_device_history(room_sensor.mac, start_time, end_time, room_sensor.DeviceEventType.HUMIDITY)

        # Print out the temperature and humidity readings for the room sensor
        for reading in temperature_data:
            print(f'Room Sensor: {room_sensor.nickname}, Temperature: {reading.value}°F, Timestamp: {reading.created_at}')
        for reading in humidity_data:
            print(f'Room Sensor: {room_sensor.nickname}, Humidity: {reading.value}%, Timestamp: {reading.created_at}')
else:
    print('No Wyze thermostats found.')
 """