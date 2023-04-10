from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError
from datetime import datetime, timezone

import boto3
import pandas as pd
import os
import psycopg2

# Wyze credentials
email = os.environ.get('WYZE_USER')
password = os.environ.get('WYZE_PSWD')


def write_to_postgresql(df_results):

    # Postgres Credentials
    postgres_user = os.environ.get('ETL_USER')
    postgres_pwd = os.environ.get('ETL_PASSWORD')

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            database="myhome_iot",
            user=postgres_user,
            password=postgres_pwd, 
            host="localhost"
        )

        # Create a cursor to execute SQL queries
        cur = conn.cursor()

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        
    try:
        # Insert the temperature data into the table
        for index, row in df_results.iterrows():
            cur.execute("INSERT INTO wyze_temperature (sensor_name, device_id, mac_address, product_model, temperature, humidity, battery_level, is_online, create_dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (row['sensor_name'], row['device_id'], row['mac_address'], row['product_model'], row['temperature'], row['humidity'], row['battery_level'], row['is_online'], row['create_dt']))

    except psycopg2.Error as e:
        print(f"Error: Could not insert record into temperature table: {e}")
    
    #  Commit the changes to the database and close the connection
    cur.close()
    conn.commit()
    conn.close()

    return 0;

def write_to_s3_bucket(df_results):
    
    # AWS credentials
    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY = ''
    S3_BUCKET_NAME = ''

    try:
        # Upload the temperature data to S3
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        s3.put_object(Bucket=S3_BUCKET_NAME, Key='temperature_data.txt', Body=df_results)

    except e:
        print(f"Error: Count not write to AWS S3 Bucket: {e}")

    return 0;

def write_to_azure_blob(df_results):
    return 0;


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
    exit() # Exit the program

# Create a Pandas dataframe from temperature data
temperature_df = pd.DataFrame(columns=['sensor_name', 'device_id', 'mac_address', 'product_model', 'temperature', 'humidity', 'battery_level', 'is_online', 'create_dt'])

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

        temperature_df = temperature_df.append(
            {
                'sensor_name': sensor_name, 
                'device_id': device_id,
                'mac_address': mac_address,
                'product_model': product_model,
                'temperature': temperature,
                'humidity': humidity,
                'battery_level': battery_level,
                'is_online': is_online,
                'create_dt': create_dt
            }
            , ignore_index=True
        )

    except WyzeApiError as e:
        print(e) # Print the error
        exit() # Exit the program

print(temperature_df)

# Logout from the Wyze Client
#client.logout()