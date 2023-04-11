
import boto3
import pandas as pd
import os
import psycopg2
import argparse

from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError
from datetime import datetime, timezone
from botocore.exceptions import ClientError

"""
    This function writes the temperature data to a PostgreSQL database.
"""
def write_to_postgresql(postgres_cxn_str, df_results):

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(postgres_cxn_str)

        # Create a cursor to execute SQL queries
        cur = conn.cursor()

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database > {e}")
        print("Connection String: {postgres_cxn_str}")
        exit() # Exit the program
        
    try:
        # Insert the temperature data into the table
        for index, row in df_results.iterrows():
            cur.execute("INSERT INTO wyze_temperature (sensor_name, device_id, mac_address, product_model, temperature, humidity, battery_level, is_online, create_dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (row['sensor_name'], row['device_id'], row['mac_address'], row['product_model'], row['temperature'], row['humidity'], row['battery_level'], row['is_online'], row['create_dt']))

    except psycopg2.Error as e:
        print(f"Error: Could not insert record into temperature table: {e}")
        exit() # Exit the program
    
    #  Commit the changes to the database and close the connection
    cur.close()
    conn.commit()
    conn.close()

    return 0;

"""
    This function writes the temperature data to an AWS S3 Bucket.
"""
def write_to_s3_bucket(aws_access_key, aws_secret_access_key, bucket_name, df_results):
    
    try:
        # Upload the temperature data to S3
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key)
        s3.put_object(Bucket=bucket_name, Key='temperature_data.txt', Body=df_results)
        return 'Successly wrote to AWS S3 Bucket.'

    except ClientError as e:
        print(f"Error: Could not write to AWS S3 Bucket: {e}")
        return None;

"""
    This function writes the temperature data to an Azure Blob Storage.
"""
def write_to_azure_blob(df_results):
    return 0;

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
def get_wyze_temperatures(client):

    # Get the list of all Wyze devices associated with your account
    try:
        # Get the list of all Wyze devices associated with your account
        my_thermostat =client.thermostats.list()[0]  #get_user_devices()

        # Get a list of all Room Sensors for Thermostat devices
        room_sensors = client.thermostats.get_sensors(device_mac=my_thermostat.mac, device_model='CO_EA1')

        # create a new Pandas DataFrame with explicit column names and data types for the temperature data
        temperature_df = pd.DataFrame(columns=['sensor_name', 'device_id', 'mac_address', 'product_model', 'temperature', 'humidity', 'battery_level', 'is_online', 'create_dt'])
        temperature_df = temperature_df.astype(dtype={'sensor_name': 'object', 'device_id': 'object', 'mac_address': 'object', 'product_model': 'object', 'temperature': 'float64', 'humidity': 'float64', 'battery_level': 'int64', 'is_online': 'bool', 'create_dt': 'datetime64'})

        # Loop through the temperature sensors and insert readings into the database
        for r in room_sensors:
                sensor_name = r.nickname # Get the sensor name
                device_id = r.did # Get the device ID
                mac_address = r.mac # Get the MAC address
                product_model = 'WS01C' if r.product.model is None else r.product.model # Get the product model
                temperature = r.temperature # Get the temperature
                humidity = r.humidity # Get the humidity
                create_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M") # Get the current date and time
                is_online = r.is_online # Get the online status
                battery_level = int(r.battery.name.split('_')[1]) # Extract the integer value of the battery level

                # Create a new row in the dataframe
                new_row_df = pd.DataFrame([[sensor_name, device_id, mac_address, product_model, temperature, humidity, battery_level, is_online, create_dt]], columns=['sensor_name', 'device_id', 'mac_address', 'product_model', 'temperature', 'humidity', 'battery_level', 'is_online', 'create_dt'])
                
                # Append the new row to the dataframe
                temperature_df = pd.concat([temperature_df, new_row_df], ignore_index=True)

    except WyzeApiError as e:
        print(e) # Print the error
        exit() # Exit the program

    # Return the temperature data
    return temperature_df

def main(storage_option):

    # Wyze credentials
    email = os.environ.get('WYZE_USER')
    password = os.environ.get('WYZE_PSWD')

    # Get the Wyze API client
    wyze_client = wyze_authentication(email, password)

    # Get the temperature data
    temperature_df = get_wyze_temperatures(wyze_client)

    if storage_option == 'AWS S3 Bucket':
        AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
        AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
        BUCKET_NAME = os.environ.get('BUCKET_NAME')

        # Write the temperature data to AWS S3 Bucket
        write_to_s3_bucket(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, temperature_df)

    elif storage_option == 'Azure Blob':
        # Write the temperature data to Azure Blob
        write_to_azure_blob(temperature_df)

    elif storage_option == 'PostgreSQL':

        # Get the connection string from the environment variable
        conn_string = os.environ.get('MYHOME_IOT_DB_URL')

        # Write the temperature data to PostgreSQL
        write_to_postgresql(conn_string, temperature_df)

    else:
        # Print the temperature data to the console
        print(temperature_df)

if __name__ == "__main__":

    # Initialize the ArgumentParser
    parser = argparse.ArgumentParser()

    # Add the 'output' argument as an optional argument with a default value of 'PostgreSQL'
    parser.add_argument('--storage', help='The output storage type: AWS S3 Bucket, Azure Blob, PostgreSQL or Print', default='Print', choices=('AWS S3 Bucket', 'Azure Blob', 'PostgreSQL', 'Print'))

    # Parse the arguments
    args = parser.parse_args()

    # Access the 'output-storage' argument value
    storage = args.storage

    # Use the output file path in your script
    print(f'The output-type is: {storage}')

    main(storage)

    ## python script.py --output CSV
