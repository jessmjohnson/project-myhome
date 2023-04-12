
import boto3
import pandas as pd
import os
import psycopg2
import argparse
import json
import requests

from wyze_sdk import Client
from wyze_sdk.errors import WyzeApiError
from datetime import datetime, timezone
from botocore.exceptions import NoCredentialsError


"""
This function gets the current temperature and humidity from OpenWeatherMap using the OpenWeatherMap API.
"""
def get_openweathermap(openweathermap_api_key, zip_code):

    # Make a GET request to the OpenWeatherMap API
    response = requests.get(f'https://api.openweathermap.org/data/2.5/weather?zip={zip_code},us&appid={openweathermap_api_key}&units=imperial')

    # Check if the request was successful
    if response.status_code == 200:
        # Extract the current weather information
        data = response.json()
        current_weather = {
            'temp_f': data['main']['temp'],
            'humidity': data['main']['humidity']
        }

    else:
        # Print an error message if the request was unsuccessful
        print(f"Error {response.status_code}: {response.text}")
        exit() # Exit the program

    return current_weather # Return the current weather information

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
            cur.execute("INSERT INTO wyze_temperature (sensor_name, device_id, mac_address, product_model, temperature, humidity, battery_level, is_online, current_temperature, current_humidity, create_dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (row['sensor_name'], row['device_id'], row['mac_address'], row['product_model'], row['temperature'], row['humidity'], row['battery_level'], row['is_online'], row['current_temperature'], row['current_humidity'], row['create_dt']))

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

    current_time = datetime.now().strftime('%Y%m%d_%H%M')   # Get the current date and time
    file_name = f'temperature_data_{current_time}.json'     # Create the file name for the temperature data

    # Write the DataFrame to a JSON file
    json_data = df_results.to_json(orient='records')

    # Upload the JSON file to S3
    try:
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key)
        json_string = json.dumps(json_data)

        # Write the string to the S3 bucket
        s3.Object(bucket_name, file_name).put(Body=json_string)

        print("File uploaded successfully to S3")
    except NoCredentialsError:
        print("AWS credentials not available")
        exit() # Exit the program
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

def main(storage_option, zip_code):

    # Wyze credentials
    email = os.environ.get('WYZE_USER')
    password = os.environ.get('WYZE_PSWD')

    # Get the Wyze API client
    wyze_client = wyze_authentication(email, password)

    # Get the temperature data
    temperature_df = get_wyze_temperatures(wyze_client)

    # Get the OpenWeatherMap API key
    openweathermap_api_key = os.environ.get('OPENWEATHERMAP_API')

    # Get the current weather data
    current_weather_dict = get_openweathermap(openweathermap_api_key, zip_code)

    # Add 'current_temperature' and 'current_humidity' columns to temperature_df
    temperature_df['current_temperature'] = current_weather_dict['temp_f']
    temperature_df['current_humidity'] = current_weather_dict['humidity']

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
    parser.add_argument('--storage', help='The output storage type: AWS S3, Azure Blob, PostgreSQL or Print', default='Print', choices=('AWS S3', 'Azure Blob', 'PostgreSQL', 'Print'))

    parser.add_argument('--zipcode', help='5 Digit Zip code', default='15212')

    # Parse the arguments
    args = parser.parse_args()

    # Access the 'output-storage' argument value
    storage = args.storage

    # Access the 'zipcode' argument value
    zipcode = args.zipcode

    # print(f'The output storage type is: {storage}')

    main(storage, zipcode)
