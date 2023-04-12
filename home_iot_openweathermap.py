import requests

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