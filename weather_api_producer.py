# Import all the required libraries

from kafka import KafkaProducer
import requests
import csv
import time
from datetime import datetime as dt
from dotenv import load_dotenv
import os

# Optional: Load environment variables from .env file
load_dotenv()

"""
# Debug: Print environment variables to confirm they are loaded
print("Loaded environment variables:")
print(f"KAFKA_BOOTSTRAP_SERVERS: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
print(f"KAFKA_TOPIC: {os.getenv('KAFKA_TOPIC')}")
print(f"WEATHER_API_URL: {os.getenv('WEATHER_API_URL')}")
print(f"WEATHER_API_KEY: {os.getenv('WEATHER_API_KEY')}")
print(f"LOCATIONS: {os.getenv('LOCATIONS')}")
"""

# Here, get configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')
WEATHER_API_URL = os.getenv('WEATHER_API_URL')
api_key = os.getenv('WEATHER_API_KEY')
locations = os.getenv('LOCATIONS', '').split(',')  # Default to empty string if LOCATIONS is not set

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: v.encode("utf-8"))

def get_weather_data(location):
    params = {
        "key": api_key,
        "q": location,
        "aqi": "no"
    }

    try:
        response = requests.get(WEATHER_API_URL, params=params)
        response.raise_for_status()
        weather_data = response.json()

        place = weather_data["location"]["name"]
        region = weather_data["location"]["region"]
        country = weather_data["location"]["country"]
        continent = weather_data["location"]["tz_id"]
        local_time_str = weather_data["location"]["localtime"]
        current_temp = float(weather_data["current"]["temp_c"])
        feels_like_temp = float(weather_data["current"]["feelslike_c"])
        condition = weather_data["current"]["condition"]["text"]

        local_time = dt.strptime(local_time_str, "%Y-%m-%d %H:%M")

        # Prepare data for CSV and Kafka
        data = [local_time.strftime("%Y-%m-%d %H:%M:%S"), place, region, country, continent, current_temp, feels_like_temp, condition]
        csv_data = ','.join(map(str, data))  # Convert list to CSV string

        # Send data to Kafka
        producer.send(TOPIC, csv_data)
        producer.flush()

        # Append data to CSV file
        with open('weather_data.csv', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            # Write header if file is empty
            if file.tell() == 0:
                writer.writerow(['Timestamp', 'Place', 'Region', 'Country', 'Continent', 'Current Temp (°C)', 'Feels Like Temp (°C)', 'Condition'])
            writer.writerow(data)

        print(f"Data appended for {location}: {data} for {TOPIC} topic")
        
    # Error Handling
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {location}: {e}")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

if __name__ == "__main__":
    try:
        while True:
            for location in locations:
                get_weather_data(location)
            time.sleep(300)  # Sleep for 10 minutes before fetching data again for all locations
    except KeyboardInterrupt:
        print("Exiting the program.")
    finally:
        producer.close()
