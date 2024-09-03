import os
from kafka import KafkaProducer
import requests
import time
from datetime import datetime as dt
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import csv

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')
WEATHER_API_URL = os.getenv('WEATHER_API_URL')
API_KEY = os.getenv('WEATHER_API_KEY')
LOCATIONS = os.getenv('LOCATIONS', '').split(',')

# Gmail configuration
EMAIL_SENDER = os.getenv('EMAIL_SENDER')
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER')
EMAIL_SERVER = 'smtp.gmail.com'
EMAIL_PORT = 587
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: v.encode('utf-8'))

def send_notification(message):
    """Send email notification if no data is available."""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = "Kafka Producer Notification: No Data to Stream"
        msg.attach(MIMEText(message, 'plain'))

        with smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

        print("Notification sent successfully.")
    except Exception as e:
        print(f"Failed to send notification: {e}")

def get_weather_data(location):
    """Fetch weather data for a given location."""
    params = {
        "key": API_KEY,
        "q": location,
        "aqi": "no"
    }

    try:
        response = requests.get(WEATHER_API_URL, params=params)
        response.raise_for_status()
        weather_data = response.json()

        if weather_data:
            place = weather_data["location"]["name"]
            region = weather_data["location"]["region"]
            country = weather_data["location"]["country"]
            continent = weather_data["location"]["tz_id"]
            local_time_str = weather_data["location"]["localtime"]
            current_temp = float(weather_data["current"]["temp_c"])
            feels_like_temp = float(weather_data["current"]["feelslike_c"])
            condition = weather_data["current"]["condition"]["text"]

            local_time = dt.strptime(local_time_str, "%Y-%m-%d %H:%M")

            # Prepare data for Kafka
            data = [local_time.strftime("%Y-%m-%d %H:%M:%S"), place, region, country, continent, current_temp, feels_like_temp, condition]
            csv_data = ','.join(map(str, data))  # Convert list to CSV string

            # Send data to Kafka
            producer.send(TOPIC, csv_data)
            producer.flush()

            # Save data to CSV file
            with open('weather_data.csv', mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # Write header if file is empty
                if file.tell() == 0:
                    writer.writerow(['Timestamp', 'Place', 'Region', 'Country', 'Continent', 'Current Temp (°C)', 'Feels Like Temp (°C)', 'Condition'])
                writer.writerow(data)

            print(f"Data appended for {location}: {data} for {TOPIC} topic")
        else:
            # Trigger notification if no data
            send_notification(f"No data available to stream for location: {location} at {dt.now()}")
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {location}: {e}")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

if __name__ == "__main__":
    try:
        while True:
            for location in LOCATIONS:
                get_weather_data(location)
            time.sleep(70)  # Sleep for 5 minutes before fetching data again for all locations
    except KeyboardInterrupt:
        print("Exiting the program.")
    finally:
        producer.close()
