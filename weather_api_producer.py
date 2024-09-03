import os
from kafka import KafkaProducer
import requests
import time
from datetime import datetime as dt
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC')
WEATHER_API_URL = os.getenv('WEATHER_API_URL')
API_KEY = os.getenv('WEATHER_API_KEY')
LOCATIONS = os.getenv('LOCATIONS', '').split(',')

# PostgreSQL configuration
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Gmail configuration
EMAIL_SENDER = os.getenv('EMAIL_SENDER')
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER')
EMAIL_SERVER = 'smtp.gmail.com'
EMAIL_PORT = 587
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: v.encode('utf-8'))

# Connect to PostgreSQL
def connect_postgres():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Send email notification if no data is available
def send_notification(message):
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

# Insert data into PostgreSQL
def insert_into_postgres(data):
    conn = connect_postgres()
    if conn:
        try:
            cursor = conn.cursor()
            insert_query = sql.SQL("""
                INSERT INTO weather_data (timestamp, place, region, country, continent, current_temp_c, feels_like_temp_c, condition)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, data)
            conn.commit()
            cursor.close()
            print("Data inserted into PostgreSQL.")
        except Exception as e:
            print(f"Error inserting data into PostgreSQL: {e}")
            conn.rollback()
        finally:
            conn.close()

def get_weather_data(location):
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

            # Insert data into PostgreSQL
            insert_into_postgres(data)

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
            time.sleep(10)  # Sleep for 5 minutes before fetching data again for all locations
    except KeyboardInterrupt:
        print("Exiting the program.")
    finally:
        producer.close()
