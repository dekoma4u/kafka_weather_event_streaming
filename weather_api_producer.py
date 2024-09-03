import os
from kafka import KafkaConsumer
from psycopg2 import connect, sql
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
from datetime import datetime as dt

# Load environment variables from .env file
load_dotenv()

# Kafka and database configuration
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}
TOPIC = os.getenv('KAFKA_TOPIC')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

# Email configuration
EMAIL_SENDER = os.getenv('EMAIL_SENDER')
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER')
EMAIL_SERVER = os.getenv('EMAIL_SERVER')
EMAIL_PORT = int(os.getenv('EMAIL_PORT'))
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

def send_notification(subject, message):
    """Send email notification."""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))

        with smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT) as server:
            server.starttls()  # Upgrade the connection to a secure encrypted SSL/TLS connection
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

        print("Notification sent successfully.")
    except Exception as e:
        print(f"Failed to send notification: {e}")

def create_consumer():
    """Create and return a Kafka consumer."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-consumer-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

def connect_postgres():
    """Connect to PostgreSQL."""
    try:
        conn = connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def insert_into_postgres(data):
    """Insert data into PostgreSQL."""
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

def check_consumer_health(consumer):
    """Check if the Kafka consumer is still working."""
    try:
        # Attempt to fetch a single message
        msg = next(consumer, None)
        if msg is None:
            send_notification(
                "Kafka Consumer Alert",
                "No messages received from Kafka. The consumer might be down."
            )
            return False
    except Exception as e:
        send_notification(
            "Kafka Consumer Alert",
            f"Error with Kafka consumer: {e}"
        )
        return False
    return True

def main():
    """Main function to run the Kafka consumer."""
    try:
        consumer = create_consumer()
        print(f"Reading messages from the topic: {TOPIC}")

        while True:
            if not check_consumer_health(consumer):
                # Attempt to recreate the consumer if health check fails
                consumer = create_consumer()
                print("Recreated Kafka consumer.")
            else:
                for msg in consumer:
                    # Process the message
                    message = msg.value.split(',')

                    if len(message) == 8:
                        # Unpack the message into variables
                        timestamp, place, region, country, continent, current_temp_c, feels_like_temp_c, condition = message
                        data = (timestamp, place, region, country, continent, float(current_temp_c), float(feels_like_temp_c), condition)

                        # Insert data into PostgreSQL
                        insert_into_postgres(data)
                        print(f"Message processed and inserted into PostgreSQL: {message}")
            
            # Sleep for 60 seconds before checking again
            time.sleep(60)  
    except KeyboardInterrupt:
        print("Exiting the program.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
