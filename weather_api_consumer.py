# Imported all required libraries
import os
from kafka import KafkaConsumer
from psycopg2 import connect, sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection settings from .env
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Kafka settings from .env
TOPIC = os.getenv('KAFKA_TOPIC')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

# Connect to PostgreSQL
try:
    conn = connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("Connected to PostgreSQL database successfully.")

    # Create the weather_data table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        timestamp TIMESTAMP,
        place VARCHAR(50),
        region VARCHAR(50),
        country VARCHAR(50),
        continent VARCHAR(50),
        current_temp_c DOUBLE PRECISION,
        feels_like_temp_c DOUBLE PRECISION,
        condition VARCHAR(100)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Ensured that the weather_data table exists in PostgreSQL.")

except Exception as e:
    print(f"Error connecting to PostgreSQL or creating table: {e}")
    exit(1)

# Create a Kafka consumer for the specified topic
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-consumer-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

print(f"Reading messages from the topic: {TOPIC}")

# Consuming messages from the Kafka topic
for msg in consumer:
    # Message should be a comma-separated string
    message = msg.value.split(',')

    # Check the length to match the expected columns
    if len(message) == 8:
        # Unpack the message into variables
        timestamp, place, region, country, continent, current_temp_c, feels_like_temp_c, condition = message

        # Insert the data into the PostgreSQL table
        insert_query = sql.SQL("""
            INSERT INTO weather_data (timestamp, place, region, country, continent, current_temp_c, feels_like_temp_c, condition)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cursor.execute(insert_query, (timestamp, place, region, country, continent, float(current_temp_c), float(feels_like_temp_c), condition))
            conn.commit()
            print(f"Inserted data for {place} into PostgreSQL.")
        except Exception as e:
            print(f"Error inserting data into PostgreSQL: {e}")
            conn.rollback()  # Rollback in case of error

# Close the PostgreSQL connection when done
cursor.close()
conn.close()
