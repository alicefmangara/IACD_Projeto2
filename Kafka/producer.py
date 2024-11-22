import json
import random
import time

from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate random temperature readings
def generate_temperature_reading(sensor_id):
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(15.0, 30.0), 2)
    }

# Send temperature readings every 2 seconds
sensor_id = 1
while True:
    temperature_reading = generate_temperature_reading(sensor_id)
    producer.send('task1-topic', temperature_reading)
    print(f"Sent: {temperature_reading}")
    time.sleep(2)