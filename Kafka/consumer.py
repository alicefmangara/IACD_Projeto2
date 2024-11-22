import json

from kafka import KafkaConsumer

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'task1-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_serializer=lambda v: json.loads(v.decode('utf-8'))
)

# Continuously read and print messages from the topic
for message in consumer:
    temperature_reading = message.value
    print(f"Received: {temperature_reading}")