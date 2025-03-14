import pandas as pd
import json
from time import time
# from confluent_kafka import Producer
from kafka import KafkaProducer


# Define Redpanda broker
KAFKA_BROKER = "localhost:9092"  # Update if needed
TOPIC_NAME = "green-trips"

# Create Kafka Producer
# producer = Producer({'bootstrap.servers': KAFKA_BROKER})
# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Serialize JSON
)


# Load CSV.gz file
df = pd.read_csv("green_tripdata_2019-10.csv.gz")

# Keep only required columns
df = df[['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID',
         'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']]

# Convert data to a list of dictionaries
data = df.to_dict(orient="records")

# Start timing
t0 = time()

# Send each row as a message to Redpanda
for message in data:
    producer.send(TOPIC_NAME, value=message)
    #     producer.produce(TOPIC_NAME, value=json.dumps(message).encode("utf-8"))

# Flush messages
producer.flush()

# End timing
t1 = time()
took = t1 - t0

print(f"Sent {len(data)} messages. Time taken: {took:.2f} seconds")
