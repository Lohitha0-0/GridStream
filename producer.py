
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# Kafka broker address
KAFKA_SERVER = "localhost:9092"

# Kafka topic name
TOPIC_NAME = "smart-meter-readings"

# Simulated smart meter IDs
METER_IDS = ["METER_01", "METER_02", "METER_03"]


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,

    # Convert dictionary into JSON bytes
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),

    # Convert meter ID into bytes
    key_serializer=lambda key: key.encode("utf-8")
)


def generate_reading(meter_id):
    """Generate a simulated electricity reading."""

    reading = {
        "meter_id": meter_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "power_kw": round(random.uniform(50, 250), 2)
    }

    return reading


print("GridStream Smart Meter Producer Started!")
print("Press CTRL+C to stop.\n")


try:
    while True:

        for meter_id in METER_IDS:

            # Generate a reading
            reading = generate_reading(meter_id)

            # Send reading to Kafka
            future = producer.send(
                TOPIC_NAME,
                key=meter_id,
                value=reading
            )

            # Wait for Kafka acknowledgement
            metadata = future.get(timeout=10)

            print(
                f"Sent: {reading} | "
                f"Partition: {metadata.partition}"
            )

        # Wait before generating the next batch
        time.sleep(2)


except KeyboardInterrupt:
    print("\nProducer stopped by user.")

finally:
    producer.flush()
    producer.close()
    print("Kafka producer connection closed.")
