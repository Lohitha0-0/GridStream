
import json
from collections import defaultdict, deque

from kafka import KafkaConsumer

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "smart-meter-readings"
CONSUMER_GROUP = "gridstream-consumer-group"

# Monitoring configuration
WINDOW_SIZE = 5
OVERLOAD_THRESHOLD = 180.0

# Store the latest 5 readings separately for each meter
meter_readings = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))


# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    key_deserializer=lambda key: key.decode("utf-8") if key else None
)


print("GridStream Smart Meter Consumer Started!")
print("Waiting for meter readings...\n")


try:
    for message in consumer:

        # Extract the received reading
        reading = message.value

        meter_id = reading["meter_id"]
        power_kw = reading["power_kw"]

        # Get the latest readings for this meter
        readings = meter_readings[meter_id]

        # Add the current reading to the rolling window
        readings.append(power_kw)

        # Wait until we have five readings
        if len(readings) < WINDOW_SIZE:
            print(
                f"Collecting readings for {meter_id}: "
                f"{len(readings)}/{WINDOW_SIZE}"
            )
            continue

        # Calculate rolling average
        average = sum(readings) / WINDOW_SIZE

        print(
            f"Meter: {meter_id} | "
            f"Current: {power_kw} kW | "
            f"Rolling Average: {average:.2f} kW"
        )

        # Check for overload
        if average > OVERLOAD_THRESHOLD:
            print(
                f"OVERLOAD ALERT! {meter_id} "
                f"average consumption is {average:.2f} kW\n"
            )


except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    consumer.close()
    print("Kafka consumer connection closed.")
