import json
from kafka import KafkaConsumer


TOPIC_NAME = "machine_sensor_events"
BOOTSTRAP_SERVERS = "localhost:9092"


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="smart-factory-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    for message in consumer:
        print(message.value)


if __name__ == "__main__":
    main()
