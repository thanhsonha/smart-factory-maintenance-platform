import json
import time
from datetime import datetime, timedelta

import pandas as pd
from kafka import KafkaProducer


TOPIC_NAME = "machine_sensor_events"
BOOTSTRAP_SERVERS = "localhost:9092"
DATA_PATH = "data/raw/ai4i2020.csv"


def clean_column_name(col: str) -> str:
    return (
        col.lower()
        .replace(" ", "_")
        .replace("[", "")
        .replace("]", "")
        .replace("/", "_")
    )


def main():
    df = pd.read_csv(DATA_PATH)
    df.columns = [clean_column_name(c) for c in df.columns]

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    base_time = datetime.now()

    for i, row in df.iterrows():
        event = {
            "event_time": (base_time + timedelta(seconds=i)).isoformat(),
            "machine_id": f"M_{int(row['udi']):05d}",
            "product_id": row["product_id"],
            "machine_type": row["type"],
            "air_temperature_k": float(row["air_temperature_k"]),
            "process_temperature_k": float(row["process_temperature_k"]),
            "rotational_speed_rpm": int(row["rotational_speed_rpm"]),
            "torque_nm": float(row["torque_nm"]),
            "tool_wear_min": int(row["tool_wear_min"]),
            "machine_failure": int(row["machine_failure"]),
            "twf": int(row["twf"]),
            "hdf": int(row["hdf"]),
            "pwf": int(row["pwf"]),
            "osf": int(row["osf"]),
            "rnf": int(row["rnf"]),
        }

        producer.send(TOPIC_NAME, value=event)
        print(f"Sent: {event}")
        time.sleep(0.1)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
