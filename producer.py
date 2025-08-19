import json
import time
import random
from datetime import datetime, timezone

import psutil
from kafka import KafkaProducer


def make_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                retries=5,
            )
        except Exception as e:
            print(f"[producer] kafka not ready ({e}), retrying in 3s...")
            time.sleep(3)


def get_metrics(anomaly=False):
    if anomaly:
        # Inject abnormal values
        cpu = random.uniform(90, 100)
        mem = random.uniform(90, 100)
        disk = random.uniform(90, 100)
    else:
        # Normal system metrics
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent
        disk = psutil.disk_usage("/").percent

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "cpu": float(cpu),
        "memory": float(mem),
        "disk": float(disk),
    }


def main():
    topic = "system-metrics"
    producer = make_producer()
    print("[producer] streaming to topic:", topic)

    while True:
        # 10% chance of anomaly
        anomaly_flag = random.random() < 0.1
        payload = get_metrics(anomaly=anomaly_flag)

        producer.send(topic, value=payload)

        if anomaly_flag:
            print("[producer] 🚨 Injected anomaly:", payload)
        else:
            print("[producer] sent:", payload)

        time.sleep(3)


if __name__ == "__main__":
    main()
