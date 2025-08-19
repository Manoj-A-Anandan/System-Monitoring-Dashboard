import json
import time
from collections import deque

import numpy as np
import psycopg2
from kafka import KafkaConsumer
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

TOPIC = "system-metrics"
WINDOW = 120 # keep last N samples for (re)training
REFIT_EVERY = 10        # retrain IF every N messages after warmup
MIN_TRAIN_SIZE = 10     # start predicting after this many samples
CONTAM = 0.05           # expected anomaly fraction
RANDOM_STATE = 42


def connect_kafka():
    while True:
        try:
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=["kafka:9092"],
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="metrics-group",
            )
            print("[consumer] connected to kafka.")
            return c
        except Exception as e:
            print(f"[consumer] kafka not ready ({e}), retrying in 3s...")
            time.sleep(3)


def connect_pg():
    while True:
        try:
            conn = psycopg2.connect(
                dbname="metricsdb",
                user="user",
                password="password",
                host="postgres",
                port="5432",
            )
            conn.autocommit = True
            print("[consumer] connected to postgres.")
            return conn
        except Exception as e:
            print(f"[consumer] postgres not ready ({e}), retrying in 3s...")
            time.sleep(3)


def ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS system_metrics (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            cpu FLOAT,
            memory FLOAT,
            disk FLOAT,
            anomaly BOOLEAN DEFAULT FALSE,
            score DOUBLE PRECISION
        );
        """
    )


def to_row(data):
    return np.array([[float(data["cpu"]), float(data["memory"]), float(data["disk"])]])


def main():
    consumer = connect_kafka()
    conn = connect_pg()
    cur = conn.cursor()
    ensure_table(cur)

    buffer = deque(maxlen=WINDOW)
    model = None
    seen = 0
    scaler = MinMaxScaler()

    print("[consumer] consuming + training...")
    for msg in consumer:
        try:
            data = msg.value
            ts = data["ts"]
            X = to_row(data) 

            buffer.append(X.flatten())
            seen += 1

            # Train model if enough data
            if len(buffer) >= MIN_TRAIN_SIZE and (model is None or seen % REFIT_EVERY == 0):
                arr = np.vstack(buffer)
                model = IsolationForest(
                    n_estimators=100,
                    contamination=CONTAM,
                    random_state=RANDOM_STATE,
                ).fit(arr)

                # Fit scaler on anomaly scores for normalization
                raw_scores = model.score_samples(arr)
                scaler.fit(raw_scores.reshape(-1, 1))
                print(f"[consumer] model refit on window={len(buffer)}")

            # Predict if model exists
            if model is not None:
                raw_score = model.score_samples(X)[0]  # higher = more normal
                norm_score = float(scaler.transform([[raw_score]])[0][0])
                pred = int(model.predict(X)[0])        # 1 normal, -1 anomaly
                anomaly = (pred == -1)
            else:
                norm_score = None
                anomaly = False

            # Insert into DB
            cur.execute(
                """
                INSERT INTO system_metrics (ts, cpu, memory, disk, anomaly, score)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (ts, float(data["cpu"]), float(data["memory"]), float(data["disk"]), anomaly, norm_score),
            )

            print(
                f"[consumer] ts={ts} cpu={data['cpu']:.1f} mem={data['memory']:.1f} "
                f"disk={data['disk']:.1f} anomaly={anomaly} score={norm_score}"
            )

        except Exception as e:
            print("[consumer] error:", e)


if __name__ == "__main__":
    main()
