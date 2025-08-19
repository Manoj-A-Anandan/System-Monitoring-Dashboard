# 🔍 Real-Time System Monitoring with Kafka + Anomaly Detection

This project simulates system metrics (CPU, Memory, Disk), streams them via Kafka,
detects anomalies using ML (Isolation Forest), stores results
in PostgreSQL, and visualizes them with Streamlit.

## 🚀 Features
- Kafka Producer: streams CPU, Memory, Disk usage
- Kafka Consumer: anomaly detection (ML)
- PostgreSQL: stores metrics + anomalies
- Streamlit Dashboard: live charts and anomaly alerts
- Dockerized: run everything with `docker-compose up`

## 📦 Tech Stack
- Python (psutil, scikit-learn, psycopg2, kafka-python, streamlit)
- Apache Kafka
- PostgreSQL
- Docker + Docker Compose

## ⚡ Run Locally
```bash
git clone https://github.com/<your-username>/system-monitoring.git
cd system-monitoring
docker-compose up --build
