# Real-Time E-Commerce Streaming Pipeline

Portfolio project using **Kafka + PySpark Structured Streaming**.

## Architecture
```text
Order Event Producer -> Kafka -> PySpark Structured Streaming
                              |-> Bronze raw Parquet
                              |-> Bad records
                              |-> Silver validated/deduplicated orders
                              |-> Gold 1-minute revenue metrics
```

## Demonstrates
Kafka, PySpark Structured Streaming, explicit schemas, event time, watermarking, deduplication, windowed aggregations, checkpoints, data-quality routing, Parquet, Docker, tests, CI.

## Requirements
Python 3.11, Java 17, Docker Desktop.

## Setup
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d
```

Create the topic:
```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic ecommerce-events --partitions 3 --replication-factor 1
```

## Run
Terminal 1:
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 src/streaming/process_orders.py
```

Terminal 2:
```powershell
python -m src.producers.order_event_producer
```

After events have processed, inspect output:
```powershell
python -m src.streaming.read_results
```

## Output
- `data/bronze/events` raw Kafka events
- `data/silver/orders` validated and deduplicated events
- `data/gold/revenue_by_window` one-minute state revenue metrics
- `data/bad_records` rejected records and reasons
- `data/checkpoints` Structured Streaming recovery state

## Interview explanation
The producer simulates commerce events and publishes keyed messages to Kafka. Spark consumes the topic, persists immutable raw messages, parses JSON using an explicit schema, applies data-quality rules, routes invalid events, handles event-time semantics with a watermark, deduplicates by event ID, writes clean records to a partitioned Silver layer, and calculates windowed revenue metrics. Independent checkpoint directories allow each streaming sink to recover progress after restart.
