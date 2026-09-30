import os
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC","ecommerce-events")
BRONZE_PATH="data/bronze/events"
SILVER_PATH="data/silver/orders"
BAD_PATH="data/bad_records"
