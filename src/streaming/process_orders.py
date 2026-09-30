from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session
from src.common.schemas import ORDER_EVENT_SCHEMA
from src.common.config import *
from src.quality.rules import add_quality_columns

def main():
 spark=get_spark_session("RealtimeEcommerceStreaming")
 k=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS).option("subscribe",KAFKA_TOPIC).option("startingOffsets","latest").option("failOnDataLoss","false").load())
 raw=k.select(F.col("key").cast("string").alias("kafka_key"),F.col("value").cast("string").alias("raw_json"),"topic","partition","offset",F.col("timestamp").alias("kafka_timestamp"))
 q1=(raw.writeStream.format("parquet").outputMode("append").option("path",BRONZE_PATH).option("checkpointLocation","data/checkpoints/bronze").trigger(processingTime="10 seconds").start())
 parsed=(raw.withColumn("payload",F.from_json("raw_json",ORDER_EVENT_SCHEMA)).select("kafka_key","raw_json","topic","partition","offset","kafka_timestamp","payload.*").withColumn("event_time",F.to_timestamp("event_timestamp")).withColumn("line_amount",F.round(F.col("quantity")*F.col("unit_price"),2)).withColumn("ingested_at",F.current_timestamp()))
 checked=add_quality_columns(parsed)
 bad=checked.filter(~F.col("is_valid"))
 q2=(bad.writeStream.format("parquet").outputMode("append").option("path",BAD_PATH).option("checkpointLocation","data/checkpoints/bad").trigger(processingTime="10 seconds").start())
 valid=(checked.filter("is_valid").withWatermark("event_time","2 minutes").dropDuplicates(["event_id"]).drop("is_valid","quality_reason","raw_json"))
 q3=(valid.writeStream.format("parquet").outputMode("append").option("path",SILVER_PATH).option("checkpointLocation","data/checkpoints/silver").partitionBy("state").trigger(processingTime="10 seconds").start())
 revenue=(valid.groupBy(F.window("event_time","1 minute"),"state").agg(F.count("order_id").alias("event_count"),F.round(F.sum("line_amount"),2).alias("revenue")))
 q4=(revenue.writeStream.format("parquet").outputMode("append").option("path","data/gold/revenue_by_window").option("checkpointLocation","data/checkpoints/revenue").trigger(processingTime="20 seconds").start())
 print("Streaming pipeline started. Ctrl+C to stop.")
 try:spark.streams.awaitAnyTermination()
 finally:
  for q in [q1,q2,q3,q4]:
   if q.isActive:q.stop()
  spark.stop()
if __name__=="__main__":main()
