from pyspark.sql.types import *
ORDER_EVENT_SCHEMA=StructType([
 StructField("event_id",StringType()), StructField("event_type",StringType()),
 StructField("event_timestamp",StringType()), StructField("order_id",StringType()),
 StructField("customer_id",StringType()), StructField("product_id",StringType()),
 StructField("quantity",IntegerType()), StructField("unit_price",DoubleType()),
 StructField("order_status",StringType()), StructField("payment_method",StringType()),
 StructField("state",StringType())])
