from pyspark.sql import functions as F
VALID=["CREATED","PAID","SHIPPED","DELIVERED","CANCELLED"]
def add_quality_columns(df):
 return (df.withColumn("quality_reason",
  F.when(F.col("event_id").isNull(),"MISSING_EVENT_ID")
   .when(F.col("order_id").isNull(),"MISSING_ORDER_ID")
   .when(F.col("customer_id").isNull(),"MISSING_CUSTOMER_ID")
   .when(F.col("product_id").isNull(),"MISSING_PRODUCT_ID")
   .when(F.col("quantity")<=0,"INVALID_QUANTITY")
   .when(F.col("unit_price")<=0,"INVALID_PRICE")
   .when(~F.col("order_status").isin(VALID),"INVALID_STATUS")
   .when(F.col("event_time").isNull(),"INVALID_TIMESTAMP"))
  .withColumn("is_valid",F.col("quality_reason").isNull()))
