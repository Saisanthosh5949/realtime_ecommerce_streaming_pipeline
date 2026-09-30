from src.common.spark_session import get_spark_session
from src.quality.rules import add_quality_columns
def test_negative_quantity_invalid():
 s=get_spark_session("test")
 df=s.createDataFrame([("e1","o1","c1","p1",-1,10.0,"PAID","2026-01-01 00:00:00")],["event_id","order_id","customer_id","product_id","quantity","unit_price","order_status","event_time"])
 row=add_quality_columns(df).first()
 assert row.is_valid is False and row.quality_reason=="INVALID_QUANTITY"
 s.stop()
