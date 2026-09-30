from pathlib import Path
from src.common.spark_session import get_spark_session
def show(s,p,t):
 print("\n"+t)
 if Path(p).exists():s.read.parquet(p).show(20,False)
 else:print("No output yet")
def main():
 s=get_spark_session("ReadResults")
 show(s,"data/silver/orders","SILVER ORDERS")
 show(s,"data/gold/revenue_by_window","GOLD REVENUE")
 show(s,"data/bad_records","BAD RECORDS")
 s.stop()
if __name__=="__main__":main()
