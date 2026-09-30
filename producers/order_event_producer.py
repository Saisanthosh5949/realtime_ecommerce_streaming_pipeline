import json,random,time,uuid
from datetime import datetime,timezone
from kafka import KafkaProducer
from src.common.config import KAFKA_BOOTSTRAP_SERVERS,KAFKA_TOPIC
STATES=["TX","CA","CO","NY","FL","WA","IL","AZ"]
STATUSES=["CREATED","PAID","SHIPPED","DELIVERED","CANCELLED"]
PAYMENTS=["CARD","PAYPAL","APPLE_PAY","GIFT_CARD"]
def build_event(n):
    e={"event_id":str(uuid.uuid4()),"event_type":"ORDER_EVENT","event_timestamp":datetime.now(timezone.utc).isoformat(),"order_id":f"ORD-{n:012d}","customer_id":f"CUST-{random.randint(1,100000):08d}","product_id":f"PROD-{random.randint(1,20000):07d}","quantity":random.randint(1,5),"unit_price":round(random.uniform(5,500),2),"order_status":random.choice(STATUSES),"payment_method":random.choice(PAYMENTS),"state":random.choice(STATES)}
    x=random.random()
    if x<.01:e["customer_id"]=None
    elif x<.02:e["quantity"]=-1
    elif x<.03:e["unit_price"]=-99.0
    return e
def main():
    p=KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,value_serializer=lambda v:json.dumps(v).encode(),key_serializer=lambda v:v.encode(),acks="all",retries=5)
    print(f"Producing to {KAFKA_TOPIC}. Ctrl+C to stop.")
    n=1
    try:
      while True:
        e=build_event(n);p.send(KAFKA_TOPIC,key=e["order_id"],value=e)
        if n%100==0:p.flush();print(f"Produced {n:,} events")
        n+=1;time.sleep(.05)
    except KeyboardInterrupt: pass
    finally:p.flush();p.close()
if __name__=="__main__":main()
