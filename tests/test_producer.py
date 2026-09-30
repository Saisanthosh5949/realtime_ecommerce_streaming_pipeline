from src.producers.order_event_producer import build_event
def test_event():
 e=build_event(1)
 assert e["event_id"] and e["order_id"]=="ORD-000000000001" and e["event_type"]=="ORDER_EVENT"
