import json
from kafka import KafkaConsumer, KafkaProducer


BootstrapServers = 'localhost:29092'
RAW_TOPIC = 'raw_events'
PRODUCED_TOPIC = 'clean_events'
GROUP_ID = 'silver-stream-processor'

VALID_EVENT_TYPES = ['PAGE_VIEW', "ADD_TO_CART", "PURCHASE"]

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=BootstrapServers,
    group_id = GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=BootstrapServers,
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: json.dumps(v).encode('utf-8') if v else None
)


def isValidEvent(event):
    if not event.get("customer_id"):
        return False
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False
    if not event.get("amount") or event.get("amount") <= 0:
        return False
    if not event.get("currency"):
        return False
    if event.get("is_valid") is not True:
        return False
    return True

for msg in consumer:
    key = msg.key
    event = msg.value

    if isValidEvent(event):
        producer.send(
            topic = PRODUCED_TOPIC,
            key=key,
            value=event
        )
        print(f"Forwarded valid event   | Timestamp: {event['timestamp']}   |    Key: {key} ")
    else:
        print(f"Discarded invalid event | Timestamp: {event['timestamp']}   |    Key: {key} ")