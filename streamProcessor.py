import json
from confluent_kafka import Consumer, Producer


BootstrapServers = 'localhost:29092'
RAW_TOPIC = 'raw_events'
PRODUCED_TOPIC = 'clean_events'
GROUP_ID = 'silver-stream-processor'

VALID_EVENT_TYPES = ['PAGE_VIEW', "ADD_TO_CART", "PURCHASE"]

consumer = Consumer({
    'bootstrap.servers': BootstrapServers,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
})
consumer.subscribe([RAW_TOPIC])

producer = Producer({
    'bootstrap.servers': BootstrapServers
})


def deliveryReport(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

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


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        key = msg.key().decode('utf-8') if msg.key() else None
        event = json.loads(msg.value().decode('utf-8'))

        if isValidEvent(event):
            producer.produce(
                topic=PRODUCED_TOPIC,
                key=key.encode('utf-8') if key else None,
                value=json.dumps(event).encode('utf-8'),
                callback=deliveryReport
            )

            print(f"Forwarded valid event   | Timestamp: {event['timestamp']} | Key: {key}")

        else:
            print(f"Discarded invalid event | Timestamp: {event['timestamp']} | Key: {key}")

        producer.poll(0)
except KeyboardInterrupt:
    print("Shutting down stream processor...")
finally:
    consumer.close()
    producer.flush()