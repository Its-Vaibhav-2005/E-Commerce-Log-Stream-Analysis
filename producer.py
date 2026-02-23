import json
import random
import time
import uuid 
import datetime

from confluent_kafka import Producer

BootstrapServers = 'localhost:29092'
TopicName = 'raw_events'


producer = Producer({
    'bootstrap.servers': BootstrapServers
})

EVENT_TYPES = ['PAGE_VIEW', "ADD_TO_CART", "PURCHASE"]
INVALID_EVENTS = ["CLICK", "VIEW", "PAY"]

def randomTimeStamp():
    now = datetime.datetime.now(datetime.timezone.utc)
    past = now -  datetime.timedelta(days=14)

    randomSec = random.uniform(0, (now - past).total_seconds())

    return past + datetime.timedelta(seconds=randomSec)

def deliveryReport(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def generateEvent():
    isInvalid  = random.random() < 0.25

    customerId = f"CUSTOMER_{random.randint(2705,3105)}"
    eventType = random.choice(EVENT_TYPES)

    amount = round(random.uniform(200, 5000), 2)
    currency = "IND"

    invaild_feild = None

    if isInvalid:
        invaild_feild = random.choice([
            "customer_id",
            "event_type",
            "amount",
            "currency"
        ])
    
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": None if invaild_feild == "customer_id" else customerId,
        "event_type": (
            random.choice(INVALID_EVENTS)
            if invaild_feild == "event_type" else eventType
        ),
        "amount": (
            random.uniform(-5000, -100)
            if invaild_feild == "amount" else amount
        ),
        "currency": None if invaild_feild == "currency" else currency,
        "timestamp": randomTimeStamp().isoformat(),
        "is_valid": not isInvalid,
        "invalid_field": invaild_feild
    }

    return event['customer_id'], event


print("Starting Kafka Producer . . .")

try:
    while True:
        key, value = generateEvent()
        producer.produce(
            topic=TopicName,
            key=key.encode('utf-8') if key else None,
            value=json.dumps(value).encode('utf-8'),
            callback=deliveryReport
        )
        producer.poll(0)
        print(f"Produced event | Key: {key:} | Is Valid : {value['is_valid']} | Timestamp: {value['timestamp']}")
        time.sleep(.5)
except KeyboardInterrupt:
    print("\nStopping producer . . .")
finally:
    producer.flush()