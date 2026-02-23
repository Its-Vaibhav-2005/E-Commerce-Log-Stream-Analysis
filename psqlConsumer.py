import json
from confluent_kafka import Consumer
import psycopg2
from psycopg2.extras import execute_batch


BootstrapServers = "localhost:29092"
TopicName = "clean_events"
GroupId = "postgres-loader"

BatchSize = 500

consumer = Consumer({
    'bootstrap.servers': BootstrapServers,
    'group.id': GroupId,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'fetch.min.bytes': 50000,
    'fetch.wait.max.ms': 500,
    "log_level": 3
})

consumer.subscribe([TopicName])

Connection = psycopg2.connect(
    host="localhost",
    database="ecom_kafka",
    user="postgres",
    password="1234",
    port=5432
)
Connection.autocommit = False

cursor = Connection.cursor()

print("Connected to PostgreSQL")
print("Starting Kafka -> PostgreSQL Loader...")

buffer = []

InsertQuery = """
INSERT INTO kafka_events_silver
(event_id, customer_id, event_type, amount, currency, event_timestamp)
VALUES (%s, %s, %s, %s, %s, %s)
"""

def FlushToDB(records):
    execute_batch(cursor, InsertQuery, records)
    Connection.commit()
    print(f"Inserted batch of {len(records)} records to PostgreSQL . . .")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode('utf-8'))

        buffer.append((
            event['event_id'],
            event['customer_id'],
            event['event_type'],
            event['amount'],
            event['currency'],
            event['timestamp']
        ))

        if len(buffer) >= BatchSize:
            try:
                FlushToDB(buffer)
                buffer.clear()

                # commit Kafka offsets only after DB success
                consumer.commit()

            except Exception as e:
                print(f"Error flushing buffer to DB: {e}")
                Connection.rollback()
        elif len(buffer)%100 == 0:
            print(f"Buffer size: {len(buffer)} records. Waiting to flush...")
        

except KeyboardInterrupt:
    print("\nStopping loader...")

finally:
    if buffer:
        try:
            FlushToDB(buffer)
            consumer.commit()
        except Exception as e:
            print(f"Final flush failed: {e}")
            Connection.rollback()

    consumer.close()
    cursor.close()
    Connection.close()