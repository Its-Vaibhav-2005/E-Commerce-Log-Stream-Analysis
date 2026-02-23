import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch


BootstrapServers = "localhost:29092"
TopicName = "clean_events"
GroupId = "postgres-loader"

BatchSize = 20

consumer = KafkaConsumer(
    TopicName,
    bootstrap_servers=BootstrapServers,
    group_id = GroupId,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

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

for msg in consumer:
    event = msg.value

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
            consumer.commit()
        except Exception as e:
            print(f"Error flushing buffer to DB: {e}")
            Connection.rollback()