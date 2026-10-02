import psycopg
from confluent_kafka import Producer

# --- Part 1: Postgres ---
conn = psycopg.connect("host=localhost port=5432 dbname=shop user=app password=app")
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM restaurants;")
    print("Postgres OK - restaurants:", cur.fetchone()[0])
conn.close()

# --- Part 2: Kafka ---
producer = Producer({"bootstrap.servers": "localhost:9092"})
producer.produce("order_events", key="test", value="hello from python")
producer.flush()
print("Kafka OK - test message sent")
