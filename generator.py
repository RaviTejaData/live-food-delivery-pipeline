import json
import random
import time

import psycopg
from confluent_kafka import Producer

# ---------- Section 1: settings ----------
DB_DSN = "host=localhost port=5432 dbname=shop user=app password=app"
TOPIC = "order_events"

MENU = {
    "Indian":   [("Chicken Tikka Masala", 12.50), ("Garlic Naan", 3.00), ("Lamb Biryani", 13.95)],
    "Italian":  [("Margherita Pizza", 10.00), ("Spaghetti Carbonara", 11.50), ("Tiramisu", 5.50)],
    "Chinese":  [("Sweet & Sour Chicken", 9.80), ("Egg Fried Rice", 4.50), ("Spring Rolls", 4.20)],
    "American": [("Cheeseburger", 9.50), ("Loaded Fries", 4.95), ("Milkshake", 4.00)],
    "Japanese": [("Salmon Nigiri", 7.50), ("Chicken Katsu Curry", 12.00), ("Miso Soup", 3.20)],
    "Mexican":  [("Beef Burrito", 9.95), ("Chicken Tacos", 8.50), ("Nachos", 6.00)],
    "Vegan":    [("Buddha Bowl", 10.50), ("Falafel Wrap", 8.00), ("Green Smoothie", 4.50)],
    "Turkish":  [("Chicken Shish", 10.95), ("Lamb Doner", 9.50), ("Baklava", 4.00)],
}

NEXT_STATUS = {"placed": "cooking", "cooking": "on_the_way", "on_the_way": "delivered"}
CANCEL_CHANCE = 0.05

# ---------- Section 2: connections ----------
conn = psycopg.connect(DB_DSN, autocommit=True)
producer = Producer({"bootstrap.servers": "localhost:9092"})


# ---------- Section 3: load reference data ----------
def load_reference_data():
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customers")
        customers = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT restaurant_id, name, cuisine, city FROM restaurants")
        restaurants = cur.fetchall()
        cur.execute("SELECT rider_id FROM riders")
        riders = [row[0] for row in cur.fetchall()]
    return customers, restaurants, riders


# ---------- Section 4: send an event to Kafka ----------
def send_event(event):
    producer.produce(TOPIC, key=str(event["order_id"]), value=json.dumps(event))
    producer.poll(0)


# ---------- Section 5: create one order ----------
def create_order(customers, restaurants):
    customer_id = random.choice(customers)
    restaurant_id, restaurant_name, cuisine, city = random.choice(restaurants)

    dishes = random.sample(MENU[cuisine], k=random.randint(1, 3))
    lines = [(name, random.randint(1, 3), price) for name, price in dishes]
    total = round(sum(qty * price for _, qty, price in lines), 2)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO orders (customer_id, restaurant_id, total_amount)
                   VALUES (%s, %s, %s)
                   RETURNING order_id, created_at""",
                (customer_id, restaurant_id, total),
            )
            order_id, created_at = cur.fetchone()

            cur.executemany(
                """INSERT INTO order_items (order_id, item_name, quantity, unit_price)
                   VALUES (%s, %s, %s, %s)""",
                [(order_id, name, qty, price) for name, qty, price in lines],
            )

    send_event({
        "event_type": "order_created",
        "order_id": order_id,
        "status": "placed",
        "restaurant": restaurant_name,
        "cuisine": cuisine,
        "city": city,
        "total_amount": total,
        "item_count": sum(qty for _, qty, _ in lines),
        "event_time": created_at.isoformat(),
    })
    print(f"NEW  order {order_id:>5} | {restaurant_name:<13} | {city:<10} | £{total:.2f}")

# ---------- Section 5b: move existing orders forward ----------
def advance_orders(riders):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT o.order_id, o.status, r.name, r.city
               FROM orders o
               JOIN restaurants r USING (restaurant_id)
               WHERE (o.status = 'placed'     AND o.updated_at < now() - interval '5 seconds')
                  OR (o.status = 'cooking'    AND o.updated_at < now() - interval '10 seconds')
                  OR (o.status = 'on_the_way' AND o.updated_at < now() - interval '10 seconds')
               ORDER BY o.updated_at
               LIMIT 20"""
        )
        candidates = cur.fetchall()

        for order_id, status, restaurant_name, city in candidates:
            if random.random() > 0.5:
                continue

            if status == "placed" and random.random() < CANCEL_CHANCE:
                new_status = "cancelled"
            else:
                new_status = NEXT_STATUS[status]

            rider_id = random.choice(riders) if new_status == "on_the_way" else None

            cur.execute(
                """UPDATE orders
                   SET status = %s,
                       rider_id = COALESCE(%s, rider_id),
                       updated_at = now()
                   WHERE order_id = %s
                   RETURNING updated_at, rider_id""",
                (new_status, rider_id, order_id),
            )
            updated_at, current_rider = cur.fetchone()

            send_event({
                "event_type": "status_changed",
                "order_id": order_id,
                "previous_status": status,
                "status": new_status,
                "restaurant": restaurant_name,
                "city": city,
                "rider_id": current_rider,
                "event_time": updated_at.isoformat(),
            })
            print(f"     order {order_id:>5} | {status:>10} → {new_status}")

# ---------- Section 6: main loop ----------
if __name__ == "__main__":
    customers, restaurants, riders = load_reference_data()
    print(f"Loaded {len(customers)} customers, {len(restaurants)} restaurants, {len(riders)} riders. Ctrl+C to stop.\n")
    try:
        while True:
            create_order(customers, restaurants)
            advance_orders(riders)
            time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.flush()
        conn.close()
