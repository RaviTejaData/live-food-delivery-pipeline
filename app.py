import os
import asyncio
import json
import threading
from contextlib import asynccontextmanager

import psycopg
from confluent_kafka import Consumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

DB_HOST = os.getenv("DB_HOST", "localhost")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DB_DSN = f"host={DB_HOST} port=5432 dbname=shop user=app password=app"

TOPIC = "order_events"

# ---------- Connected browsers ----------
clients: set[WebSocket] = set()


async def broadcast(message: str):
    dead = []
    for ws in list(clients):
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


# ---------- Kafka consumer (runs in a background thread) ----------
def kafka_loop(loop, stop_event):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "dashboard-api",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC])
    print("Kafka consumer started")
    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Kafka error:", msg.error())
                continue

            value = msg.value().decode("utf-8")
            try:
                json.loads(value)
            except json.JSONDecodeError:
                print("Skipping non-JSON message:", value)
                continue

            asyncio.run_coroutine_threadsafe(broadcast(value), loop)
    finally:
        consumer.close()
        print("Kafka consumer stopped")


# ---------- Start/stop the consumer with the app ----------
@asynccontextmanager
async def lifespan(app):
    loop = asyncio.get_running_loop()
    stop_event = threading.Event()
    thread = threading.Thread(target=kafka_loop, args=(loop, stop_event), daemon=True)
    thread.start()
    yield
    stop_event.set()
    thread.join(timeout=5)

app = FastAPI(title="Live Food Delivery API", lifespan=lifespan)


# ---------- Health check ----------

# ---------- Dashboard page ----------
# ---------- Health check ----------
@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/")
def dashboard():
    return FileResponse("dashboard.html")

def health():
    return {"status": "ok"}


# ---------- Snapshot: the current state of the business ----------
@app.get("/api/snapshot")
def snapshot():
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        # 1. How many orders are in each status
        cur.execute("SELECT status, count(*) FROM orders GROUP BY status")
        status_counts = {status: count for status, count in cur.fetchall()}

        # 2. Today's totals
        cur.execute(
            """SELECT count(*), COALESCE(sum(total_amount), 0)
               FROM orders
               WHERE created_at >= date_trunc('day', now())
                 AND status <> 'cancelled'"""
        )
        orders_today, revenue_today = cur.fetchone()

        # 3. The 20 most recently updated orders
        cur.execute(
            """SELECT o.order_id, r.name, r.city, o.status, o.total_amount, o.updated_at
               FROM orders o
               JOIN restaurants r USING (restaurant_id)
               ORDER BY o.updated_at DESC
               LIMIT 20"""
        )
        recent = [
            {
                "order_id": order_id,
                "restaurant": restaurant,
                "city": city,
                "status": status,
                "total_amount": float(total),
                "updated_at": updated_at.isoformat(),
            }
            for order_id, restaurant, city, status, total, updated_at in cur.fetchall()
        ]

    return {
        "status_counts": status_counts,
        "orders_today": orders_today,
        "revenue_today": float(revenue_today),
        "recent_orders": recent,
    }

# ---------- WebSocket: live event stream ----------
@app.websocket("/ws/orders")
async def ws_orders(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    print(f"Browser connected ({len(clients)} total)")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.discard(websocket)
        print(f"Browser disconnected ({len(clients)} total)")



