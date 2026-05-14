from fastapi import FastAPI
from typing import List
from models import Notification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer = AIOKafkaConsumer(
        "order-confirmed",
        "product_not_found_events",
        "out_of_stock_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    task = asyncio.create_task(consume(consumer))
    
    yield
    
    task.cancel()
    await consumer.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []

async def consume(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            topic = msg.topic

            if topic == "order-confirmed":
                notification = Notification(
                    order_id=data['order_id'],
                    product_id=data['product_id'],
                    message=f"Narudžbina {data['order_id']} za proizvod {data['product_id']} je uspešno potvrđena."
                )

            elif topic == "product_not_found_events":
                notification = Notification(
                    order_id=data['order_id'],
                    product_id=data['product_id'],
                    timestamp=data.get('timestamp'),
                    error_reason=data.get('error_reason'),
                    message=(
                        f"Narudžbina {data['order_id']} je odbijena. "
                        f"Razlog: {data.get('error_reason')}. "
                        f"ID proizvoda: {data['product_id']}."
                    )
                )

            elif topic == "out_of_stock_events":
                notification = Notification(
                    order_id=data['order_id'],
                    product_id=data['product_id'],
                    timestamp=data.get('timestamp'),
                    error_reason=data.get('error_reason'),
                    message=(
                        f"Narudžbina {data['order_id']} je odbijena. "
                        f"Razlog: {data.get('error_reason')}. "
                        f"ID proizvoda: {data['product_id']}."
                    )
                )

            else:
                continue

            notifications_db.append(notification)
            print(f"[NOTIFIKACIJA] {notification.message}")

    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db