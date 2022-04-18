import logging
import time
from logging import StreamHandler, getLogger
from typing import Any

import requests
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis_om import HashModel, get_redis_connection
from starlette.requests import Request

app = FastAPI()
app.add_middleware(CORSMiddleware)

redis_db = get_redis_connection()

logger = getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(StreamHandler())


class Order(HashModel):
    product: str
    price: int
    fee: int
    quantity: int
    status: str

    class Meta:
        database = redis_db

    @classmethod
    def get(cls, pk: Any) -> "Order":
        return cls(**super().get(pk).dict())


@app.post("/")
async def create(requset: Request, bg_task: BackgroundTasks):
    body = await requset.json()
    product = requests.get(f"http://localhost:8000/{body['id']}").json()
    price = product["price"]
    fee = 0.2 * product["price"]
    order = Order(
        **{
            "product": product["pk"],
            "price": price,
            "fee": fee,
            "quantity": body["quantity"],
            "status": "pending",
        }
    )
    order.save()
    logger.info(f"Add the order {order.pk}")
    bg_task.add_task(complete, order)
    return order


def complete(order: Order):
    logger.info(f"Set status complete for the order: {order.pk}")
    time.sleep(5)
    order.status = "complete"
    order.save()
    logger.info(f"The order : {order.pk} completed")
    redis_db.xadd("order_complete", order.dict(), "*")


@app.get("/")
async def all():
    return [Order.get(pk) for pk in Order.all_pks()]


@app.get("/{pk}")
async def get(pk: str):
    return Order.get(pk)
