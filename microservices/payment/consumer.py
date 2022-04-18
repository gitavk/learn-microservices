import logging
import time
from logging import StreamHandler, getLogger

from main import Order, redis_db
from redis.exceptions import ResponseError

logger = getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(StreamHandler())

KEY = "order_refund"
GROUP = "payment-group"

try:
    redis_db.xgroup_create(KEY, GROUP, mkstream=True)
except ResponseError as err:
    logger.warning(f"Error on create the group: {err}")


while True:
    results = redis_db.xreadgroup(GROUP, KEY, {KEY: ">"}, None)
    for data in results:
        order_bk = data[1][0][1]
        order: Order = Order.get(order_bk["pk"])
        if order:
            order.status = "refund"
            order.save()
            logger.info(f"Order refund: {order}")
        else:
            logger.warning(f"Order not found: {order}")

    time.sleep(1)
