import logging
import time
from logging import StreamHandler, getLogger

from main import Product, redis_db
from redis.exceptions import ResponseError

logger = getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(StreamHandler())

KEY = "order_complete"
GROUP = "inventory-group"

try:
    redis_db.xgroup_create(KEY, GROUP, mkstream=True)
except ResponseError as err:
    logger.warning(f"Error on create the group: {err}")


while True:
    results = redis_db.xreadgroup(GROUP, KEY, {KEY: ">"}, None)
    for data in results:
        order = data[1][0][1]
        amount: int = int(order["quantity"])
        product = Product.get(order["product"])
        if product and product.quantity - amount > 0:
            product.quantity = product.quantity - amount
            product.save()
            logger.info(f"Order approved: {product}")
        else:
            redis_db.xadd("order_refund", order, "*")
            logger.warning(f"Order refund: {order}")

    time.sleep(1)
