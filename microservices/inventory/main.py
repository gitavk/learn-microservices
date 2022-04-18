from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis_om import HashModel, get_redis_connection

app = FastAPI()
app.add_middleware(CORSMiddleware)

redis_db = get_redis_connection()


class Product(HashModel):
    title: str
    price: int
    quantity: int

    class Meta:
        database = redis_db

    @classmethod
    def get(cls, pk: Any) -> "Product":
        return cls(**super().get(pk).dict())


@app.get("/")
async def all():
    return [Product.get(pk) for pk in Product.all_pks()]


@app.post("/")
async def create(product: Product):
    return product.save()


@app.get("/{pk}")
async def get(pk: str):
    return Product.get(pk)


@app.delete("/{pk}")
async def delete(pk: str):
    return Product.delete(pk)
