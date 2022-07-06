import faust
import asyncio
import random
from time import time
from datetime import datetime, timedelta, timezone

# from runner import window_processor


APP = 'orders_aggregation'
KAFKA = 'kafka://localhost:9092'
STORE = 'rocksdb://'
PARTITIONS = 8
CLEANUP_INTERVAL = 1.0
WINDOW = 30
WINDOW_EXPIRES = 60

app = faust.App(
    APP,
    broker=KAFKA,
    store=STORE,
    topic_partitions=PARTITIONS,
)
app.conf.table_cleanup_interval = CLEANUP_INTERVAL

class Order(faust.Record):
    id: int
    user_id: str
    country_origin: str
    price: float
    date_created: datetime

class Alert(faust.Record):
    user_id: str
    date_created: datetime
    mean: float


orders_topic = app.topic('orders', partitions=PARTITIONS, value_type=Order)
alert_topic = app.topic('orders_alert', partitions=PARTITIONS, value_type=Alert)

def window_processor(key, value):
    user_id = key[0]
    timestamp = key[1][-1]
    if value > 4.99:
        alert_topic.send_soon(value=Alert(date_created=timestamp, user_id=user_id, mean=value))

user_to_average_price = app.Table(
    'user_to_average_price',
    default=float,
    on_window_close=window_processor
).tumbling(10, expires=10)


@app.task
async def create_topics(app):
    await alert_topic.declare()


@app.agent(orders_topic)
async def get_user_to_average(stream):
    async for order in stream.group_by(Order.user_id):
        if order.price > 0:
            user_to_average_price[order.user_id] += (1 / order.price)
