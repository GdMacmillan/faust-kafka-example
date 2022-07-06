import faust
import random

from datetime import datetime, timezone


APP = 'orders_aggregation'
KAFKA = 'kafka://localhost:9092'
STORE = 'rocksdb://'
PARTITIONS = 8
CLEANUP_INTERVAL = 1.0
WINDOW = 30
WINDOW_EXPIRES = 10

app = faust.App(
    APP,
    broker=KAFKA,
    store=STORE,
    topic_partitions=PARTITIONS,
)

orders_count = 0
users = ["willy", "roger", "brad", "twila", "natalie", "roger", "marissa", "meghan", "blair", "chester"]
countries = ["france", "germany", "belgium", "italy", "america"]


class Order(faust.Record):
    id: int
    user_id: str
    country_origin: str
    price: float
    date_created: datetime


orders_topic = app.topic('orders', partitions=PARTITIONS, value_type=Order)

@app.task
async def create_topics(app):
    await orders_topic.declare()

@app.timer(0.3, on_leader=True)
async def publish_every_2secs():
    # await orders_topic.declare()
    global orders_count
    orders_count += 1
    msg = Order(
        id=str(orders_count),
        user_id=users[int(random.random() * 10)],
        country_origin=countries[int(random.random() * 5)],
        price=round(random.random() * 10, 2),
        date_created=datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    )
    print("publishing message {}".format(msg))
    await orders_topic.send(value=msg)
