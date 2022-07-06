import faust
import random
import json
from datetime import datetime, timezone


class RawModel(faust.Record):
    value: float

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

channel = app.channel()

@app.timer(5.0, on_leader=True)
async def publish_every_5secs():
    msg = RawModel(
        value=random.random()
    )
    print("sending message: {}".format(msg))
    await channel.put(value=msg)

@app.agent()
async def read_from_channel(stream):
    async for event in channel:
        print("read message value: {}".format(event))
