from datetime import datetime, timedelta
from time import time
import random
import io
import json
import faust
import fastavro
import logging
from typing import Final
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers.faust import FaustSerializer
# from schema_registry.serializers.faust import AvroMessageSerializer

L: Final = logging.getLogger(__name__)


SCHEMA_REGISTRY_URL = "http://schemaregistry:8081"



# create an instance of the `SchemaRegistryClient`
client = SchemaRegistryClient(url=SCHEMA_REGISTRY_URL)
a = FaustSerializer(client)
# a = AvroMessageSerializer(client)

# avro_deployment_model_schema = client.get_schema('test-deployment')
# avro_deployment_serializer = FaustSerializer(client, "deployment_messages", avro_deployment_model_schema)


# class AvroDeployment(faust.Record, serializer=avro_deployment_serializer):
#     image: str
#     replicas: int
#     port: int = None


# schema that we want to use
# avro_sensor_message_schema = client.get_schema('sensor.messages.avro-value')
# avro_user_serializer = FaustSerializer(client, "sensor_messages", avro_sensor_message_schema)


	
	
# class AvroSchemaDecoder(faust.Schema):

#     def __fast_avro_decode(self, schema, encoded_message):
#         stringio = io.BytesIO(encoded_message)
#         return fastavro.schemaless_reader(stringio, schema)

#     def loads_value(self, app, message, *, loads=None, serializer=None):
#         L.info("message headers: ", message.headers)
#         headers = dict(message.headers)
#         avro_schema = fastavro.parse_schema(json.loads(headers["avro.schema"]))
#         return self.__fast_avro_decode(avro_schema, message.value)

#     def loads_key(self, app, message, *, loads=None, serializer=None):
#         # return json.loads(message.key)
#         return message.key
        
        



# class SensorMessageRaw(faust.Record):
#     date: datetime
#     value: float


# class AggModel(faust.Record):
#     date: datetime
#     count: int
#     mean: float


TOPIC = 'sensor.messages.avro'
# SINK = 'agg-event'
# TABLE = 'tumbling_table'
KAFKA = 'kafka://kafka:9092'
# CLEANUP_INTERVAL = 1.0
# WINDOW = 10
# WINDOW_EXPIRES = 1
PARTITIONS = 1

app = faust.App('windowed-agg', broker=KAFKA, version=1, topic_partitions=PARTITIONS)

# app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic(
    TOPIC,
    value_serializer='raw',
    # schema=AvroSchemaDecoder(),
)

@app.agent(source)
async def processor(stream):
    async for payload in stream:
        data = a.decode_message(payload)
        print(data)

# sink = app.topic(SINK, value_type=AggModel)


# @app.task
# async def create_topics(app):
#     await source.declare()
#     await sink.declare()

# def window_processor(key, events):
#     timestamp = key[1][0]
#     values = [event.value for event in events]
#     count = len(values)
#     mean = sum(values) / count

#     print(
#         f'processing window:'
#         f'{len(values)} events,'
#         f'mean: {mean:.2f},'
#         f'timestamp {timestamp}',
#     )

#     sink.send_soon(value=AggModel(date=timestamp, count=count, mean=mean))


# tumbling_table = (
#     app.Table(
#         TABLE,
#         default=list,
#         partitions=PARTITIONS,
#         on_window_close=window_processor,
#     )
#     .tumbling(WINDOW, expires=timedelta(seconds=WINDOW_EXPIRES))
#     .relative_to_field(SensorMessageRaw.date)
# )


# @app.agent(source)
# async def print_windowed_events(stream):
#     async for event in stream:
#         value_list = tumbling_table['events'].value()
#         value_list.append(event)
#         tumbling_table['events'] = value_list

# @app.timer(5.0, on_leader=True)
# async def publish_deployments():
#     deployment = {"image": "foo", "replicas": int(random.random() * 5)}
#     await source.send(value=deployment, value_serializer=avro_deployment_serializer)


# if __name__ == '__main__':
#     app.main()

