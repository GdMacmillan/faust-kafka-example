# Toy Example

This example is used to prove that the table aggregation and partitioning of data work against a stream of incoming data

The orders are partitioned on user_id and then a tumbling window is used to provide a rolling average of the order price amount for that user.

To run, ensure that rocksdb shared libraries are installed on your computer. You can then pip install `faust-streaming[rocksdb]`

## Start a confluent kafka cluster using the quick start documentation here:

https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#

## Running publisher to publish a random assortment of orders

`faust -A publisher worker -p 6066 -l info`

## Running consumer to consume order messages as source and produce an output alerts topic

`faust -A consumer worker -p 6067 -l info`

Note the different port ensures the 2 apps can run at the same time. `ctrl-c` to quit either one of them.

## consuming from the alerts topic

Open a shell in the broker container on whatever platform the kafka cluster is running. Then, execute the following command to subscribe to the alerts topic:

`/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic orders_alert --property print.key=True --from-beginning`

The results should look roughly like the following:

```
[appuser@broker ~]$ /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic orders_alert --property print.key=True --from-beginning
null	{"user_id":"jared","date_created":1656085919.9,"mean":111.10224577869114,"__faust":{"ns":"runner1.Alert"}}
null	{"user_id":"nikki","date_created":1656085919.9,"mean":5.276776002236749,"__faust":{"ns":"runner1.Alert"}}
null	{"user_id":"john","date_created":1656085919.9,"mean":5.283349297136588,"__faust":{"ns":"runner1.Alert"}}
null	{"user_id":"gordon","date_created":1656085929.9,"mean":15.992839626835238,"__faust":{"ns":"runner1.Alert"}}
null	{"user_id":"thomas","date_created":1656085939.9,"mean":14.484465538078016,"__faust":{"ns":"runner1.Alert"}}
null	{"user_id":"bill","date_created":1656085939.9,"mean":14.417466591379634,"__faust":{"ns":"runner1.Alert"}}
```
