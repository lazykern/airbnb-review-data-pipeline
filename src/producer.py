import socket
from pathlib import Path

import polars as pl
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8099"

TOPIC = "airbnb-reviews"
VALUE_SCHEMA_SUBJECT = TOPIC + "-value"

data_path = Path("data")
reviews_df = pl.read_csv(data_path / "reviews" / "reviews.csv")

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

avro_schema = schema_registry_client.get_latest_version(VALUE_SCHEMA_SUBJECT).schema

avro_value_ser = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=avro_schema.schema_str,
    conf={"auto.register.schemas": False},
)

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": socket.gethostname(),
    "value.serializer": avro_value_ser,
}

producer = SerializingProducer(producer_conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


for row in reviews_df.iter_rows(named=True):
    producer.produce(topic=TOPIC, value=row)
    producer.flush()
