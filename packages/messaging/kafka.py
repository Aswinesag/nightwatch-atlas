from kafka import KafkaProducer, KafkaConsumer
import json
import os

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "intel-group")

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def get_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id=GROUP_ID
    )


class KafkaClient:
    def producer(self):
        return get_producer()

    def consumer(self, topic):
        return get_consumer(topic)
