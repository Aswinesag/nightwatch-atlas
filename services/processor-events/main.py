import uuid
from packages.messaging.kafka import KafkaClient

consumer = KafkaClient().consumer("entities.extracted")
producer = KafkaClient().producer()

for msg in consumer:
    article = msg.value

    event = {
        "id": str(uuid.uuid4()),
        "title": article["title"],
        "description": article["content"],
        "entities": article["entities"],
        "risk_score": 0.5
    }

    producer.send("events.detected", event)