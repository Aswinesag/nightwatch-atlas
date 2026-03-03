from packages.messaging.kafka import KafkaClient

consumer = KafkaClient().consumer("processed.news")
producer = KafkaClient().producer()

def extract_entities(text):
    return [
        {"name": "Fleet", "type": "MILITARY", "confidence": 0.9}
    ]

for msg in consumer:
    article = msg.value

    article["entities"] = extract_entities(article["clean_text"])

    producer.send("entities.extracted", article)