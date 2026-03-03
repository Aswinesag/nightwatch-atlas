from packages.messaging.kafka import KafkaClient

consumer = KafkaClient().consumer("raw.news")
producer = KafkaClient().producer()

for msg in consumer:
    article = msg.value

    article["clean_text"] = article["content"].lower()

    producer.send("processed.news", article)