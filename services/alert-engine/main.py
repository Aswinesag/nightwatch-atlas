from packages.messaging.kafka import KafkaClient

consumer = KafkaClient().consumer("threat.analysis")
producer = KafkaClient().producer()

for msg in consumer:
    event = msg.value

    if event["risk_score"] > 0.6:
        producer.send("alerts.generated", event)