import sys
import os
import signal
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'packages'))
import messaging.kafka
from messaging.kafka import get_consumer, get_producer
import random

def signal_handler(sig, frame):
    print("\nShutting down processor...")
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def random_location():
    return {
        "lat": random.uniform(-60, 70),
        "lon": random.uniform(-180, 180)
    }

consumer = get_consumer("raw.news")
producer = get_producer()

print("Processor started")

def classify_event(text):
    t = text.lower()

    if any(k in t for k in ["fleet", "naval", "missile", "troop"]):
        return "military"

    if any(k in t for k in ["election", "government", "policy"]):
        return "political"

    if any(k in t for k in ["pipeline", "power", "grid", "port"]):
        return "infrastructure"

    return "general"

def risk_score(text):
    if "fleet" in text.lower():
        return 0.8
    return 0.2

def get_severity(event_type):
    if event_type == "military":
        return "high"
    elif event_type == "political":
        return "medium"
    elif event_type == "infrastructure":
        return "low"
    else:
        return "unknown"

try:
    for msg in consumer:
        article = msg.value
        loc = random_location()
        event_type = classify_event(article["content"])
        event = {
            "id": article["id"],
            "title": article["title"],
            "description": article["content"],
            "risk_score": risk_score(article["content"]),
            "type": event_type,
            "severity": get_severity(event_type),
            "lat": loc["lat"],
            "lon": loc["lon"]
        }

        producer.send("events", event)
        print("Processed:", event["title"])
except KeyboardInterrupt:
    print("\nShutting down processor...")
    consumer.close()
    producer.close()
except Exception as e:
    print(f"Error: {e}")
    consumer.close()
    producer.close()