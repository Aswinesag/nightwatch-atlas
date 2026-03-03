import time
import uuid
import signal
import sys
from packages.messaging.kafka import get_producer

producer = get_producer()

def fetch_news():
    return [
        {
            "title": "Naval fleet deployed",
            "content": "Fleet deployed in disputed waters",
            "source": "news"
        }
    ]

def signal_handler(sig, frame):
    print("\nShutting down news harvester...")
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

print("News harvester started")

try:
    while True:
        news = fetch_news()

        for article in news:
            article["id"] = str(uuid.uuid4())
            producer.send("raw.news", article)
            print("Sent:", article["title"])

        time.sleep(5)
except KeyboardInterrupt:
    print("\nShutting down news harvester...")
    producer.close()
except Exception as e:
    print(f"Error: {e}")
    producer.close()