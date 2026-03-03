import requests
import time
import json
import logging
from kafka import KafkaProducer
from datetime import datetime, timedelta
import uuid
import signal
import sys
import os
import xml.etree.ElementTree as ET

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "raw.gdelt"
REQUEST_TIMEOUT_SECONDS = int(os.getenv("GDELT_REQUEST_TIMEOUT", "20"))
POLL_INTERVAL_SECONDS = int(os.getenv("GDELT_POLL_INTERVAL", "45"))
MAX_RECORDS = int(os.getenv("GDELT_MAX_RECORDS", "12"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
session = requests.Session()
session.headers.update({"User-Agent": "nightwatch-atlas-gdelt-harvester/1.1"})

GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
CONFLICT_RSS_FEEDS = [
    "https://www.crisisgroup.org/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
]

# Real-time geopolitical conflict and military focused keywords
CONFLICT_KEYWORDS = "(war OR conflict OR ceasefire OR invasion OR missile OR drone strike OR clashes)"

# Real-time military activity keywords  
MILITARY_KEYWORDS = "(military OR army OR navy OR air force OR troops OR defense ministry OR military exercise)"

# Track last processed time to avoid duplicates
last_processed_time = datetime.utcnow() - timedelta(hours=1)
last_gdelt_ok_at = None

def signal_handler(sig, frame):
    print("\nShutting down GDELT harvester...")
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def fetch_gdelt_conflicts():
    """Fetch geopolitical conflict articles"""
    params = {
        "query": CONFLICT_KEYWORDS,
        "mode": "artlist",
        "format": "json",
        "maxrecords": MAX_RECORDS,
        "sort": "datedesc",
        "sourcelang": "eng",
        "startdatetime": last_processed_time.strftime("%Y%m%d%H%M%S")
    }

    try:
        response = session.get(GDELT_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
        
        if "articles" not in data or not data["articles"]:
            logger.warning("No conflict articles found in GDELT response")
            return None
        
        logger.info(f"Successfully fetched {len(data['articles'])} conflict articles from GDELT")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching GDELT conflict data: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing GDELT response: {e}")
        return None

def fetch_gdelt_military():
    """Fetch military activity articles"""
    params = {
        "query": MILITARY_KEYWORDS,
        "mode": "artlist",
        "format": "json",
        "maxrecords": MAX_RECORDS,
        "sort": "datedesc",
        "sourcelang": "eng",
        "startdatetime": last_processed_time.strftime("%Y%m%d%H%M%S")
    }

    try:
        response = session.get(GDELT_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
        
        if "articles" not in data or not data["articles"]:
            logger.warning("No military articles found in GDELT response")
            return None
        
        logger.info(f"Successfully fetched {len(data['articles'])} military articles from GDELT")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching GDELT military data: {e}")
        return None

def classify_rss_item(title: str, description: str) -> str:
    text = f"{title} {description}".lower()
    military_words = ["military", "army", "navy", "air force", "troops", "missile", "drone strike", "exercise"]
    conflict_words = ["war", "conflict", "invasion", "ceasefire", "clash", "fighting", "attack", "hostilities"]
    if any(word in text for word in military_words):
        return "military"
    if any(word in text for word in conflict_words):
        return "conflict"
    return "general"

def fetch_conflict_rss_fallback():
    """Fallback source when GDELT is unreachable."""
    collected = []
    for feed in CONFLICT_RSS_FEEDS:
        try:
            response = session.get(feed, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            items = root.findall(".//item")
            for item in items[:20]:
                title = (item.findtext("title") or "").strip()
                description = (item.findtext("description") or "").strip()
                if not title:
                    continue
                event_type = classify_rss_item(title, description)
                if event_type == "general":
                    continue
                url = (item.findtext("link") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()
                collected.append({
                    "id": str(uuid.uuid4()),
                    "title": title,
                    "description": description[:600],
                    "source": "ConflictRSS",
                    "url": url,
                    "timestamp": datetime.utcnow().isoformat(),
                    "gdelt_date": pub_date,
                    "gdelt_language": "eng",
                    "event_type": event_type,
                    "alert_level": "medium" if event_type == "military" else "low",
                })
        except Exception as exc:
            logger.error(f"RSS fallback error for {feed}: {exc}")
            continue
    logger.info(f"RSS fallback collected {len(collected)} conflict items")
    return collected[:35]

def convert_to_event(article, event_type="general"):
    """Convert article to event with type classification"""
    return {
        "id": str(uuid.uuid4()),
        "title": article.get("title"),
        "description": article.get("seendate", ""),
        "source": "GDELT",
        "url": article.get("url"),
        "timestamp": datetime.utcnow().isoformat(),
        "latitude": None,
        "longitude": None,
        "risk": 0.3,
        "gdelt_date": article.get("seendate", ""),
        "gdelt_language": article.get("language", "unknown"),
        "event_type": event_type,
        "alert_level": "medium" if event_type == "military" else "low"
    }

def run():
    """Main harvester loop with dual-topic processing"""
    global last_processed_time, last_gdelt_ok_at
    print("Starting GDELT real-time harvester...")
    print(f"Polling interval: {POLL_INTERVAL_SECONDS} seconds")
    print(f"Conflict Keywords: {CONFLICT_KEYWORDS[:50]}...")
    print(f"Military Keywords: {MILITARY_KEYWORDS[:50]}...")

    while True:
        try:
            print(f"Fetching GDELT data since {last_processed_time.strftime('%Y-%m-%d %H:%M:%S')}...")
            
            # Fetch both conflict and military data
            conflict_data = fetch_gdelt_conflicts()
            military_data = fetch_gdelt_military()
            
            total_processed = 0
            
            # Process conflict articles
            if conflict_data:
                conflict_articles = conflict_data.get("articles", [])
                print(f"Fetched {len(conflict_articles)} conflict articles")
                
                for article in conflict_articles:
                    event = convert_to_event(article, "conflict")
                    producer.send(TOPIC, event)
                    total_processed += 1
                    logger.info(f"Conflict: {article.get('title', 'Unknown')[:50]}...")
            
            # Process military articles
            if military_data:
                military_articles = military_data.get("articles", [])
                print(f"Fetched {len(military_articles)} military articles")
                
                for article in military_articles:
                    event = convert_to_event(article, "military")
                    producer.send(TOPIC, event)
                    total_processed += 1
                    logger.info(f"Military: {article.get('title', 'Unknown')[:50]}...")

            if total_processed > 0:
                producer.flush()
                print(f"Sent {total_processed} events to Kafka topic '{TOPIC}'")
                last_gdelt_ok_at = datetime.utcnow()
                
                # Update last processed time to newest article
                all_articles = []
                if conflict_data:
                    all_articles.extend(conflict_data.get("articles", []))
                if military_data:
                    all_articles.extend(military_data.get("articles", []))
                
                if all_articles:
                    newest_date = all_articles[0].get("seendate")
                    if newest_date:
                        try:
                            # Convert GDELT date format to datetime
                            last_processed_time = datetime.strptime(newest_date, "%Y%m%d%H%M%S")
                        except ValueError:
                            last_processed_time = datetime.utcnow()
            else:
                print("No new articles found")
                # If GDELT is producing no data for a while, use RSS fallback.
                stale_minutes = 999 if not last_gdelt_ok_at else (datetime.utcnow() - last_gdelt_ok_at).total_seconds() / 60
                if stale_minutes >= 8:
                    fallback_items = fetch_conflict_rss_fallback()
                    for item in fallback_items:
                        producer.send(TOPIC, item)
                    if fallback_items:
                        producer.flush()
                        print(f"Sent {len(fallback_items)} fallback conflict items to Kafka topic '{TOPIC}'")

        except Exception as e:
            print(f"Error in harvester loop: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    run()
