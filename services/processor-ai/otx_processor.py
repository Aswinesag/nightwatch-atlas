"""
OTX Cyber Threat Data Processor
Processes real cyber threat data from OTX API
"""
import json
from kafka import KafkaConsumer, KafkaProducer
import uuid
import signal
import sys
import logging
import os
from typing import Dict, Optional
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    "raw.otx",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="otx-processor-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def signal_handler(sig, frame):
    logger.info("Shutting down OTX processor...")
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def calculate_cyber_risk(pulse_data: Dict) -> float:
    """Calculate risk based on cyber threat indicators"""
    try:
        title = pulse_data.get("title", "").lower()
        summary = (pulse_data.get("summary") or pulse_data.get("description") or "").lower()
        
        risk = 0.3  # Base risk
        
        # High-risk indicators
        high_risk_indicators = [
            "malware", "ransomware", "apt", "zero-day", "breach", "hack",
            "vulnerability", "exploit", "trojan", "backdoor", "phishing"
        ]
        
        medium_risk_indicators = [
            "attack", "campaign", "suspicious", "anomaly", "intrusion",
            "compromise", "incident", "threat"
        ]
        
        # Count risk indicators
        high_risk_count = sum(1 for indicator in high_risk_indicators if indicator in title or indicator in summary)
        medium_risk_count = sum(1 for indicator in medium_risk_indicators if indicator in title or indicator in summary)
        
        # Calculate risk
        if high_risk_count > 0:
            risk = 0.8 + (high_risk_count * 0.1)
        elif medium_risk_count > 0:
            risk = 0.5 + (medium_risk_count * 0.05)
        
        # Specific high-risk patterns
        if any(pattern in title or pattern in summary for pattern in [
            "critical infrastructure", "government", "military", "defense"
        ]):
            risk += 0.2
        
        return min(0.95, risk)
        
    except Exception as e:
        logger.error(f"Error calculating cyber risk: {e}")
        return 0.5

def determine_cyber_event_type(pulse_data: Dict) -> str:
    """Determine event type based on cyber threat data"""
    title = pulse_data.get("title", "").lower()
    summary = (pulse_data.get("summary") or pulse_data.get("description") or "").lower()
    
    # Check for specific cyber threat types
    if any(indicator in title or indicator in summary for indicator in [
        "apt", "advanced persistent threat", "state-sponsored"
    ]):
        return "political"
    
    if any(indicator in title or indicator in summary for indicator in [
        "malware", "ransomware", "virus", "trojan"
    ]):
        return "military"
    
    if any(indicator in title or indicator in summary for indicator in [
        "infrastructure", "ics", "scada", "industrial"
    ]):
        return "infrastructure"
    
    if any(indicator in title or indicator in summary for indicator in [
        "data breach", "leak", "exposure", "privacy"
    ]):
        return "general"
    
    return "general"

def extract_cyber_location(pulse_data: Dict) -> Optional[Dict[str, float]]:
    """Extract location information from cyber threat data"""
    try:
        # Prefer already-extracted location from upstream harvester when present.
        location_info = pulse_data.get("location_info")
        if isinstance(location_info, dict) and location_info.get("lat") is not None and location_info.get("lon") is not None:
            return {
                "lat": location_info["lat"],
                "lon": location_info["lon"],
                "display_name": location_info.get("display_name", "Unknown"),
                "country": location_info.get("country", "Unknown"),
                "confidence": location_info.get("confidence", 0.7),
            }

        title = pulse_data.get("title", "")
        summary = pulse_data.get("summary", pulse_data.get("description", ""))
        text = f"{title} {summary}".lower()
        
        # Look for country mentions
        countries = [
            "united states", "china", "russia", "north korea", "iran",
            "israel", "ukraine", "saudi arabia", "india", "pakistan",
            "united kingdom", "germany", "france", "japan", "south korea",
            "australia", "canada", "brazil", "mexico", "italy", "spain"
        ]
        
        for country in countries:
            if country in text:
                # Use approximate coordinates for major countries
                country_coords = {
                    "united states": {"lat": 39.8283, "lon": -98.5795},
                    "china": {"lat": 35.8617, "lon": 104.1954},
                    "russia": {"lat": 61.5240, "lon": 105.3188},
                    "north korea": {"lat": 40.3399, "lon": 127.5101},
                    "iran": {"lat": 32.4279, "lon": 53.6880},
                    "israel": {"lat": 31.0461, "lon": 34.8516},
                    "ukraine": {"lat": 48.3794, "lon": 31.1656},
                    "saudi arabia": {"lat": 23.8859, "lon": 45.0792},
                    "india": {"lat": 20.5937, "lon": 78.9629},
                    "pakistan": {"lat": 30.3753, "lon": 69.3451},
                    "united kingdom": {"lat": 55.3781, "lon": -3.4360},
                    "germany": {"lat": 51.1657, "lon": 10.4515},
                    "france": {"lat": 46.2276, "lon": 2.2137},
                    "japan": {"lat": 36.2048, "lon": 138.2529},
                    "south korea": {"lat": 35.9078, "lon": 127.7669},
                    "australia": {"lat": -25.2744, "lon": 133.7751},
                    "canada": {"lat": 56.1304, "lon": -106.3468},
                    "brazil": {"lat": -14.2350, "lon": -51.9253},
                    "mexico": {"lat": 23.6345, "lon": -102.5528},
                    "italy": {"lat": 41.8719, "lon": 12.5674},
                    "spanish": {"lat": 40.4637, "lon": -3.7492}
                }
                
                if country in country_coords:
                    coords = country_coords[country]
                    return {
                        "lat": coords["lat"],
                        "lon": coords["lon"],
                        "display_name": country.title(),
                        "country": country.title(),
                        "confidence": 0.6
                    }
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting cyber location: {e}")
        return None

def process_cyber_data(pulse_data: Dict) -> Dict:
    """Process cyber threat data into standardized event format"""
    try:
        location = extract_cyber_location(pulse_data)
        
        existing_lat = pulse_data.get("latitude")
        existing_lon = pulse_data.get("longitude")

        event = {
            "id": str(uuid.uuid4()),
            "title": pulse_data.get("title", "Unknown cyber threat"),
            "description": pulse_data.get("summary", pulse_data.get("description", "Cyber threat detected")),
            "source": "OTX",
            "timestamp": datetime.utcnow().isoformat(),
            "latitude": existing_lat if existing_lat is not None else (location["lat"] if location else None),
            "longitude": existing_lon if existing_lon is not None else (location["lon"] if location else None),
            "risk": calculate_cyber_risk(pulse_data),
            "type": determine_cyber_event_type(pulse_data),
            "severity": "high" if calculate_cyber_risk(pulse_data) > 0.7 else "medium",
            "location_info": location,
            "processed_at": datetime.utcnow().isoformat()
        }
        
        return event
        
    except Exception as e:
        logger.error(f"Error processing cyber data: {e}")
        return None

def main():
    """Main processor loop"""
    logger.info("OTX cyber threat processor started. Waiting for messages...")
    
    try:
        for msg in consumer:
            try:
                pulse_data = msg.value
                logger.info(f"Processing cyber threat: {pulse_data.get('title', 'Unknown')}")
                
                event = process_cyber_data(pulse_data)
                if event:
                    producer.send("events", event)
                    producer.flush()
                    logger.info(f"Processed cyber event - Type: {event['type']}, Risk: {event['risk']:.2f}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        consumer.close()
        producer.close()
        logger.info("OTX processor stopped.")

if __name__ == "__main__":
    main()
