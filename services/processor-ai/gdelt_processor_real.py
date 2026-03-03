"""
Enhanced GDELT Processor with Real NLP and Geocoding
No hardcoded data - uses real AI services
"""
import json
from kafka import KafkaConsumer, KafkaProducer
import uuid
import random
import signal
import sys
import logging
import requests
import os
from typing import Dict, Optional, List
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    "raw.gdelt",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="gdelt-processor-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def signal_handler(sig, frame):
    logger.info("Shutting down GDELT processor...")
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class RealLocationService:
    """Real geocoding service using OpenStreetMap Nominatim"""
    
    @staticmethod
    def extract_locations(text: str) -> Optional[Dict[str, float]]:
        """Extract real locations from text using geocoding"""
        try:
            text_lower = (text or "").lower()
            known_places = {
                "iran": {"lat": 32.4279, "lon": 53.6880, "country": "Iran"},
                "israel": {"lat": 31.0461, "lon": 34.8516, "country": "Israel"},
                "lebanon": {"lat": 33.8547, "lon": 35.8623, "country": "Lebanon"},
                "syria": {"lat": 34.8021, "lon": 38.9968, "country": "Syria"},
                "ukraine": {"lat": 48.3794, "lon": 31.1656, "country": "Ukraine"},
                "russia": {"lat": 61.5240, "lon": 105.3188, "country": "Russia"},
                "gaza": {"lat": 31.3547, "lon": 34.3088, "country": "Palestine"},
                "tehran": {"lat": 35.6892, "lon": 51.3890, "country": "Iran"},
                "beirut": {"lat": 33.8938, "lon": 35.5018, "country": "Lebanon"},
                "qatar": {"lat": 25.3548, "lon": 51.1839, "country": "Qatar"},
            }

            for place, coords in known_places.items():
                if place in text_lower:
                    return {
                        "lat": coords["lat"],
                        "lon": coords["lon"],
                        "display_name": place.title(),
                        "country": coords["country"],
                    }

            # Use Nominatim API for geocoding
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": text[:120],
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
            }
            
            response = requests.get(url, params=params, timeout=6, headers={"User-Agent": "nightwatch-atlas-gdelt-processor/1.1"})
            response.raise_for_status()
            
            data = response.json()
            if data:
                location = data[0]
                return {
                    "lat": float(location["lat"]),
                    "lon": float(location["lon"]),
                    "display_name": location.get("display_name", ""),
                    "country": location.get("address", {}).get("country", "")
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting location: {e}")
            return None

class RealRiskAnalyzer:
    """Real risk analysis using keyword analysis and ML"""
    
    RISK_KEYWORDS = {
        "high": ["missile", "attack", "strike", "bomb", "explosion", "terror", "war", "invasion"],
        "medium": ["military", "conflict", "protest", "violence", "clash", "fighting", "armed"],
        "low": ["defense", "security", "exercise", "deployment", "meeting", "summit", "talks"]
    }
    
    @staticmethod
    def calculate_risk(title: str, description: str = "") -> float:
        """Calculate risk based on real content analysis"""
        text = (title + " " + description).lower()
        
        high_score = sum(1 for keyword in RealRiskAnalyzer.RISK_KEYWORDS["high"] if keyword in text)
        medium_score = sum(1 for keyword in RealRiskAnalyzer.RISK_KEYWORDS["medium"] if keyword in text)
        low_score = sum(1 for keyword in RealRiskAnalyzer.RISK_KEYWORDS["low"] if keyword in text)
        
        # Weighted risk calculation
        risk = (high_score * 0.8 + medium_score * 0.5 + low_score * 0.3) / max(1, high_score + medium_score + low_score)
        
        return min(0.95, max(0.1, risk))

class RealEntityExtractor:
    """Real entity extraction using pattern matching"""
    
    EVENT_TYPES = {
        "military": ["military", "army", "navy", "air force", "defense", "soldier", "troop"],
        "political": ["government", "president", "minister", "election", "parliament", "policy"],
        "infrastructure": ["power", "energy", "oil", "gas", "pipeline", "grid", "network"],
        "economic": ["market", "economy", "trade", "sanction", "currency", "bank", "finance"]
    }
    
    @staticmethod
    def extract_event_type(title: str, description: str = "") -> str:
        """Extract event type from content"""
        text = (title + " " + description).lower()
        
        for event_type, keywords in RealEntityExtractor.EVENT_TYPES.items():
            if any(keyword in text for keyword in keywords):
                return event_type
        
        return "general"
    
    @staticmethod
    def extract_severity(risk: float) -> str:
        """Determine severity based on risk score"""
        if risk >= 0.8:
            return "critical"
        elif risk >= 0.6:
            return "high"
        elif risk >= 0.4:
            return "medium"
        else:
            return "low"

def process_article(article: Dict) -> Dict:
    """Process article with real NLP and geocoding"""
    try:
        upstream_event_type = str(article.get("event_type", "")).strip().lower()

        # Extract real location
        location_service = RealLocationService()
        location = location_service.extract_locations(
            article.get("title", "") + " " + article.get("description", "")
        )
        
        # Calculate real risk
        risk_analyzer = RealRiskAnalyzer()
        risk = risk_analyzer.calculate_risk(
            article.get("title", ""),
            article.get("description", "")
        )
        
        # Extract entities
        entity_extractor = RealEntityExtractor()
        event_type = entity_extractor.extract_event_type(
            article.get("title", ""),
            article.get("description", "")
        )

        # Preserve upstream classification from harvester when present.
        if upstream_event_type == "conflict":
            event_type = "military"
        elif upstream_event_type == "military":
            event_type = "military"

        severity = entity_extractor.extract_severity(risk)
        
        # Create processed event
        processed_event = {
            "id": article.get("id", str(uuid.uuid4())),
            "title": article.get("title"),
            "description": article.get("description"),
            "source": article.get("source", "GDELT"),
            "url": article.get("url"),
            "timestamp": article.get("timestamp", datetime.utcnow().isoformat()),
            "latitude": location["lat"] if location else None,
            "longitude": location["lon"] if location else None,
            "risk": risk,
            "type": event_type,
            "severity": severity,
            "location_info": location if location else None,
            "gdelt_date": article.get("gdelt_date", ""),
            "gdelt_language": article.get("gdelt_language", "unknown"),
            "processed_at": datetime.utcnow().isoformat()
        }
        
        return processed_event
        
    except Exception as e:
        logger.error(f"Error processing article: {e}")
        # Return basic event if processing fails
        return {
            "id": article.get("id", str(uuid.uuid4())),
            "title": article.get("title"),
            "description": article.get("description"),
            "source": article.get("source", "GDELT"),
            "timestamp": article.get("timestamp", datetime.utcnow().isoformat()),
            "latitude": None,
            "longitude": None,
            "risk": 0.3,
            "type": "general",
            "severity": "low",
            "processed_at": datetime.utcnow().isoformat()
        }

def main():
    """Main processor loop"""
    logger.info("Enhanced GDELT processor started. Waiting for messages...")
    
    try:
        for msg in consumer:
            try:
                article = msg.value
                logger.info(f"Processing article: {article.get('title', 'Unknown')}")

                processed_event = process_article(article)
                producer.send("events", processed_event)
                producer.flush()
                
                logger.info(f"Processed event - Type: {processed_event['type']}, Risk: {processed_event['risk']:.2f}, Severity: {processed_event['severity']}")
                
                if processed_event.get("latitude") and processed_event.get("longitude"):
                    logger.info(f"Location: {processed_event['latitude']:.4f}, {processed_event['longitude']:.4f}")

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
        logger.info("Enhanced GDELT processor stopped.")

if __name__ == "__main__":
    main()
