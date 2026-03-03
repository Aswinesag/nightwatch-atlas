import json
from kafka import KafkaConsumer, KafkaProducer
import uuid
import signal
import sys
import requests
import re
import logging
import os
from typing import Optional, Dict
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

class RealGeolocator:
    """Real geocoding service using OpenStreetMap Nominatim"""
    
    @staticmethod
    def extract_location_from_text(text: str) -> Optional[Dict[str, float]]:
        """Extract real location from text using geocoding"""
        try:
            # First, try to extract location names from text
            location_names = RealGeolocator._extract_place_names(text)
            
            if location_names:
                # Try geocoding with extracted names
                for place_name in location_names[:3]:  # Try top 3 candidates
                    location = RealGeolocator._geocode_place_name(place_name)
                    if location:
                        logger.info(f"Geocoded '{place_name}' to {location['lat']:.4f}, {location['lon']:.4f}")
                        return location
            
            # If no specific locations found, try geocoding the full text
            location = RealGeolocator._geocode_place_name(text)
            if location:
                logger.info(f"Geocoded full text to {location['lat']:.4f}, {location['lon']:.4f}")
                return location
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting location: {e}")
            return None
    
    @staticmethod
    def _extract_place_names(text: str) -> list:
        """Extract potential place names from text"""
        # Common location patterns
        patterns = [
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z][a-z]+)\b',  # City, Country
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:in|at|near|of)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b',  # Place in Place
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:City|Region|Province|State|Country)\b',  # Place + Type
        ]
        
        place_names = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if isinstance(match, tuple):
                    place_names.extend(match)
                else:
                    place_names.append(match)
        
        # Common country names to look for
        countries = [
            'Afghanistan', 'Albania', 'Algeria', 'Argentina', 'Australia', 'Austria', 'Bangladesh',
            'Belgium', 'Brazil', 'Canada', 'China', 'Colombia', 'Congo', 'Cuba', 'Denmark',
            'Egypt', 'France', 'Germany', 'Greece', 'India', 'Indonesia', 'Iran', 'Iraq',
            'Ireland', 'Israel', 'Italy', 'Japan', 'Mexico', 'Netherlands', 'Nigeria',
            'North Korea', 'Pakistan', 'Philippines', 'Poland', 'Portugal', 'Romania',
            'Russia', 'Saudi Arabia', 'South Africa', 'South Korea', 'Spain', 'Sweden',
            'Switzerland', 'Syria', 'Turkey', 'Ukraine', 'United Kingdom', 'United States',
            'Venezuela', 'Vietnam', 'Yemen', 'Zimbabwe'
        ]
        
        # Check for country mentions
        for country in countries:
            if country.lower() in text.lower():
                place_names.append(country)
        
        # Remove duplicates and common words
        filtered_names = []
        for name in place_names:
            name = name.strip()
            if (len(name) > 2 and 
                name.lower() not in ['the', 'and', 'for', 'with', 'from', 'that', 'this', 'have', 'been'] and
                name not in filtered_names):
                filtered_names.append(name)
        
        return filtered_names
    
    @staticmethod
    def _geocode_place_name(place_name: str) -> Optional[Dict[str, float]]:
        """Geocode a place name using Nominatim API"""
        try:
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": place_name,
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
                "countrycodes": "us,gb,fr,de,ru,cn,in,br,za,au,ca,mx,jp,kr,in,pk,sa,ir,iq,af,ua,il,tr,eg,za,ng,ke,ar,co,ve,cl,pe,ec,bo,py,uy,gy,sr,sr"
            }
            
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            if data:
                location = data[0]
                return {
                    "lat": float(location["lat"]),
                    "lon": float(location["lon"]),
                    "display_name": location.get("display_name", ""),
                    "country": location.get("address", {}).get("country", ""),
                    "confidence": 0.8
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error geocoding '{place_name}': {e}")
            return None

def assign_real_location(article: Dict) -> Optional[Dict[str, float]]:
    """Assign real location based on article content"""
    text = f"{article.get('title', '')} {article.get('description', '')}"
    
    geolocator = RealGeolocator()
    location = geolocator.extract_location_from_text(text)
    
    if location:
        return {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "location_info": {
                "display_name": location["display_name"],
                "country": location["country"],
                "confidence": location["confidence"]
            }
        }
    
    return None

def calculate_risk(title, description=""):
    """Calculate risk based on geopolitical conflict indicators"""
    text = f"{title} {description}".lower()
    risk = 0.3  # Base risk
    
    # High-risk conflict indicators
    high_risk_terms = [
        "invasion", "war declared", "full-scale war", "military operation",
        "airstrike", "missile strike", "artillery barrage", "ground offensive",
        "heavy fighting", "intense combat", "major battle", "frontline"
    ]
    
    # Medium-risk conflict indicators
    medium_risk_terms = [
        "armed conflict", "military clash", "border skirmish", "troop deployment",
        "military exercises", "tensions rise", "escalating conflict", "ceasefire violation"
    ]
    
    # Humanitarian impact indicators
    humanitarian_terms = [
        "casualties", "fatalities", "deaths", "injured", "wounded",
        "refugees", "displaced", "evacuated", "humanitarian crisis",
        "mass evacuation", "emergency evacuation"
    ]
    
    # Infrastructure impact indicators
    infrastructure_terms = [
        "power plant", "energy facility", "oil refinery", "pipeline",
        "bridge destroyed", "airport closed", "port damaged", "telecommunications"
    ]
    
    # Check for high-risk terms
    if any(term in text for term in high_risk_terms):
        risk += 0.5
    
    # Check for medium-risk terms
    if any(term in text for term in medium_risk_terms):
        risk += 0.3
    
    # Check for humanitarian impact
    if any(term in text for term in humanitarian_terms):
        risk += 0.2
    
    # Check for infrastructure impact
    if any(term in text for term in infrastructure_terms):
        risk += 0.2
    
    # Check for specific high-risk weapons
    if any(weapon in text for weapon in ["nuclear", "chemical", "biological"]):
        risk += 0.4
    
    # Check for major military powers involvement
    major_powers = ["usa", "united states", "russia", "china", "uk", "united kingdom", 
                   "france", "germany", "israel", "iran", "north korea"]
    if any(power in text for power in major_powers):
        risk += 0.1
    
    # Check for escalation terms
    escalation_terms = ["escalating", "intensifying", "worsening", "deteriorating"]
    if any(term in text for term in escalation_terms):
        risk += 0.1
    
    return min(0.95, risk)  # Cap at 0.95

print("GDELT processor started. Waiting for messages...")

try:
    for msg in consumer:
        try:
            article = msg.value
            logger.info(f"Processing article: {article.get('title', 'Unknown')}")

            # Use real geolocation instead of random
            location_data = assign_real_location(article)
            
            if location_data:
                event_type = classify_event_type(article["title"], article["description"])
                risk = calculate_risk(article["title"], article["description"])
                
                # Calculate severity based on risk
                if risk >= 0.8:
                    severity = "critical"
                elif risk >= 0.6:
                    severity = "high"
                elif risk >= 0.4:
                    severity = "medium"
                else:
                    severity = "low"
                
                # Generate alert for military activity
                alert_level = "high" if article.get("event_type") == "military" else "medium"
                if article.get("event_type") == "military" and risk >= 0.6:
                    alert_level = "critical"
                
                event = {
                    "id": str(uuid.uuid4()),
                    "title": article["title"],
                    "description": article["description"],
                    "latitude": location_data["latitude"],
                    "longitude": location_data["longitude"],
                    "risk": risk,
                    "type": event_type,
                    "severity": severity,
                    "source": "GDELT",
                    "location_info": location_data.get("location_info"),
                    "alert_level": alert_level,
                    "event_category": article.get("event_type", "general"),
                    "conflict_analysis": {
                        "event_type": event_type,
                        "risk_factors": {
                            "military_involvement": any(term in f"{article['title']} {article['description']}".lower() for term in ["military", "armed", "troops", "combat"]),
                            "humanitarian_impact": any(term in f"{article['title']} {article['description']}".lower() for term in ["casualties", "refugees", "displaced", "injured"]),
                            "infrastructure_damage": any(term in f"{article['title']} {article['description']}".lower() for term in ["power plant", "bridge", "airport", "port", "pipeline"]),
                            "escalation_indicators": any(term in f"{article['title']} {article['description']}".lower() for term in ["escalating", "intensifying", "worsening"])
                        },
                        "processed_at": datetime.utcnow().isoformat()
                    }
                }
                logger.info(f"Geocoded event to {location_data['latitude']:.4f}, {location_data['longitude']:.4f} - Risk: {risk:.2f}, Type: {event_type}, Alert: {alert_level}")
            else:
                # If geocoding fails, still create event but without location
                event_type = classify_event_type(article["title"], article["description"])
                risk = calculate_risk(article["title"], article["description"])
                
                # Calculate severity based on risk
                if risk >= 0.8:
                    severity = "critical"
                elif risk >= 0.6:
                    severity = "high"
                elif risk >= 0.4:
                    severity = "medium"
                else:
                    severity = "low"
                
                # Generate alert for military activity
                alert_level = "high" if article.get("event_type") == "military" else "medium"
                if article.get("event_type") == "military" and risk >= 0.6:
                    alert_level = "critical"
                
                event = {
                    "id": str(uuid.uuid4()),
                    "title": article["title"],
                    "description": article["description"],
                    "latitude": None,
                    "longitude": None,
                    "risk": risk,
                    "type": event_type,
                    "severity": severity,
                    "source": "GDELT",
                    "location_info": None,
                    "alert_level": alert_level,
                    "event_category": article.get("event_type", "general"),
                    "conflict_analysis": {
                        "event_type": event_type,
                        "risk_factors": {
                            "military_involvement": any(term in f"{article['title']} {article['description']}".lower() for term in ["military", "armed", "troops", "combat"]),
                            "humanitarian_impact": any(term in f"{article['title']} {article['description']}".lower() for term in ["casualties", "refugees", "displaced", "injured"]),
                            "infrastructure_damage": any(term in f"{article['title']} {article['description']}".lower() for term in ["power plant", "bridge", "airport", "port", "pipeline"]),
                            "escalation_indicators": any(term in f"{article['title']} {article['description']}".lower() for term in ["escalating", "intensifying", "worsening"])
                        },
                        "processed_at": datetime.utcnow().isoformat()
                    }
                }
                logger.warning("Could not geocode event - no location assigned")

            producer.send("events", event)
            producer.flush()
            logger.info(f"Processed and sent event with risk: {event['risk']}")

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
    logger.info("GDELT processor stopped.")
