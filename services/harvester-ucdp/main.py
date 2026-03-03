"""
Uppsala Conflict Data Program Harvester
Accesses real-time geopolitical conflict data from UCDP
"""
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
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# UCDP API configuration
UCDP_API_KEY = os.getenv("UCDP_API_KEY")
UCDP_URL = "https://ucdp.uu.se/api/v1.0/conflicts"

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def signal_handler(sig, frame):
    logger.info("Shutting down UCDP harvester...")
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def fetch_ucdp_conflicts():
    """Fetch real conflict data from UCDP API"""
    try:
        headers = {
            "Authorization": f"Bearer {UCDP_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Get active conflicts
        params = {
            "limit": 50,
            "sort": "start_date_desc",
            "active": True,
            "include": "events,deaths,displacements"
        }
        
        response = requests.get(UCDP_URL, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if "data" not in data or not data["data"]:
            logger.warning("No conflict data found in UCDP response")
            return None
        
        logger.info(f"Successfully fetched {len(data['data'])} conflicts from UCDP")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching UCDP data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None

def extract_conflict_location(conflict_data: dict) -> dict:
    """Extract real location from conflict data"""
    try:
        # UCDP provides geographic data
        if "geography" in conflict_data:
            geo = conflict_data["geography"]
            
            # Get coordinates from geography data
            if "coordinates" in geo:
                coords = geo["coordinates"]
                if coords and len(coords) >= 2:
                    return {
                        "lat": coords[0],  # latitude
                        "lon": coords[1],  # longitude
                        "display_name": geo.get("name", "Unknown Location"),
                        "country": geo.get("country", "Unknown Country"),
                        "region": geo.get("region", "Unknown Region"),
                        "confidence": 0.9  # High confidence from official data
                    }
            
            # Try to extract from location names
            if "location" in geo:
                location_name = geo["location"]
                if location_name:
                    return {
                        "lat": None,  # Will be geocoded later
                        "lon": None,
                        "display_name": location_name,
                        "country": geo.get("country", "Unknown Country"),
                        "region": geo.get("region", "Unknown Region"),
                        "confidence": 0.7
                    }
        
        return {
            "lat": None,
            "lon": None,
            "display_name": "Unknown Location",
            "country": "Unknown",
            "region": "Unknown",
            "confidence": 0.0
        }
        
    except Exception as e:
        logger.error(f"Error extracting conflict location: {e}")
        return {
            "lat": None,
            "lon": None,
            "display_name": "Unknown Location",
            "country": "Unknown",
            "region": "Unknown",
            "confidence": 0.0
        }

def calculate_conflict_risk(conflict_data: dict) -> float:
    """Calculate risk based on conflict characteristics"""
    try:
        risk = 0.3  # Base risk
        
        # Intensity level (UCDP provides this)
        if "intensity" in conflict_data:
            intensity = conflict_data["intensity"].lower()
            if intensity == "high":
                risk += 0.4
            elif intensity == "medium":
                risk += 0.2
            elif intensity == "low":
                risk += 0.1
        
        # Conflict type
        conflict_type = conflict_data.get("type", "").lower()
        high_risk_types = ["war", "armed conflict", "civil war", "international conflict"]
        medium_risk_types = ["border conflict", "territorial dispute", "political conflict"]
        
        if any(risk_type in high_risk_types):
            risk += 0.3
        elif any(risk_type in medium_risk_types):
            risk += 0.2
        
        # Recent activity increases risk
        if "events" in conflict_data:
            recent_events = conflict_data["events"][:5] if conflict_data["events"] else []
            for event in recent_events:
                event_desc = event.get("description", "").lower()
                if any(weapon in event_desc for weapon in ["attack", "violence", "casualties", "military"]):
                    risk += 0.1
        
        # Death toll increases risk
        if "deaths" in conflict_data:
            deaths = conflict_data["deaths"]
            if deaths and deaths > 100:
                risk += 0.2
            elif deaths and deaths > 50:
                risk += 0.1
        
        # Displacement increases risk
        if "displaced" in conflict_data:
            displaced = conflict_data["displaced"]
            if displaced and displaced > 10000:
                risk += 0.1
        
        return min(0.95, max(0.1, risk))
        
    except Exception as e:
        logger.error(f"Error calculating conflict risk: {e}")
        return 0.5

def determine_conflict_type(conflict_data: dict) -> str:
    """Determine conflict type from UCDP data"""
    try:
        conflict_type = conflict_data.get("type", "").lower()
        
        # Map UCDP types to our standard types
        type_mapping = {
            "war": "military",
            "armed conflict": "military", 
            "civil war": "military",
            "international conflict": "political",
            "border conflict": "political",
            "territorial dispute": "political",
            "political conflict": "political",
            "ethnic conflict": "general",
            "religious conflict": "general",
            "resource conflict": "infrastructure"
        }
        
        return type_mapping.get(conflict_type, "general")
        
    except Exception as e:
        logger.error(f"Error determining conflict type: {e}")
        return "general"

def convert_to_conflict_event(conflict_data: dict) -> dict:
    """Convert UCDP conflict data to standardized event format"""
    try:
        location = extract_conflict_location(conflict_data)
        risk = calculate_conflict_risk(conflict_data)
        event_type = determine_conflict_type(conflict_data)
        
        # Determine severity based on risk
        if risk >= 0.8:
            severity = "critical"
        elif risk >= 0.6:
            severity = "high"
        elif risk >= 0.4:
            severity = "medium"
        else:
            severity = "low"
        
        event = {
            "id": str(uuid.uuid4()),
            "title": conflict_data.get("name", "Unknown Conflict"),
            "description": conflict_data.get("description", f"Conflict in {location.get('country', 'unknown')}"),
            "source": "UCDP",
            "url": conflict_data.get("url", ""),
            "timestamp": datetime.utcnow().isoformat(),
            "latitude": location["lat"],
            "longitude": location["lon"],
            "risk": risk,
            "type": event_type,
            "severity": severity,
            "location_info": {
                "display_name": location["display_name"],
                "country": location["country"],
                "region": location["region"],
                "confidence": location["confidence"]
            },
            "ucdp_id": conflict_data.get("id"),
            "intensity": conflict_data.get("intensity"),
            "deaths": conflict_data.get("deaths"),
            "displaced": conflict_data.get("displaced"),
            "start_date": conflict_data.get("start_date"),
            "side_a": conflict_data.get("side_a"),
            "side_b": conflict_data.get("side_b"),
            "event_count": len(conflict_data.get("events", [])),
            "processed_at": datetime.utcnow().isoformat()
        }
        
        return event
        
    except Exception as e:
        logger.error(f"Error converting conflict data: {e}")
        return None

def run():
    """Main harvester loop"""
    logger.info("Starting UCDP conflict harvester...")
    logger.info(f"API URL: {UCDP_URL}")
    logger.info(f"Polling interval: 60 seconds")
    
    while True:
        try:
            logger.info("Fetching UCDP conflict data...")
            data = fetch_ucdp_conflicts()
            
            if data and data.get("data"):
                conflicts = data["data"]
                logger.info(f"Processing {len(conflicts)} conflicts")
                
                processed_count = 0
                for conflict in conflicts:
                    try:
                        event = convert_to_conflict_event(conflict)
                        if event:
                            producer.send("raw.ucdp", event)
                            processed_count += 1
                            logger.info(f"Processed conflict: {event['title']} - Risk: {event['risk']:.2f}")
                    except Exception as e:
                        logger.error(f"Error processing conflict: {e}")
                        continue
                
                producer.flush()
                logger.info(f"Sent {processed_count} conflict events to Kafka topic 'raw.ucdp'")
                
            else:
                logger.warning("No conflict data received from UCDP")
                
        except Exception as e:
            logger.error(f"Error in UCDP harvester loop: {e}")
        
        # UCDP updates less frequently than news sources
        time.sleep(60)

if __name__ == "__main__":
    if not UCDP_API_KEY:
        logger.error("UCDP_API_KEY not found in environment variables")
        logger.error("Please set UCDP_API_KEY in your .env file")
        sys.exit(1)
    
    run()
