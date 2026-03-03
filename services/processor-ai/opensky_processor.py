"""
OpenSky Aircraft Data Processor
Processes real aircraft tracking data from OpenSky API
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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    "raw.opensky",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="opensky-processor-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def signal_handler(sig, frame):
    logger.info("Shutting down OpenSky processor...")
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def calculate_aircraft_risk(aircraft_data: Dict) -> float:
    """Calculate risk based on aircraft behavior and type"""
    try:
        # Risk factors for aircraft
        altitude = aircraft_data.get("altitude", 0)
        velocity = aircraft_data.get("velocity", 0)
        
        # High altitude or unusual speed increases risk
        if altitude > 10000:  # Very high altitude
            risk = 0.6
        elif altitude > 5000:  # High altitude
            risk = 0.4
        else:
            risk = 0.2
        
        # Unusual velocity increases risk
        if velocity > 500:  # Very fast
            risk += 0.3
        elif velocity > 300:  # Fast
            risk += 0.1
        
        # Military aircraft (based on callsign patterns)
        callsign = aircraft_data.get("callsign", "")
        if any(military in callsign.upper() for military in ["MIL", "AF", "NAVY", "ARMY"]):
            risk += 0.4
        
        return min(0.9, risk)
        
    except Exception as e:
        logger.error(f"Error calculating risk: {e}")
        return 0.3

def determine_event_type(aircraft_data: Dict) -> str:
    """Determine event type based on aircraft data"""
    callsign = aircraft_data.get("callsign", "").upper()
    
    # Military aircraft patterns
    if any(pattern in callsign for pattern in ["MIL", "AF", "NAVY", "ARMY", "FIGHTER", "BOMBER"]):
        return "military"
    
    # Commercial aircraft patterns
    if any(pattern in callsign for pattern in ["UAL", "DAL", "AAL", "SWA", "LUF"]):
        return "infrastructure"
    
    # Unknown/unusual patterns
    if not callsign or callsign.startswith("X"):
        return "general"
    
    return "infrastructure"

def process_aircraft_data(aircraft_data: Dict) -> Dict:
    """Process aircraft data into standardized event format"""
    try:
        event = {
            "id": str(uuid.uuid4()),
            "title": f"Aircraft {aircraft_data.get('callsign', 'Unknown')} detected",
            "description": f"Aircraft callsign {aircraft_data.get('callsign', 'Unknown')} at altitude {aircraft_data.get('altitude', 'Unknown')}m, velocity {aircraft_data.get('velocity', 'Unknown')}m/s",
            "source": "OpenSky",
            "timestamp": datetime.utcnow().isoformat(),
            "latitude": aircraft_data.get("latitude"),
            "longitude": aircraft_data.get("longitude"),
            "risk": calculate_aircraft_risk(aircraft_data),
            "type": determine_event_type(aircraft_data),
            "severity": "medium",  # Default for aircraft
            "altitude": aircraft_data.get("altitude"),
            "velocity": aircraft_data.get("velocity"),
            "callsign": aircraft_data.get("callsign"),
            "processed_at": datetime.utcnow().isoformat()
        }
        
        return event
        
    except Exception as e:
        logger.error(f"Error processing aircraft data: {e}")
        return None

def main():
    """Main processor loop"""
    logger.info("OpenSky aircraft processor started. Waiting for messages...")
    
    try:
        for msg in consumer:
            try:
                aircraft_data = msg.value
                logger.info(f"Processing aircraft: {aircraft_data.get('callsign', 'Unknown')}")
                
                event = process_aircraft_data(aircraft_data)
                if event:
                    producer.send("events", event)
                    producer.flush()
                    logger.info(f"Processed aircraft event - Type: {event['type']}, Risk: {event['risk']:.2f}")
                
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
        logger.info("OpenSky processor stopped.")

if __name__ == "__main__":
    main()
