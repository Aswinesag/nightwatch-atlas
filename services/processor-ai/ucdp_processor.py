"""
UCDP Conflict Data Processor
Processes real geopolitical conflict data from UCDP API
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
    "raw.ucdp",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="ucdp-processor-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def signal_handler(sig, frame):
    logger.info("Shutting down UCDP processor...")
    consumer.close()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def enhance_conflict_event(event_data: Dict) -> Dict:
    """Enhance conflict event with additional analysis"""
    try:
        # Extract additional insights from UCDP data
        enhanced_event = event_data.copy()
        
        # Add conflict-specific analysis
        if event_data.get("intensity") == "high":
            enhanced_event["threat_level"] = "severe"
        elif event_data.get("intensity") == "medium":
            enhanced_event["threat_level"] = "elevated"
        else:
            enhanced_event["threat_level"] = "moderate"
        
        # Add casualty analysis
        deaths = event_data.get("deaths", 0)
        displaced = event_data.get("displaced", 0)
        
        if deaths > 0:
            enhanced_event["casualty_level"] = "fatalities" if deaths > 10 else "injuries"
        else:
            enhanced_event["casualty_level"] = "none"
        
        if displaced > 0:
            if displaced > 50000:
                enhanced_event["humanitarian_impact"] = "mass_displacement"
            elif displaced > 10000:
                enhanced_event["humanitarian_impact"] = "major_displacement"
            else:
                enhanced_event["humanitarian_impact"] = "minor_displacement"
        else:
            enhanced_event["humanitarian_impact"] = "none"
        
        # Add regional stability impact
        country = event_data.get("location_info", {}).get("country", "")
        if country:
            # High-risk regions for ongoing conflicts
            high_risk_regions = [
                "middle east", "ukraine", "russia", "israel", "palestine",
                "sudan", "ethiopia", "somalia", "yemen", "syria", "iraq",
                "afghanistan", "myanmar", "north korea", "south sudan"
            ]
            
            if any(region.lower() in country.lower() for region in high_risk_regions):
                enhanced_event["regional_stability"] = "high_risk"
            else:
                enhanced_event["regional_stability"] = "stable"
        else:
            enhanced_event["regional_stability"] = "unknown"
        
        # Add conflict duration analysis
        start_date = event_data.get("start_date")
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                duration_days = (datetime.utcnow() - start_dt).days
                enhanced_event["conflict_duration_days"] = duration_days
                
                if duration_days > 365:
                    enhanced_event["conflict_category"] = "protracted"
                elif duration_days > 30:
                    enhanced_event["conflict_category"] = "ongoing"
                else:
                    enhanced_event["conflict_category"] = "emerging"
            except:
                enhanced_event["conflict_duration_days"] = 0
                enhanced_event["conflict_category"] = "unknown"
        else:
            enhanced_event["conflict_duration_days"] = 0
            enhanced_event["conflict_category"] = "unknown"
        
        enhanced_event["enhanced_at"] = datetime.utcnow().isoformat()
        return enhanced_event
        
    except Exception as e:
        logger.error(f"Error enhancing conflict event: {e}")
        return event_data

def process_conflict_event(conflict_data: Dict) -> Dict:
    """Process UCDP conflict data into standardized event format"""
    try:
        # Validate required fields
        if not conflict_data.get("title"):
            logger.warning("Conflict event missing title, skipping")
            return None
        
        # Ensure location data is properly formatted
        location_info = conflict_data.get("location_info", {})
        if not location_info.get("lat") or not location_info.get("lon"):
            logger.warning(f"Conflict event {conflict_data.get('title')} missing coordinates")
            # Still process but mark as missing location
            conflict_data["latitude"] = None
            conflict_data["longitude"] = None
        else:
            conflict_data["latitude"] = location_info["lat"]
            conflict_data["longitude"] = location_info["lon"]
        
        # Add processing metadata
        conflict_data["processed_at"] = datetime.utcnow().isoformat()
        conflict_data["processor"] = "ucdp-processor"
        
        logger.info(f"Processed conflict: {conflict_data.get('title')} - {conflict_data.get('type', 'unknown')}")
        return conflict_data
        
    except Exception as e:
        logger.error(f"Error processing conflict event: {e}")
        return None

def main():
    """Main processor loop"""
    logger.info("UCDP conflict processor started. Waiting for messages...")
    
    try:
        for msg in consumer:
            try:
                conflict_data = msg.value
                logger.info(f"Processing conflict: {conflict_data.get('title', 'Unknown')}")
                
                # Process and enhance the conflict event
                processed_event = process_conflict_event(conflict_data)
                if processed_event:
                    # Further enhance with additional analysis
                    enhanced_event = enhance_conflict_event(processed_event)
                    producer.send("events", enhanced_event)
                    producer.flush()
                    
                    logger.info(f"Enhanced conflict event sent - Risk: {enhanced_event['risk']:.2f}, "
                              f"Type: {enhanced_event['type']}, "
                              f"Severity: {enhanced_event['severity']}")
                
            except Exception as e:
                logger.error(f"Error processing conflict message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        consumer.close()
        producer.close()
        logger.info("UCDP conflict processor stopped.")

if __name__ == "__main__":
    main()
