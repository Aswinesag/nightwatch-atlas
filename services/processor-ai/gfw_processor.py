"""
Global Fishing Watch vessel processor.
Converts raw vessel positions to normalized `events` records.
"""
import json
import logging
import os
import signal
import sys
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    "raw.gfw",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="gfw-processor-group",
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def signal_handler(sig, frame):
    logger.info("Shutting down GFW processor")
    consumer.close()
    producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def vessel_risk(speed: Optional[float], course: Optional[float]) -> float:
    risk = 0.24
    if speed is not None:
        if speed > 35:
            risk += 0.34
        elif speed > 20:
            risk += 0.2
        elif speed > 12:
            risk += 0.1

    if course is None:
        risk += 0.08

    return min(0.85, risk)


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def process_vessel(vessel: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    lat = to_float(vessel.get("latitude"))
    lon = to_float(vessel.get("longitude"))
    if lat is None or lon is None:
        return None

    course = to_float(vessel.get("course"))
    heading = to_float(vessel.get("heading"))
    speed = to_float(vessel.get("speed"))
    vessel_id = str(vessel.get("vessel_id") or vessel.get("id") or uuid.uuid4())
    vessel_name = str(vessel.get("vessel_name") or "Unknown Vessel").strip() or "Unknown Vessel"
    observed_at = vessel.get("timestamp") or datetime.utcnow().isoformat()
    ingested_timestamp = datetime.utcnow().isoformat()

    risk = vessel_risk(speed=speed, course=course or heading)

    event = {
        "id": str(uuid.uuid4()),
        "title": f"Vessel {vessel_name} tracked",
        "description": (
            f"Maritime track update for {vessel_name} "
            f"(ID {vessel_id}) at lat {lat:.3f}, lon {lon:.3f}."
        ),
        "source": "GFW",
        # Use ingestion time for live stream ordering/windowing; keep source observation separately.
        "timestamp": ingested_timestamp,
        "latitude": lat,
        "longitude": lon,
        "risk": risk,
        "type": "infrastructure",
        "severity": "medium" if risk >= 0.45 else "low",
        "course": course,
        "heading": heading if heading is not None else course,
        "speed": speed,
        "vessel_id": vessel_id,
        "vessel_name": vessel_name,
        "location_info": {
            "display_name": "Maritime Track",
            "provider": "GlobalFishingWatch",
            "provider_id": vessel_id,
            "mode": "vessel",
        },
        "vessel_data": {
            "vessel_id": vessel_id,
            "vessel_name": vessel_name,
            "observed_at": observed_at,
            "course": course,
            "heading": heading if heading is not None else course,
            "speed": speed,
            "endpoint": vessel.get("endpoint"),
            "provider": vessel.get("provider", "GFW"),
        },
        "processed_at": datetime.utcnow().isoformat(),
    }
    return event


def main():
    logger.info("GFW vessel processor started")
    try:
        for msg in consumer:
            vessel = msg.value
            event = process_vessel(vessel)
            if not event:
                continue
            producer.send("events", event)
            producer.flush()
    except KeyboardInterrupt:
        logger.info("GFW processor interrupted")
    except Exception as exc:
        logger.error("GFW processor error: %s", exc)
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
