import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

API_TOKEN = os.getenv("GLOBAL_FISH_API")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
POLL_INTERVAL_SECONDS = int(os.getenv("GLOBAL_FISH_POLL_INTERVAL", "60"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("GLOBAL_FISH_REQUEST_TIMEOUT", "25"))
DEFAULT_LIMIT = int(os.getenv("GLOBAL_FISH_LIMIT", "120"))
DEFAULT_SORT = os.getenv("GLOBAL_FISH_SORT", "-start")

DATASETS = [
    dataset.strip()
    for dataset in os.getenv("GLOBAL_FISH_DATASETS", "public-global-fishing-events:latest").split(",")
    if dataset.strip()
]

EVENTS_URL = os.getenv("GLOBAL_FISH_EVENTS_URL", "https://gateway.api.globalfishingwatch.org/v3/events")

session = requests.Session()
session.headers.update(
    {
        "Authorization": f"Bearer {API_TOKEN}" if API_TOKEN else "",
        "Accept": "application/json",
        "User-Agent": "nightwatch-atlas-gfw-harvester/1.1",
    }
)


def create_producer() -> KafkaProducer:
    retries = 0
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as exc:
            retries += 1
            wait_seconds = min(15, 2 + retries)
            logger.warning("Kafka producer not ready (%s). Retry in %ss", exc, wait_seconds)
            time.sleep(wait_seconds)


def build_query(limit: int, offset: int) -> List[Tuple[str, Any]]:
    params: List[Tuple[str, Any]] = [("limit", limit), ("offset", offset), ("sort", DEFAULT_SORT)]
    for idx, dataset in enumerate(DATASETS):
        params.append((f"datasets[{idx}]", dataset))
    return params


def fetch_events() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    params = build_query(limit=DEFAULT_LIMIT, offset=0)
    try:
        response = session.get(EVENTS_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        if response.status_code in (401, 403):
            return [], f"auth_failed_http_{response.status_code}"
        if response.status_code == 422:
            try:
                payload = response.json()
            except Exception:
                payload = {"raw": response.text[:260]}
            return [], f"query_validation_failed: {payload}"

        response.raise_for_status()
        payload = response.json()
        entries = payload.get("entries") if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            return [], "invalid_payload_entries"
        return [item for item in entries if isinstance(item, dict)], None
    except requests.exceptions.RequestException as exc:
        return [], str(exc)


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_event(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    position = record.get("position") if isinstance(record.get("position"), dict) else {}
    lat = to_float(position.get("lat"))
    lon = to_float(position.get("lon"))
    if lat is None or lon is None:
        return None

    vessel = record.get("vessel") if isinstance(record.get("vessel"), dict) else {}
    vessel_id = str(
        vessel.get("ssvid")
        or vessel.get("id")
        or record.get("vessel_id")
        or record.get("id")
        or uuid.uuid4()
    )
    vessel_name = str(vessel.get("name") or vessel.get("callsign") or "Unknown Vessel")
    course = to_float(record.get("course"))
    speed = to_float(record.get("speed"))
    observed_at = record.get("start") or record.get("end") or datetime.utcnow().isoformat()

    return {
        "id": str(uuid.uuid4()),
        "vessel_id": vessel_id,
        "vessel_name": vessel_name,
        "latitude": lat,
        "longitude": lon,
        "course": course,
        "heading": course,
        "speed": speed,
        "source": "GlobalFishingWatch",
        "provider": "GFW",
        "dataset": record.get("dataset"),
        "event_type": record.get("type"),
        "timestamp": observed_at,
        "ingested_at": datetime.utcnow().isoformat(),
        "raw": record,
    }


def run() -> None:
    if not API_TOKEN:
        logger.error("GLOBAL_FISH_API is missing")
        raise SystemExit(1)

    producer = create_producer()
    logger.info("Starting Global Fishing Watch harvester")
    logger.info("Using datasets: %s", ", ".join(DATASETS))

    total_sent = 0
    while True:
        try:
            records, error = fetch_events()
            if error:
                logger.error("GFW fetch failed: %s", error)
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            sent_now = 0
            for record in records:
                normalized = normalize_event(record)
                if not normalized:
                    continue
                producer.send("raw.gfw", normalized)
                sent_now += 1

            producer.flush()
            total_sent += sent_now
            logger.info("GFW cycle complete: sent %s vessel updates (total %s)", sent_now, total_sent)
        except Exception as exc:
            logger.error("GFW harvester loop failure: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
