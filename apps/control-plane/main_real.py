"""
Enhanced Control Plane with Real API Integration
No hardcoded data - supports dynamic event processing
"""
from fastapi import FastAPI, WebSocket, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sys
import os
import signal
import asyncio
import json
import logging
import requests
import time
import hashlib
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from collections import Counter

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'packages'))
import messaging.kafka
from messaging.kafka import get_consumer as get_kafka_consumer, get_producer as get_kafka_producer
import threading
import uvicorn

# Enhanced imports
from db.database import engine, Base, SessionLocal
from models_real import Event, EventEntity, EventLocation, DataSource, ProcessingLog
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, inspect, text, and_
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_event_schema():
    inspector = inspect(engine)
    if "events" not in inspector.get_table_names():
        Base.metadata.create_all(bind=engine)
        return

    existing_cols = {c["name"] for c in inspector.get_columns("events")}
    required_cols = {
        "source": "VARCHAR",
        "url": "VARCHAR",
        "timestamp": "TIMESTAMP",
        "latitude": "DOUBLE PRECISION",
        "longitude": "DOUBLE PRECISION",
        "gdelt_date": "VARCHAR",
        "gdelt_language": "VARCHAR",
        "processed_at": "TIMESTAMP",
        "location_info": "JSONB",
        "raw_text": "TEXT",
        "canonical_type": "VARCHAR",
        "confidence": "DOUBLE PRECISION",
        "quality_score": "DOUBLE PRECISION",
        "correlation_key": "VARCHAR",
        "fingerprint": "VARCHAR",
        "expires_at": "TIMESTAMP",
    }

    with engine.begin() as conn:
        for col_name, col_type in required_cols.items():
            if col_name not in existing_cols:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {col_name} {col_type}"))

        # Backfill modern coordinate columns from legacy schema if present.
        if "lat" in existing_cols:
            conn.execute(text("UPDATE events SET latitude = lat WHERE latitude IS NULL AND lat IS NOT NULL"))
        if "lon" in existing_cols:
            conn.execute(text("UPDATE events SET longitude = lon WHERE longitude IS NULL AND lon IS NOT NULL"))


# Ensure schema is compatible with the enhanced model before startup.
ensure_event_schema()
Base.metadata.create_all(bind=engine)

# Pydantic models for API
class EventResponse(BaseModel):
    id: str
    title: str
    description: str
    risk: float
    lat: Optional[float]
    lon: Optional[float]
    type: str
    severity: str
    time: str
    created_at: str
    source: str
    location_info: Optional[Dict[str, Any]] = None
    confidence: Optional[float] = None
    quality_score: Optional[float] = None
    correlation_key: Optional[str] = None

class EventStats(BaseModel):
    total_events: int
    events_by_type: Dict[str, int]
    events_by_severity: Dict[str, int]
    average_risk: float
    latest_event: Optional[str]


class AggregateCell(BaseModel):
    cell_id: str
    center_lat: float
    center_lon: float
    event_count: int
    avg_risk: float
    max_risk: float


class AIBriefRequest(BaseModel):
    selected_event_id: Optional[str] = None
    limit: int = 140


class AIBriefResponse(BaseModel):
    brief: str
    generated_at: str
    model: str
    used_fallback: bool
    context_events: int

app = FastAPI(title="NightWatch Atlas API", version="2.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://localhost:5176",
        "http://127.0.0.1:5176",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket clients
clients = []

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def build_fallback_brief(events: List[Event], focus_event: Optional[Event]) -> str:
    if not events:
        return (
            "Executive Summary:\n"
            "- No recent events available in the selected time window.\n\n"
            "Top Risks:\n"
            "- Insufficient telemetry for threat prioritization.\n\n"
            "Recommended Actions:\n"
            "- Verify harvester ingestion and Kafka topic flow."
        )

    source_counts = Counter((event.source or "Unknown") for event in events)
    type_counts = Counter((event.type or "general") for event in events)
    top_risk = sorted(events, key=lambda e: e.risk or 0.0, reverse=True)[:3]

    source_line = ", ".join(f"{name}:{count}" for name, count in source_counts.most_common(4))
    type_line = ", ".join(f"{name}:{count}" for name, count in type_counts.most_common(4))
    risk_lines = [f"- {event.title} (risk {event.risk:.2f}, source {event.source or 'Unknown'})" for event in top_risk]

    focus_line = ""
    if focus_event:
        focus_line = (
            f"- Focused Incident: {focus_event.title} | risk {focus_event.risk:.2f} | "
            f"type {focus_event.type} | source {focus_event.source or 'Unknown'}\n"
        )

    return (
        "Executive Summary:\n"
        f"- Event volume in context: {len(events)}.\n"
        f"- Source mix: {source_line}.\n"
        f"- Type mix: {type_line}.\n"
        f"{focus_line}\n"
        "Top Risks:\n"
        + ("\n".join(risk_lines) if risk_lines else "- No high-risk incidents in current slice.")
        + "\n\nRecommended Actions:\n"
        "- Maintain geospatial watch on top-risk clusters.\n"
        "- Cross-check OTX indicators with concurrent political/military events.\n"
        "- Increase analyst review cadence if risk > 0.80 incidents increase."
    )


def call_groq_brief(events: List[Event], focus_event: Optional[Event]) -> Optional[Dict[str, str]]:
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        return None

    model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    context_rows = []
    # Keep payload compact to avoid provider-side 413 errors.
    for event in events[:45]:
        context_rows.append({
            "id": event.id,
            "title": (event.title or "")[:180],
            "risk": round(float(event.risk or 0), 3),
            "type": event.type,
            "source": event.source,
            "time": event.timestamp.isoformat() if event.timestamp else None,
            "lat": event.latitude,
            "lon": event.longitude,
        })

    focus_payload = None
    if focus_event:
        focus_payload = {
            "id": focus_event.id,
            "title": focus_event.title,
            "risk": round(float(focus_event.risk or 0), 3),
            "type": focus_event.type,
            "source": focus_event.source,
            "time": focus_event.timestamp.isoformat() if focus_event.timestamp else None,
            "description": (focus_event.description or "")[:420],
        }

    body = {
        "model": model,
        "temperature": 0.25,
        "max_tokens": 900,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an intelligence analyst assistant for a real-time operations center. "
                    "Return concise markdown with sections: Executive Summary, Top Risks, Recommended Actions. "
                    "Use only supplied context. Avoid speculation. Mention uncertainty when needed."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "focus_event": focus_payload,
                        "event_count": len(events),
                        "events": context_rows,
                    }
                ),
            },
        ],
    }

    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {groq_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=25,
        )
        response.raise_for_status()
        payload = response.json()
        content = payload.get("choices", [{}])[0].get("message", {}).get("content")
        if not content:
            return None
        return {"brief": content, "model": model}
    except Exception as exc:
        logger.error(f"Groq brief generation failed: {exc}")
        return None

def signal_handler(sig, frame):
    logger.info("Shutting down control plane...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class EnhancedEventProcessor:
    """Enhanced event processor with real-time capabilities"""
    
    def __init__(self):
        self.clients = clients
    
    async def broadcast_to_clients(self, event: Dict[str, Any]):
        """Broadcast event to all WebSocket clients"""
        message = json.dumps(event)
        disconnected_clients = []
        
        for client in self.clients:
            try:
                await client.send_text(message)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected_clients.append(client)
        
        # Remove disconnected clients
        for client in disconnected_clients:
            if client in self.clients:
                self.clients.remove(client)

    def validate_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Canonical schema + quality gate for incoming stream events."""
        errors: List[str] = []

        event_id = str(event.get("id", "")).strip()
        if not event_id:
            errors.append("missing_id")

        title = str(event.get("title", "")).strip()
        if not title:
            errors.append("missing_title")

        try:
            risk = float(event.get("risk", 0.0))
        except (TypeError, ValueError):
            risk = 0.0
            errors.append("invalid_risk")
        risk = max(0.0, min(1.0, risk))

        source = str(event.get("source", "Unknown")).strip() or "Unknown"
        event_type = str(event.get("type", "general")).strip().lower() or "general"
        severity = str(event.get("severity", "medium")).strip().lower() or "medium"

        lat = event.get("latitude", event.get("lat"))
        lon = event.get("longitude", event.get("lon"))
        lat_ok = isinstance(lat, (int, float)) and -90 <= float(lat) <= 90
        lon_ok = isinstance(lon, (int, float)) and -180 <= float(lon) <= 180
        if lat is not None and not lat_ok:
            errors.append("invalid_latitude")
        if lon is not None and not lon_ok:
            errors.append("invalid_longitude")
        if lat_ok != lon_ok:
            errors.append("partial_geo")

        timestamp_raw = event.get("timestamp") or event.get("time") or datetime.utcnow().isoformat()
        try:
            timestamp = datetime.fromisoformat(str(timestamp_raw).replace("Z", "+00:00"))
            if timestamp.tzinfo is not None:
                timestamp = timestamp.replace(tzinfo=None)
        except ValueError:
            errors.append("invalid_timestamp")
            timestamp = datetime.utcnow()

        confidence = event.get("confidence")
        try:
            confidence_value = float(confidence) if confidence is not None else 0.5
        except (TypeError, ValueError):
            confidence_value = 0.5
        confidence_value = max(0.0, min(1.0, confidence_value))

        quality_score = 1.0
        quality_score -= 0.18 * len(errors)
        if lat_ok and lon_ok:
            quality_score += 0.1
        quality_score += confidence_value * 0.12
        quality_score = max(0.0, min(1.0, quality_score))

        canonical_type = event_type if event_type in {"general", "military", "political", "infrastructure", "cyber"} else "general"

        geo_bucket = "nogeo"
        if lat_ok and lon_ok:
            geo_bucket = f"{round(float(lat), 1)}:{round(float(lon), 1)}"
        time_bucket = timestamp.strftime("%Y%m%d%H%M")
        fingerprint_raw = f"{source}|{canonical_type}|{title.lower()}|{geo_bucket}|{time_bucket}"
        fingerprint = hashlib.sha1(fingerprint_raw.encode("utf-8")).hexdigest()
        correlation_key = f"{source.lower()}:{canonical_type}:{geo_bucket}"

        # Drop only severely malformed events.
        is_valid = "missing_id" not in errors and "missing_title" not in errors and quality_score >= 0.25

        return {
            "is_valid": is_valid,
            "errors": errors,
            "normalized": {
                **event,
                "id": event_id,
                "title": title,
                "risk": risk,
                "source": source,
                "type": canonical_type,
                "canonical_type": canonical_type,
                "severity": severity,
                "timestamp": timestamp.isoformat(),
                "latitude": float(lat) if lat_ok else None,
                "longitude": float(lon) if lon_ok else None,
                "confidence": confidence_value,
                "quality_score": quality_score,
                "fingerprint": fingerprint,
                "correlation_key": correlation_key,
                "expires_at": (timestamp + timedelta(hours=36)).isoformat(),
            },
        }

    def store_event(self, event: Dict[str, Any], db: Session) -> Event:
        """Store event in database with enhanced logging"""
        try:
            normalized = self.validate_event(event)
            if not normalized["is_valid"]:
                logger.warning(f"Dropping low-quality event {event.get('id')} errors={normalized['errors']}")
                raise ValueError("event_failed_quality_gate")

            clean_event = normalized["normalized"]

            # Deduplicate recent equivalent events by fingerprint.
            duplicate = (
                db.query(Event)
                .filter(
                    and_(
                        Event.fingerprint == clean_event["fingerprint"],
                        Event.timestamp >= datetime.utcnow() - timedelta(minutes=6),
                    )
                )
                .first()
            )
            if duplicate:
                logger.info(f"Deduped event by fingerprint: {clean_event['fingerprint']}")
                return duplicate

            # Create database event
            db_event = Event(
                id=clean_event["id"],
                title=clean_event["title"],
                description=clean_event.get("description", ""),
                source=clean_event.get("source", "Unknown"),
                url=clean_event.get("url"),
                timestamp=datetime.fromisoformat(clean_event.get("timestamp", datetime.utcnow().isoformat())),
                latitude=clean_event.get("latitude"),
                longitude=clean_event.get("longitude"),
                risk=clean_event["risk"],
                type=clean_event["type"],
                severity=clean_event["severity"],
                gdelt_date=clean_event.get("gdelt_date"),
                gdelt_language=clean_event.get("gdelt_language"),
                processed_at=datetime.fromisoformat(clean_event.get("processed_at", datetime.utcnow().isoformat())),
                location_info=clean_event.get("location_info"),
                raw_text=clean_event.get("raw_text"),
                canonical_type=clean_event.get("canonical_type"),
                confidence=clean_event.get("confidence", 0.5),
                quality_score=clean_event.get("quality_score", 0.5),
                correlation_key=clean_event.get("correlation_key"),
                fingerprint=clean_event.get("fingerprint"),
                expires_at=datetime.fromisoformat(clean_event.get("expires_at")),
            )
            
            db.add(db_event)
            db.commit()
            db.refresh(db_event)
            
            # Log processing
            log_entry = ProcessingLog(
                event_id=db_event.id,
                processor_type="ingestion",
                processing_time=0.1,
                success=True
            )
            db.add(log_entry)
            db.commit()
            
            logger.info(f"Stored event: {event['title']}")
            return db_event
            
        except Exception as e:
            logger.error(f"Error storing event: {e}")
            db.rollback()
            raise

# Initialize processor
processor = EnhancedEventProcessor()

def consume_events():
    """Consume events from Kafka and process them"""
    logger.info("Starting Kafka consumer...")

    while True:
        consumer = None
        try:
            consumer = get_kafka_consumer("events")
            logger.info("Kafka consumer created successfully")

            for msg in consumer:
                try:
                    event_data = msg.value
                    logger.info(f"Received event: {event_data.get('title', 'Unknown')}")

                    # Store in database
                    db = SessionLocal()
                    try:
                        stored_event = processor.store_event(event_data, db)

                        # Broadcast to WebSocket clients
                        if stored_event:
                            outbound = {
                                **event_data,
                                "confidence": float(stored_event.confidence or 0.5),
                                "quality_score": float(stored_event.quality_score or 0.5),
                                "correlation_key": stored_event.correlation_key,
                                "canonical_type": stored_event.canonical_type,
                            }
                            asyncio.run(processor.broadcast_to_clients(outbound))
                        logger.info("Event broadcasted to clients")

                    except Exception as e:
                        logger.error(f"Database error: {e}")
                        db.rollback()
                    finally:
                        db.close()

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except Exception as e:
            logger.error(f"Consumer error: {e}. Retrying in 5s...")
            time.sleep(5)
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

# Start consumer thread
threading.Thread(target=consume_events, daemon=True).start()

# API Endpoints
@app.get("/")
async def root():
    return {"message": "NightWatch Atlas API v2.0", "status": "running"}

@app.get("/events", response_model=List[EventResponse])
def get_events(
    limit: int = 50,
    min_risk: Optional[float] = None,
    max_risk: Optional[float] = None,
    event_type: Optional[str] = None,
    severity: Optional[str] = None,
    min_quality: float = 0.0,
    db: Session = Depends(get_db)
):
    """Get events with dynamic filtering"""
    query = db.query(Event)
    
    # Apply filters
    if min_risk is not None:
        query = query.filter(Event.risk >= min_risk)
    if max_risk is not None:
        query = query.filter(Event.risk <= max_risk)
    if event_type:
        query = query.filter(Event.type == event_type)
    if severity:
        query = query.filter(Event.severity == severity)
    query = query.filter(Event.quality_score >= max(0.0, min(min_quality, 1.0)))
    
    # Order by timestamp and limit
    events = query.order_by(desc(Event.timestamp)).limit(limit).all()
    
    return [
        EventResponse(
            id=event.id,
            title=event.title,
            description=event.description,
            risk=event.risk,
            lat=event.latitude,
            lon=event.longitude,
            type=event.type,
            severity=event.severity,
            time=event.timestamp.isoformat() if event.timestamp else "",
            created_at=event.created_at.isoformat(),
            source=event.source,
            location_info=event.location_info,
            confidence=event.confidence,
            quality_score=event.quality_score,
            correlation_key=event.correlation_key,
        )
        for event in events
    ]

@app.get("/events/stats", response_model=EventStats)
def get_event_stats(db: Session = Depends(get_db)):
    """Get real-time event statistics"""
    total_events = db.query(Event).count()
    
    # Events by type
    events_by_type = dict(
        db.query(Event.type, func.count(Event.id))
        .group_by(Event.type)
        .all()
    )
    
    # Events by severity
    events_by_severity = dict(
        db.query(Event.severity, func.count(Event.id))
        .group_by(Event.severity)
        .all()
    )
    
    # Average risk
    avg_risk = db.query(func.avg(Event.risk)).scalar() or 0.0
    
    # Latest event
    latest_event_obj = db.query(Event).order_by(desc(Event.timestamp)).first()
    latest_event = latest_event_obj.title if latest_event_obj else None
    
    return EventStats(
        total_events=total_events,
        events_by_type=events_by_type,
        events_by_severity=events_by_severity,
        average_risk=round(avg_risk, 2),
        latest_event=latest_event
    )

@app.get("/events/types")
def get_event_types(db: Session = Depends(get_db)):
    """Get all available event types"""
    types = db.query(Event.type).distinct().all()
    return {"types": [t[0] for t in types]}

@app.get("/events/bbox")
def get_events_in_bbox(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int = 220,
    min_quality: float = 0.2,
    db: Session = Depends(get_db)
):
    """Get events within geographic bounding box"""
    query = db.query(Event).filter(
        Event.latitude.between(min_lat, max_lat),
        Event.longitude.between(min_lon, max_lon),
        Event.quality_score >= max(0.0, min(min_quality, 1.0)),
    ).order_by(desc(Event.timestamp)).limit(limit)
    
    events = query.all()
    
    return [
        {
            "id": event.id,
            "title": event.title,
            "description": event.description,
            "risk": event.risk,
            "lat": event.latitude,
            "lon": event.longitude,
            "type": event.type,
            "severity": event.severity,
            "canonical_type": event.canonical_type,
            "confidence": event.confidence,
            "quality_score": event.quality_score,
            "correlation_key": event.correlation_key,
            "time": event.timestamp.isoformat() if event.timestamp else "",
            "source": event.source,
            "location_info": event.location_info
        }
        for event in events
    ]


@app.get("/events/aggregates", response_model=List[AggregateCell])
def get_event_aggregates(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    precision: int = 1,
    min_quality: float = 0.2,
    db: Session = Depends(get_db),
):
    """Return server-side spatial aggregation bins for map density management."""
    precision = max(0, min(precision, 2))

    events = (
        db.query(Event.latitude, Event.longitude, Event.risk)
        .filter(
            Event.latitude.between(min_lat, max_lat),
            Event.longitude.between(min_lon, max_lon),
            Event.quality_score >= max(0.0, min(min_quality, 1.0)),
        )
        .all()
    )

    bins: Dict[str, Dict[str, float]] = {}
    for lat, lon, risk in events:
        if lat is None or lon is None:
            continue
        key_lat = round(float(lat), precision)
        key_lon = round(float(lon), precision)
        key = f"{key_lat}:{key_lon}"
        if key not in bins:
            bins[key] = {
                "count": 0,
                "risk_sum": 0.0,
                "risk_max": 0.0,
                "lat": key_lat,
                "lon": key_lon,
            }
        bins[key]["count"] += 1
        bins[key]["risk_sum"] += float(risk or 0.0)
        bins[key]["risk_max"] = max(bins[key]["risk_max"], float(risk or 0.0))

    cells = []
    for key, value in bins.items():
        count = int(value["count"])
        cells.append(
            AggregateCell(
                cell_id=key,
                center_lat=float(value["lat"]),
                center_lon=float(value["lon"]),
                event_count=count,
                avg_risk=round(value["risk_sum"] / count if count else 0.0, 3),
                max_risk=round(value["risk_max"], 3),
            )
        )
    cells.sort(key=lambda c: c.event_count, reverse=True)
    return cells[:450]


@app.post("/ai/brief", response_model=AIBriefResponse)
def get_ai_brief(payload: AIBriefRequest, db: Session = Depends(get_db)):
    """Generate an operational brief from recent events, using Groq when configured."""
    limit = max(30, min(payload.limit, 320))
    events = db.query(Event).order_by(desc(Event.timestamp)).limit(limit).all()

    focus_event = None
    if payload.selected_event_id:
        focus_event = db.query(Event).filter(Event.id == payload.selected_event_id).first()

    groq_result = call_groq_brief(events, focus_event)
    if groq_result:
        return AIBriefResponse(
            brief=groq_result["brief"],
            generated_at=datetime.utcnow().isoformat(),
            model=groq_result["model"],
            used_fallback=False,
            context_events=len(events),
        )

    fallback = build_fallback_brief(events, focus_event)
    return AIBriefResponse(
        brief=fallback,
        generated_at=datetime.utcnow().isoformat(),
        model="fallback-rule-engine",
        used_fallback=True,
        context_events=len(events),
    )

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await ws.accept()
    logger.info(f"WebSocket client connected: {ws.client}")
    clients.append(ws)
    
    try:
        while True:
            # Keep connection alive
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if ws in clients:
            clients.remove(ws)
        logger.info(f"WebSocket client disconnected: {ws.client}")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "websocket_clients": len(clients),
        "version": "2.0.0"
    }

if __name__ == "__main__":
    logger.info("Starting Enhanced Intel Control Plane...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
