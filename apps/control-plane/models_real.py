"""
Enhanced Database Schema for Real Intelligence Data
No hardcoded values - supports dynamic data
"""
from sqlalchemy import Column, String, Float, DateTime, Text, Integer, Boolean, JSON, ForeignKey
from sqlalchemy.orm import relationship
from db.database import Base
from datetime import datetime

class Event(Base):
    __tablename__ = "events"

    id = Column(String, primary_key=True, index=True)
    title = Column(String, index=True)
    description = Column(Text)
    source = Column(String, index=True)
    url = Column(String)
    timestamp = Column(DateTime, index=True)
    latitude = Column(Float, index=True)
    longitude = Column(Float, index=True)
    risk = Column(Float, index=True)
    type = Column(String, index=True)
    severity = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    
    # Enhanced fields for real data
    gdelt_date = Column(String)
    gdelt_language = Column(String)
    processed_at = Column(DateTime)
    location_info = Column(JSON)  # Store detailed location data
    raw_text = Column(Text)  # Original text for analysis
    canonical_type = Column(String, index=True)
    confidence = Column(Float, default=0.5, index=True)
    quality_score = Column(Float, default=0.5, index=True)
    correlation_key = Column(String, index=True)
    fingerprint = Column(String, index=True)
    expires_at = Column(DateTime, index=True)
    
    # Relationships
    entities = relationship("EventEntity", back_populates="event")
    locations = relationship("EventLocation", back_populates="event")

class EventEntity(Base):
    __tablename__ = "event_entities"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, ForeignKey("events.id"))
    entity_type = Column(String)  # person, organization, location, etc.
    entity_name = Column(String)
    confidence = Column(Float)
    extracted_at = Column(DateTime, default=datetime.now)
    
    event = relationship("Event", back_populates="entities")

class EventLocation(Base):
    __tablename__ = "event_locations"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, ForeignKey("events.id"))
    country = Column(String)
    city = Column(String)
    region = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    confidence = Column(Float)
    source = Column(String)  # geocoding service used
    extracted_at = Column(DateTime, default=datetime.now)
    
    event = relationship("Event", back_populates="locations")

class DataSource(Base):
    __tablename__ = "data_sources"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    type = Column(String)  # gdelt, newsapi, manual, etc.
    url = Column(String)
    api_key_required = Column(Boolean, default=False)
    rate_limit = Column(Integer)  # requests per hour
    last_fetch = Column(DateTime)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)

class ProcessingLog(Base):
    __tablename__ = "processing_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, ForeignKey("events.id"))
    processor_type = Column(String)  # nlp, geocoding, risk_analysis
    processing_time = Column(Float)  # seconds
    success = Column(Boolean)
    error_message = Column(Text)
    processed_at = Column(DateTime, default=datetime.now)
