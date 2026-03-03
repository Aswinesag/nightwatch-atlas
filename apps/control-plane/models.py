from sqlalchemy import Column, String, Float, DateTime, Text, Integer
from db.database import Base
from datetime import datetime

class Event(Base):
    __tablename__ = "events"

    id = Column(String, primary_key=True, index=True)
    title = Column(String)
    description = Column(Text)
    risk = Column(Float)
    lat = Column(Float)
    lon = Column(Float)
    type = Column(String)
    severity = Column(String)
    created_at = Column(DateTime, default=datetime.now)