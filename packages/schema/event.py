from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class Location(BaseModel):
    lat: float
    lon: float
    name: Optional[str]

class Entity(BaseModel):
    name: str
    type: str
    confidence: float

class Event(BaseModel):
    id: str
    title: str
    description: str
    timestamp: datetime
    source: str
    location: Optional[Location]
    entities: List[Entity]
    risk_score: float = 0.0