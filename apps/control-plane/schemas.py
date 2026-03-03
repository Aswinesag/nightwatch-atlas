from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class EventCreate(BaseModel):
    id: Optional[str] = None
    title: str
    description: str
    risk: float
    lat: float
    lon: float
    type: str
    severity: str
    created_at: Optional[datetime] = None
