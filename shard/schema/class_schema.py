from pydantic import BaseModel, Field, UUID4, field_validator,EmailStr
from datetime import datetime
from typing import List
import uuid
from shard.utils.get_utc import get_utc_now

# --- Base Models ---
class Coordinates(BaseModel):
    lat: float = Field(..., ge=-90.0, le=90.0, description="Latitude must be between -90 and 90")
    lon: float = Field(..., ge=-180.0, le=180.0, description="Longitude must be between -180 and 180")

# 1.
# --- Ingestion (Kafka Producer / HTTP POST) ---
class IncidentCreate(BaseModel):
    incident_id: UUID4 = Field(default_factory=uuid.uuid4, description="Unique identifier for the incident")
    created_at: datetime = Field(default_factory=get_utc_now)
    incident_level: str
    location: Coordinates
    description: str = Field(max_length=500)
    
# 2.
# --- State Management (Redis Ingestion / HTTP POST) ---

class User(BaseModel):
    personal_number: str
    first_name: str
    last_name: str
    phone_number: str
    email: EmailStr
    
class LocationUpdate(BaseModel):
    incident_id: UUID4
    responder_id: str = Field(..., min_length=3, max_length=50)
    coordinates: Coordinates
    timestamp: datetime = Field(default_factory=get_utc_now)

# 3.
# --- Client Retrieval (HTTP GET Response) ---

class ResponderLocation(BaseModel):
    responder_id: str
    distance_meters: float = Field(0.0, ge=0)
    coordinates: Coordinates

class IncidentLocationsResponse(BaseModel):
    incident_id: UUID4
    active_responders: List[ResponderLocation]