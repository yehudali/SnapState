from pydantic import BaseModel, Field, UUID4, field_validator
from datetime import datetime
from typing import List

# --- Base Models ---
class Coordinates(BaseModel):
    lat: float = Field(..., ge=-90.0, le=90.0, description="Latitude must be between -90 and 90")
    lon: float = Field(..., ge=-180.0, le=180.0, description="Longitude must be between -180 and 180")

# 1.
# --- Ingestion (Kafka Producer / HTTP POST) ---
class IncidentCreate(BaseModel):
    Incident_id: UUID4 = Field(default_factory=UUID4, description="Unique identifier for the incident")
    creator_id: str =  Field(...) # (min_length=3, max_length=50)
    Incident_level: str
    location: Coordinates
    description: str = Field(max_length=500)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_payload(self) -> str:
        return self.model_dump_json()
    
# 2.
# --- State Management (Redis Ingestion / HTTP POST) ---
class LocationUpdate(BaseModel):
    responder_id: str = Field(..., min_length=3, max_length=50)
    coordinates: Coordinates

# 3.
# --- Client Retrieval (HTTP GET Response) ---

class ResponderLocation(BaseModel):
    responder_id: str
    distance_meters: float = Field(..., ge=0)
    coordinates: Coordinates

class IncidentLocationsResponse(BaseModel):
    active_responders: List[ResponderLocation]