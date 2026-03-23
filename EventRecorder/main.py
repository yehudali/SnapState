import logging
from fastapi import FastAPI, HTTPException, status, Query
from pydantic import UUID4
from shard.config.configurator_class import Configurator
from shard.kafka_service.kafka_producer import KafkaProducer
from shard.schema.class_schema import IncidentCreate, LocationUpdate, IncidentLocationsResponse
from EventRecorder.dal import ActionsKafka, LocationService
from shard.database.redis_manager import RedisManager

config = Configurator()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Emergency Response Location API",
    description="API for handling incidents and responder locations using Kafka and Redis",
    version="1.0.0"
)
try:
    producer = KafkaProducer(bootstrap_servers = config.BOOTSTRAP_SERVERS,client_id="EventRecorder")
    kafka_action = ActionsKafka(producer)
    redis_con = RedisManager(redis_host = config.REDIS_HOST, redis_port=config.REDIS_PORT)
    location_service = LocationService(redis_con)
except Exception as e:
    logger.critical(f"Failed to initialize infrastructure services: {e}")
    raise


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
def create_event(incident: IncidentCreate):
    """
  קליטת אירוע חדש, ביצוע ולידציה, שליחה לטופיק של קפקא 
  'incidents.created' טופיק
    """
    try:
        kafka_action.insert_incident_to_Kafka(incident)
        return {"status": "accepted", "incident_id": incident.incident_id}
    except Exception as e:
        logger.error(f"Internal error processing event: {e}", exc_info=True)
        # מחזירים 500 למשתמש בלי לחשוף פרטי תשתית
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/locations", status_code=status.HTTP_200_OK)
def update_location(location_data: LocationUpdate):
    """
    מעדכן מיקום של מגיב ב-Redis.
    """
    try:
        location_service.save_responder_location(location_data)
        return {"status": "updated"}
    except Exception as e:
        logger.error(f"Failed to update location in Redis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/locations/{incident_id}", response_model=IncidentLocationsResponse)
def get_incident_locations(incident_id: UUID4):
    """
    שולף את כל מיקומי המגיבים לאירוע נתון.
    """
    try:
        # הפעלת הלוגיקה מול Redis והחזרת מודל Pydantic
        return location_service.get_all_incident_locations(incident_id=incident_id)
    except Exception as e:
        # טיפול שקט בשגיאות תשתית כדי למנוע דליפת מידע החוצה
        logger.error(f"Failed to fetch locations for incident {incident_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")



@app.get("/locations_by_radius/{incident_id}", response_model=IncidentLocationsResponse)
def get_locations(
    incident_id: UUID4,
    lon: float = Query(..., description="Center longitude", ge=-180, le=180),
    lat: float = Query(..., description="Center latitude", ge=-90, le=90),
    radius: float = Query(50.0, description="Search radius in kilometers", gt=0)
):
    """
    אופציונאלי
    שולף מ-Redis את כל המגיבים ברדיוס מסוים.
    """
    try:
        response_data = location_service.get_incident_locations(
            incident_id=incident_id, 
            center_lon=lon, 
            center_lat=lat, 
            radius_km=radius
        )
        return response_data
    except Exception as e:
        logger.error(f"Failed to fetch locations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("EventRecorder.main:app", host="0.0.0.0", port=8000)