import json
import redis
from pydantic import UUID4
from typing import List, Tuple, Optional, cast
from shard.kafka_service.kafka_producer import KafkaProducer
from shard.schema.class_schema import IncidentCreate, LocationUpdate, IncidentLocationsResponse, ResponderLocation, Coordinates

class ActionsKafka:
    def __init__(self, producer : KafkaProducer):
        self.producer = producer


    def insert_incident_to_Kafka(self ,incident: IncidentCreate):
        try:
            incident_json = incident.model_dump_json()
            self.producer.produce(topic="incidents", kay="incidents.created", value=incident_json)
        except Exception as e:
            print(e)
            raise

    def close_producer(self):
        self.producer.close_kafka_producer()


class LocationService:
    def __init__(self, redis_host: str , redis_port: int):
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            decode_responses=True # מחזיר Strings במקום Bytes
        )
        self.incident_ttl_seconds = 7200
    

    def save_responder_location(self, update_data: LocationUpdate) -> None:

        key = f"incident:{update_data.incident_id}:locations"
        
        with self.redis_client.pipeline() as pipe:
            # redis-py geoadd syntax: geoadd(name, values) where values is tuple/list of (lon, lat, member)
            values = (update_data.coordinates.lon, update_data.coordinates.lat, update_data.responder_id)
            
            pipe.geoadd(key, values) 
            pipe.expire(key, self.incident_ttl_seconds)
            pipe.execute()



    def get_incident_locations(self, incident_id: UUID4, center_lon: float, center_lat: float, radius_km: float = 50.0) -> IncidentLocationsResponse:
        """
        אופציונאלי: שולף את כל המגיבים ברדיוס מסוים מנקודת האירוע 
        """
        key = f"incident:{incident_id}:locations"
        
        results = self.redis_client.geosearch(
            name=key,
            longitude=center_lon,
            latitude=center_lat,
            radius=radius_km,
            unit='km',
            withdist=True,  # קבל את המרחק מהמרכז
            withcoord=True  # קבלת הקואורדינטות המדויקות
        )
        
        # בניית אובייקטי Pydantic מהתוצאות של Redis
        responders = []
        for member, distance, (lon, lat) in results: # type: ignore
            responders.append(
                ResponderLocation(
                    responder_id=member,
                    # distance_meters=distance * 1000, # המרה למטרים
                    coordinates=Coordinates(lat=lat, lon=lon)
                )
            )
            
        return IncidentLocationsResponse(
            incident_id=incident_id,
            active_responders=responders
        )




    def get_all_incident_locations(self, incident_id: UUID4) -> IncidentLocationsResponse:
        """
        שולף את כל המגיבים שמשויכים לאירוע מתוך Redis, ללא סינון מרחבי.
        """
        key = f"incident:{incident_id}:locations"
        
        # 1. שליפת המזהים וביצוע Cast לרשימת מחרוזות
        raw_members = self.redis_client.zrange(key, 0, -1)
        members = cast(List[str], raw_members)
        
        if not members:
            return IncidentLocationsResponse(incident_id=incident_id, active_responders=[])
            
        # 2. שליפת הקואורדינטות וביצוע Cast לרשימה של טאפלים (או None אם המשתמש נמחק)
        raw_positions = self.redis_client.geopos(key, *members)
        positions = cast(List[Optional[Tuple[float, float]]], raw_positions)
        
        responders = []
        # עכשיו zip מקבל שתי רשימות תקניות וה-Linter שותק
        for member, pos in zip(members, positions):
            if pos: 
                lon, lat = pos
                responders.append(
                    ResponderLocation(
                        responder_id=member,
                        # distance_meters=0.0, 
                        coordinates=Coordinates(lat=lat, lon=lon)
                    )
                )
                
        return IncidentLocationsResponse(
            incident_id=incident_id,
            active_responders=responders
        )