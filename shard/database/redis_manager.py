import redis

class RedisManager:
    def __init__(self, redis_host: str , redis_port: int):
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            decode_responses=True #!מחזיר Strings 
        )
        
    