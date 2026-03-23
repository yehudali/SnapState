import os


class Configurator:
    def __init__(self) -> None:
        ## kafka:
        self.BOOTSTRAP_SERVERS:str = os.getenv("BOOTSTRAP_SERVERS","localhost:9092")
        self.REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


        self.validate()

    def validate(self):
        if not self.BOOTSTRAP_SERVERS:
            print("i not hav a BOOTSTRAP_SERVERS env!")
            raise
        if not self.REDIS_HOST:
            print("i not hav a REDIS_HOST env!")
            raise
        if not self.REDIS_PORT:
            print("i not hav a REDIS_PORT env!")
            raise
        
        