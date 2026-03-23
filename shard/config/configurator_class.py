import os


class Configurator:
    def __init__(self) -> None:
        ## kafka:
        self.BOOTSTRAP_SERVERS:str = os.getenv("BOOTSTRAP_SERVERS","localhost:9092")

        self.validate()

    def validate(self):
        if not self.BOOTSTRAP_SERVERS:
            print("i not hav a BOOTSTRAP_SERVERS env!")
            raise
        