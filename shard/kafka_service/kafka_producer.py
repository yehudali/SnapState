from confluent_kafka import Producer , KafkaException




class KafkaProducer:
    def __init__(self, bootstrap_servers:str, client_id:str) -> None:
        self.conf = {
                'bootstrap.servers': bootstrap_servers, 
                'client.id':client_id
                }
        
        try:
            self.producer :Producer = Producer(self.conf)
            print("INFO", "kafka DEBUG create")
        except KafkaException as e:
            print("ERROR", f"failed to create kafka producer: {e}")
            raise

    def kafka_colbak(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))
    
    

        
    def produce(self, topic:str, kay:str, value:str):
        try:
            byts_kay = kay.encode()
            byts_value = value.encode()

            self.producer.produce(
                topic=topic,
                key=byts_kay,
                value= byts_value,
                callback=self.kafka_colbak
            )
            self.producer.poll(0)

        except Exception as e:
            print("ERROR", "Failed to send message to Kafka... error: {e}")

            
    def close_kafka_producer(self):
        self.producer.flush()
        print("INFO", "close_kafka_producer!")
