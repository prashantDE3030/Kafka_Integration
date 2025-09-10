from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from logs.logger import logger
from dotenv import load_dotenv
import os
from stage import generate_final_file
import time
import json
load_dotenv()
class DataConsumer:

    def __init__(self,subject):
        

        self.subject_name = subject

        self.schema_registry_client = SchemaRegistryClient({
        "url": os.getenv("SCHEMA_URL"),
        "basic.auth.user.info": "{}:{}".format(os.getenv("SCHEMA_USER"),os.getenv("SCHEMA_PASSWORD"))
        })
        
        self.kafka_config = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
            "sasl.mechanisms": os.getenv("KAFKA_MECHANISMS"),
            "security.protocol": os.getenv("KAFKA_PROTOCOL"),
            "sasl.username": os.getenv("KAFKA_USER"),
            "sasl.password": os.getenv("KAFKA_PASSWORD"),
            "key.deserializer": StringDeserializer("utf-8"),
            "value.deserializer": self.__get_latest_schema(),
            "group.id": "G1",
            "auto.offset.reset": "earliest" 

        }

    def consumer_data_from_topic(self):

        product_consumer = DeserializingConsumer({**self.kafka_config})

        product_consumer.subscribe([self.subject_name])
        try:
            while True:
                msg = product_consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    logger.info("Consumer error: {}".format(msg.error()))
                    continue
            
                logger.info("Successfully consumed record from partition {} and offset {}".
                            format(msg.partition(),msg.offset()))
                
                logger.info("Key {} and value {}".format(msg.key(),msg.value()))
                logger.info("******************************************")

                stage_dir = "stage"
                stage_data = "stage.json"
                if not os.path.exists(stage_dir):
                    os.makedirs(stage_dir)
                    os.path.join(stage_dir,stage_data)
                    with open(os.path.join(stage_dir,stage_data),"a") as ap:
                        json_str=json.dumps(msg.value(),indent=None)
                        ap.write(json_str+"\n")
                else:
                    with open(os.path.join(stage_dir,stage_data),"a") as ap:
                       json_str=json.dumps(msg.value(),indent=None)
                       ap.write(json_str+"\n")
            #logger.info("Data written to the file")

        except KeyboardInterrupt:
            pass
        finally:
            logger.info("Data written to the file")
            generate_final_file.generate_final_file_func()
            product_consumer.close()


    def __get_latest_schema(self):
        schema = self.schema_registry_client.get_latest_version(f"{self.subject_name}-value").schema.schema_str
        return AvroDeserializer(self.schema_registry_client,schema) 


# kafka_consumer = DataConsumer("product-data-topic")


# kafka_consumer.consumer_data_from_topic()