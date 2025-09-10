from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from logs.logger import logger
from dotenv import load_dotenv
import os
from data_ingestion.mysql_connection import mysql_connection
import json
import time

load_dotenv()

kafka_config = {
"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
"sasl.mechanisms": os.getenv("KAFKA_MECHANISMS"),
"security.protocol": os.getenv("KAFKA_PROTOCOL"),
"sasl.username": os.getenv("KAFKA_USER"),
 "sasl.password": os.getenv("KAFKA_PASSWORD") 
}

def get_schema_registry_client(): 
    return SchemaRegistryClient({"url": os.getenv("SCHEMA_URL"),"basic.auth.user.info": "{}:{}".format(os.getenv("SCHEMA_USER"),os.getenv("SCHEMA_PASSWORD"))})


def delivery_report(err,msg):
    """ Report the success or failure 
       of kafka message"""
    if err is not None:
        logger.info("Delivery failed for the record {} : {}".format(msg.key(),err))
        return
    else:
        print("Record {} successfully produced to {} [{}] at offset {}".format(msg.key(),msg.topic(),msg.partition(),msg.offset()))
        print("**********************************")

def produce_data_on_topic(subject):


    key_serializer = StringSerializer("utf-8")
        # mysql_connection = MySQLConnection(host_name=os.getenv("MYSQL_HOST"),user=os.getenv("MYSQL_USER"),
        #                                 password=os.getenv("MYSQL_PASSWORD"),database="ecommerce",
        #                                 port=os.getenv("MYSQL_PORT"))

    success_df = mysql_connection.extract_data(table_name="products")

    print(success_df)

    #mysql_connection.update_last_date(table_name="products")

    product_producer = SerializingProducer({**kafka_config,
                                                "key.serializer": key_serializer,
                                                "value.serializer": get_latest_schema(subject=subject)
                                                })
    
    batch_size = 10000
    data_list = success_df.to_dict(orient="records")
    for data_value in data_list:
        data_value["price"] = str(data_value["price"])
        data_value["last_updated_time"] = str(data_value["last_updated_time"])
    for i in range(0,len(data_list),batch_size):
        batch = data_list[i:i+batch_size]
        batch_key = f"batch_{i//batch_size}"
        product_producer.produce(
            topic=subject,
            key=str(batch_key),
            value= batch,
            on_delivery= delivery_report
        )
        product_producer.flush()
        time.sleep(0.4)
    logger.info("All data published to kafka topic")


def get_latest_schema(subject):
    client = get_schema_registry_client()
    schema = client.get_latest_version(f"{subject}-value").schema.schema_str
    return AvroSerializer(client,schema)


produce_data_on_topic("product-data-topic")