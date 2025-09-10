from dotenv import load_dotenv
import os
from cc_sr.cluster_creation_deletion import ConfluentCLoudManager
from logs.logger import logger
from cc_sr.topic_schema_creation_deletion import ClusterResourceManager
import time
from config.update_env import update_env_file
from data_ingestion.mysql_connection import mysql_connection
import json


def cluster_main():
    load_dotenv()

    cloud_api_key = os.getenv("CLOUD_RESOURCE_USER")
    cloud_api_secret = os.getenv("CLOUD_RESOURCE_SECRET")
    owner_id =  os.getenv("OWNER_ID")
    env_name="development"
    cc_manager=ConfluentCLoudManager(cloud_api_key,cloud_api_secret)

    environment_details=cc_manager.create_enivronment(env_name)
    env_id=next(env["id"] for env in environment_details["data"] if env["display_name"]==env_name)
    logger.info("Development environment id: {}".format(env_id))

    logger.info("********Starting cluster creation*********")

    cluster = cc_manager.create_cluster(env_id=env_id,
                                      cluster_name="mysql-kafka-cluster",
                                      cloud_provider="AWS",
                                      region="ap-south-1",
                                      cluster_type="Standard",
                                      availablity="SINGLE_ZONE")
    bootstrap = cluster["spec"]["kafka_bootstrap_endpoint"]
    logger.info("Bootstrap URI {}".format(bootstrap))
    update_env_file("KAFKA_BOOTSTRAP",bootstrap.split("://")[1])
    update_env_file("KAFKA_MECHANISMS","PLAIN")
    update_env_file("KAFKA_PROTOCOL","SASL_SSL")
    
    cluster_id = cluster["id"]

    logger.info("Cluster id is {}".format(cluster_id))

    if cluster_id is not None:
        if cc_manager.wait_for_cluster_ready(cluster_id=cluster_id,env_id=env_id):
            logger.info("Cluster is now ready for user!!!!\n")

            list_cluster_response = cc_manager.list_of_cluster(env_id=env_id)

            schema_registry = list_cluster_response.get("data",[])

            logger.info(schema_registry)
            
            for sc in schema_registry:
                schema_registry_id = sc.get("id")
                schema_registry_url = sc["spec"]["http_endpoint"]

            print(schema_registry_id)
            print(schema_registry_url) 

            update_env_file("SCHEMA_URL",schema_registry_url)

            logger.info("************Creating API key for cluster************")
            cluster_api_key = cc_manager.generate_api_key(cluster_id,owner_id)
            print("API key: {}".format(cluster_api_key["id"]))
            print("API secret: {}".format(cluster_api_key["spec"]["secret"]))
            update_env_file("KAFKA_USER",cluster_api_key["id"])
            update_env_file("KAFKA_PASSWORD",cluster_api_key["spec"]["secret"])

            logger.info("************Creating Topic in cluster************")

            cluster_base_url = cluster["spec"]["http_endpoint"]

            logger.info("Base url for topic {}".format(cluster_base_url.split(":443")[0]))
            cc_cluster_manager = ClusterResourceManager(cluster_api_key=cluster_api_key["id"],
                                                        cluster_api_secret=cluster_api_key["spec"]["secret"],
                                                        base_url=cluster_base_url.split(":443")[0])
            subject = "product-data-topic"
            topic_response = cc_cluster_manager.create_topic(cluster_id=cluster_id,
                                                             topic_name= subject,
                                                             partitions=6,rep_factor=3)
            logger.info("Topic response: {}".format(topic_response))


            logger.info("************Creating API key for Schema Registry************")

            schema_api_key = cc_manager.generate_api_key(schema_registry_id,owner_id)

            print("API key: {}".format(schema_api_key["id"]))
            print("API secret: {}".format(schema_api_key["spec"]["secret"]))

            update_env_file("SCHEMA_USER",schema_api_key["id"])
            update_env_file("SCHEMA_PASSWORD",schema_api_key["spec"]["secret"])

            logger.info("************Starting schema registry in the topic************")
            avro_schema = mysql_connection.schema_generation(database="ecommerce",table_name="products")

            logger.info("JSON SCHEMA {}".format(json.dumps(avro_schema)))

            time.sleep(120)

            sc_schema_registry = ClusterResourceManager(cluster_api_key=schema_api_key["id"],cluster_api_secret=schema_api_key["spec"]["secret"],
                                                        base_url= schema_registry_url)
            schema_reponse = sc_schema_registry.register_data_contract(subject_name=subject,schema_type="AVRO",schema=json.dumps(avro_schema))
            logger.info("Registered schema response {}".format(schema_reponse))

            return subject

    else:
        logger.info("Cluster creation is failed or timed out!!!!")
        return None
    
    
    

    
# cluster_main()
# if __name__=="__main__":
#     main()




