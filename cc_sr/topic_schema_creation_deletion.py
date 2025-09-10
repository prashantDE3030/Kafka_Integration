import requests
import base64
from logs.logger import logger
import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

class ClusterResourceManager:
    """
    This class is used to create/delete the cluster resources like
    topic, schema etc.

    ARGS:
     cluster_api_key: API key for the cluster
     
     Cluster_api_secret: API secret to access the cluster 
    """

    def __init__(self,cluster_api_key,cluster_api_secret,base_url):
        """
        ARGS:
        here cluster_api_key and cluster_api_secret will
        be the credentials of your kafka cluster. You need
        to generate the key for your cluster.   
        """
        
        self.cluster_api_key = cluster_api_key
        self.cluster_api_secret = cluster_api_secret
        self.base_url = base_url
        self.auth_header = self.__get_auth_header()

    def __get_auth_header(self):
        """ The funcion create the basic auth for the api call
            in the base64 encode format"""

        credentails = f"{self.cluster_api_key}:{self.cluster_api_secret}"

        encoded_credentails = base64.b64encode(credentails.encode()).decode()

        return F"BASIC {encoded_credentails}"
    

    def create_topic(self,cluster_id,topic_name,partitions,rep_factor):
        """ This function is used to create the topic
        ARGS:
        cluster_id: id of cluster
        topic_name: name of the topic that need to be created
        partitions: number of partition
        rep_factor: number of replication that needs to be created"""

        complete_url = f"{self.base_url}/kafka/v3/clusters/{cluster_id}/topics"

        logger.info("Complete url {}".format(complete_url))

        request_header = {
            "AUTHORIZATION": self.auth_header,
            "Content-Type": "application/json"
        }

        request_body = {
            "topic_name": topic_name,
            "partitions_count": partitions,
            "replication_factor": rep_factor
        }

        topic_api_reponse = requests.post(url=complete_url,headers=request_header,json=request_body)

        if topic_api_reponse.status_code == 201:
            logger.info("Topic create successfully")
            return topic_api_reponse.json()
        else:
            logger.info("Failed to create topic {} - {}".format(topic_api_reponse.status_code,topic_api_reponse.text))
            return None
    

    def register_data_contract(self,subject_name,schema_type,schema):
        """ This function is used to register schema under the specified subject
            
            ARGS:
                    subject_name: name of the topic under which schema needs to be regsitered
                    schema: schema details
        """

        complete_url = f"{self.base_url}/subjects/{subject_name}-value/versions"

        #print(complete_url)

        #auth = (self.cluster_api_key,self.cluster_api_secret)

        #print(auth)

        logger.info("Complete url {}".format(complete_url))

        if schema_type == "AVRO":
            schema_str = json.dumps(json.loads(schema))
        else:
            schema_str = schema if isinstance(schema, str) else json.dumps(json.loads(schema))

        request_header = {
            "Authorization": self.auth_header,
            "Content-Type": "application/vnd.schemaregistry.v1+json"
        }        
        

        request_body = {
            "schemaType": schema_type,
            "schema": schema_str
            
        }

        schema_response = requests.post(url=complete_url,headers=request_header,json=request_body)

        if schema_response.status_code == 200:
            logger.info("Schema successfully registered in the {}".format(subject_name))
            return schema_response.json()
        
        else:
            logger.info("Failed to register schema {} - {}".format(schema_response.status_code,schema_response.text))
            return None
#         schema_registry_conf = {
#             'url': self.base_url,  # e.g. 'https://<region>.azure.confluent.cloud'
#             'basic.auth.user.info': f"{self.cluster_api_key}:{self.cluster_api_secret}"
#         }

#         print(schema_registry_conf["url"])
#         print(schema_registry_conf["basic.auth.user.info"])
# # Initialize the client
#         client = SchemaRegistryClient(schema_registry_conf)

#         # Define your Avro/JSON/Protobuf schema as a string
#         # Choose the schema type: 'AVRO', 'JSON', or 'PROTOBUF'
#         schema = Schema(schema, schema_type)

#         # Use '<topic>-value' as the subject for value schema, or '<topic>-key' for key schema
#         subject = f"{subject_name}-value"

#         # Register the schema
#         schema_id = client.register_schema(subject, schema)

#         if schema_id :
#             logger.info("Schema registered successfully under {}".format(subject_name))
#             return schema_id
#         else:
#             logger.info("failed to register schema under {}"
#                         .format(subject))
#             return None

# avro_schema = """
# {
# "type": "record",
# "name": "MyRecord",
# "fields": [
# {"name": "name", "type": "string"}
# ]
#  }
# """

# sc_schema_registry = ClusterResourceManager(cluster_api_key="TBK6DZY3G6XULBP5",cluster_api_secret="cfltBE3MelxGJ7vzk2F5COfGq/uAMJdAv7sH3RqpzSQTwIWy6kP0bdoUIxVt+JoQ",
#                                                         base_url= "https://psrc-13gydo7.ap-south-1.aws.confluent.cloud")
# schema_reponse = sc_schema_registry.register_data_contract(subject_name="product-data-topic",schema_type="AVRO",schema=avro_schema)