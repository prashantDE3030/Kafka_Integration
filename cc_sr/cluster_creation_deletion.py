import requests
import base64
from logs.logger import logger
import time

class ConfluentCLoudManager:
    def __init__(self,api_key,api_secret):
        """
        ARGS:
        here api_key and api_secret will
        be the credentials of your confluent
        cloud resource manager. You need
        to generate the key for your account.   
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.confluent.cloud"
        self.auth_header = self.__get_auth_header()
    

    def __get_auth_header(self):
        """ Create basic auth header of API request.
        As per the confluent API document the keys must be in
        base64 encoded form"""
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded_credentials =  base64.b64encode(credentials.encode()).decode()
        return f"BASIC {encoded_credentials}"
    
    def create_enivronment(self,env_name):
        """
        This function is responsible for creating
        environment in the confluent cloud
        """

        complete_url = f"{self.base_url}/org/v2/environments"
        #print(complete_url)

        request_header = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json" 
        }

        #print(request_header)

        request_body = {
            "display_name": env_name
        }


        api_response = requests.post(url=complete_url, headers=request_header, json= request_body)

        if api_response.status_code == 201:
            logger.info("Environment {} created successfully".format(env_name))
            env_list_response=self.__list_environment()
            return env_list_response

        else:
            env_list_response=self.__list_environment()
            if env_list_response is None:
                logger.info("Failed to create environment,error_code: {} - error_message {}".format(api_response.status_code,api_response.text))
                return None
            return env_list_response
            
        
    

    def __list_environment(self):
        """ This function is used to list
        all the available environment available in your
        confluent cloud
        """

        complete_url=f"{self.base_url}/org/v2/environments"

        request_header = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }

        env_response = requests.get(url=complete_url,headers=request_header)

        if env_response.status_code == 200:
            logger.info("Successfully fetched the environment details {}".format(env_response))
            return env_response.json()
        else:
            logger.info("Failed to list environment {} - {}".format(env_response.status_code,env_response.text))
            return None
    
    def create_cluster(self,env_id,cluster_name,cloud_provider,region,cluster_type,availablity):
        """This function take the necessary configuration as arguments and create
        cluster based on the configuration provided in confluent cloud"""

        cluster_url =  f"{self.base_url}/cmk/v2/clusters"

        request_header = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }

        request_body= {
            "spec": {
                "display_name": cluster_name,
                "availability": availablity,
                "cloud": cloud_provider,
                "region": region,
                "config":
                {
                    "kind": cluster_type
                },
                "environment":
                {
                    "id": env_id
                }
            }
        }

        ## Call the cluster API

        cluster_api_response = requests.post(url=cluster_url,headers=request_header,json=request_body)

        if cluster_api_response.status_code == 202:
            cluster_data = cluster_api_response.json()
            logger.info("Kafka cluster {} cluster initiated".format(cluster_name))
            logger.info("Cluster ID {}".format(cluster_data["id"]))
            return cluster_data
        else:
            logger.info("Failed to create cluster: {} - {}".format(cluster_api_response.status_code,cluster_api_response.text))
            return None
    
    def __get_cluster_status(self,cluster_id,env_id):
        """ Get the status of the cluster
        Args:
        cluster id: id of the cluster for which you need status"""

        status_url = f"{self.base_url}/cmk/v2/clusters/{cluster_id}?environment={env_id}"

        request_header = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }

        status_api_response = requests.get(url=status_url,headers=request_header)

        if status_api_response.status_code == 200:
            logger.info("Successfully fetched the cluster status {}".format(status_api_response.json()))
            return status_api_response.json()
        else:
            logger.info("Unable to fetch the cluster status {} - {}".format(status_api_response.status_code,status_api_response.text))
            return None
    
    def wait_for_cluster_ready(self,cluster_id,env_id,timeout=600):
        """
        Wait for cluster to be in PROVISIONED state.
        
        Args:
            cluster_id: ID of the cluster to monitor
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if cluster is ready, False if timeout
        """

        logger.info("Waiting for cluster to be provisioned..")

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                cluster_info = self.__get_cluster_status(cluster_id,env_id)
                status = cluster_info.get('status',{}).get('phase','UNKNOWN')
                logger.info("Current status: {}".format(status))

                if status == "PROVISIONED":
                    logger.info("Cluster Ready!!!")
                    return True
                elif status in ["FAILED","DELETING"]:
                    logger.info("Cluster creation failed with status {}".format(status))
                    return False
            except Exception as e:
                logger.info("Failed to check the status: {}".format(e))
                time.sleep(30)
        logger.info("Timeout waiting for cluster to be ready")
        return False
    
    def generate_api_key(self,resource_id,owner_id,description="mysql-api-key"):
        """
        Create an API key for the Kafka cluster.
        
        Args:
            cluster_id: ID of the cluster
            description: Description for the API key
            
        Returns:
           API key details
        """

        api_url = f"{self.base_url}/iam/v2/api-keys"

        request_header = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }

        request_body = {
            "spec": {
                "display_name": description,
                "description": description,
                "owner": {
                    "id": owner_id
                },
                "resource": {
                    "id": resource_id
                }

            }

        }

        api_key_response = requests.post(url=api_url,headers=request_header,json=request_body)

        if api_key_response.status_code == 202:
            api_key_data =  api_key_response.json()
            logger.info("API key created {}".format(api_key_data['id']))
            return api_key_data
        else:
            logger.info("Failed to create API key {} - {}".format(api_key_response.status_code,api_key_response.text))
            return None
    

    def read_cluster_schema_registry(self,cluster_id,env_id):
        """ The function used the read the cluster configuration 
        with in a evironment"""

        complete_url = f"{self.base_url}/srcm/v3/clusters/{cluster_id}?environment={env_id}"

        request_header = {
            "AUTHORIZATION": self.auth_header,
            "Content-Type": "application/json"
        }

        cluster_response = requests.get(url=complete_url,headers=request_header)

        return cluster_response.json()
    
    def list_of_cluster(self,env_id):
        """ This function is used to list all
        the cluster with in the environment"""

        complete_url = f"{self.base_url}/srcm/v3/clusters?environment={env_id}"

        request_header = {
            "AUTHORIZATION": self.auth_header,
        }

        cluster_list_response = requests.get(url=complete_url,headers=request_header)
        return cluster_list_response.json()



