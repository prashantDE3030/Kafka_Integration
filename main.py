from cc_sr.cluster_main import cluster_main
from data_ingestion.create_table import table_create
# from producer_consumer.data_producer import produce_data_on_topic
# from producer_consumer.data_consumer import DataConsumer
from logs.logger import logger
from stage import generate_final_file

import time

def main():

    
    table_name="products"
    table_create(table_name=table_name)

    topic_name=cluster_main()

    logger.info("Topic name {}".format(topic_name))

    if topic_name is None:
        logger.info("Cluster creation failed. Please check")
    
    # # else:      
    # #     time.sleep(120)

    #     # produce_data_on_topic("product-data-topic")

    #     # consumer = DataConsumer("product-data-topic")

    #     # consumer.consumer_data_from_topic()

    #     # generate_final_file.generate_final_file_func()

if __name__=="__main__":
    main()
