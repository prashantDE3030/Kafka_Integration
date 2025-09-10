from producer_consumer.data_producer import produce_data_on_topic
from producer_consumer.data_consumer import DataConsumer
from data_ingestion.data_ingestion_main import data_ingestion_main

data_ingestion_main()

produce_data_on_topic("product-data-topic")

consumer = DataConsumer("product-data-topic")

consumer.consumer_data_from_topic()

