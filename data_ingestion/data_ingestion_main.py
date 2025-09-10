from dotenv import load_dotenv
from data_ingestion.mysql_connection import mysql_connection
from data_ingestion.create_table import table_create
import os
from mock_data.product_synthetic_data import GenerateSyntheticData
from logs.logger import logger


def data_ingestion_main():
    # load_dotenv()

    # MYSQL_HOST = os.getenv("MYSQL_HOST")
    # MYSQL_PORT = os.getenv("MYSQL_PORT")
    # MYSQL_USER = os.getenv("MYSQL_USER")
    # MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

    # mysql=MySQLConnection(host_name=MYSQL_HOST,user=MYSQL_USER,password=MYSQL_PASSWORD,port=MYSQL_PORT,database="ecommerce")
    connect=mysql_connection.connect()
    if connect:
        data_frame= GenerateSyntheticData.generate_product_data(records_num=40000)
        logger.info("Type of data frame {}".format(type(data_frame)))
        logger.info("Data frame \n {}".format(data_frame))
        table_name="products"
        table_list = mysql_connection.get_all_tables()

        if table_name in table_list:
            mysql_connection.insert_data(table_name,data_frame)
    
        else:        
            created = table_create(table_name=table_name)
            if created:
                inserted = mysql_connection.insert_data(table_name=table_name,data_frame=data_frame)
                logger.info("Data {} successfully".format(inserted))
            return True
    else:
        logger.info("Unable to connect to mysql. Please check if the instance is running or not!!!!")
        return False

# data_ingestion_main()