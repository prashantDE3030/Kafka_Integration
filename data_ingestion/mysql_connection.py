import mysql.connector as m
from logs.logger import logger
import pandas as pd
from mock_data.product_synthetic_data import GenerateSyntheticData
import json
import os
from dotenv import load_dotenv

load_dotenv()




class MySQLConnection:
    """ This class is used to connect to 
    mysql sandbox instance on ggogle cloud
    """

    def __init__(self,host_name,user,password,port,database):
        """ This function is used initialize the all the details that need to 
        connect to mysql instance"""

        self.host_name = host_name
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.connection = None
    
    def connect(self):

        """Establishing connection to MYSQL database"""

        try:
            self.connection = m.connect(
                host=self.host_name,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )

            if self.connection.is_connected():
                logger.info("Successfully connected to mysql database")
                return True
        except Exception as e:
            logger.info("Error connecting to mysql instance {}".format(e))
            return False
        # finally:
        #     self.connection.close()
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")
    
    
    def get_all_tables(self):
        """ This function will list all table with in the database """
        if not self.connection or not self.connection.is_connected():
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        logger.info("List of tables {}".format(tables))
        cursor.close()
        return tables
    
    def create_table(self,table_script):
        """This function is used to create the product table 
        if table is already present then it call the 
        insert  function to load the data into table"""

        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        try:
            
            cursor = self.connection.cursor()

            #table_name = "products"

                # create_table_statement = f""" CREATE TABLE {table_name} (
                # product_id varchar(20) PRIMARY_KEY,
                # product_name varchar(100) NOT NULL,
                # price DECIMAL(10,2) NOT NULL,
                # category varchar(100) NOT NULL,
                # last_updated_time TIMESTAMP )"""

            cursor.execute(table_script)
            self.connection.commit()
            logger.info("table created successfully")
            return True
        except Exception as e:
            logger.info("Unable to create table {}".format(e))
            return False

    def insert_data(self,table_name,data_frame):

        # mysql_df = data_frame.copy()

        # if id in mysql_df.columns:
        #     mysql_df = mysql_df.drop("id",axis=1)
        # mysql_df = pd.to_datetime(mysql_df["last_updated_time"],format="%Y%m%d%H%M%S%f")

        # total_rows = len(mysql_df)

        # logger.info("Loading {} rows in {} table".format(total_rows,table_name))

        # for i in range(0,total_rows,batch_size):
        #     batch = mysql_df.iloc[i:i+batch_size]

        #     batch.to_sql(name=table_name,con=self.connection,if_exists="append",index=False,method="multi")
        # logger.info("Successfully loaded {} rows in table {}".format(total_rows,table_name))
        # return "loaded"

        if not self.connection or not self.connection.is_connected():
            self.connect()

        cursor = self.connection.cursor()

        product_df = pd.DataFrame(data_frame)

        #print(type(product_df))

        #dt = pd.to_datetime(product_df["last_updated_time"], format="%Y%m%d%H%M%S%f")

        product_df["last_updated_time"] = pd.to_datetime(product_df["last_updated_time"],format="%Y%m%d%H%M%S%f")

        #product_df=pd.DataFrame(product_df)

        #print(product_df.columns)

        columns = ", ".join(product_df.columns)

        logger.info("Dataframe columns {}".format(columns))

        placeholders = ", ".join(["%s"]*len(product_df.columns))

        #logger.info("Place holders {}",format(placeholders))
        #update_part = ", ".join([f"{col}=VALUES({col})" for col in product_df.columns if col != "product_id"])

        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        data_tuples = [tuple(row) for row in product_df.to_numpy()]

        
        cursor.executemany(insert_query,data_tuples)

        self.connection.commit()

        rows_inserted= cursor.rowcount

        logger.info("Successfully loaded {} in table {}".format(rows_inserted,table_name))


        #print(max_date[0].strftime("%y%m%d%H%M%S%f"))
        # with open("Date.txt","w") as f:
        #     f.write(max_date[0].strftime("%y%m%d%H%M%S%f"))
        # logger.info("Successfully written last updated date {} in the file".format(max_date[0].strftime("%y%m%d%H%M%S%f")))
        # f.close()

        return "loaded"
    
    def extract_data(self,table_name):
        """ Select the incremental data from the table based
        on the last updated time"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()

        ## Get last updated time

        with open("Date.txt","r") as f:
            last_date = f.read().strip()
        if not last_date:
            query = f"select * from {table_name} order by last_updated_time"
            cursor.execute(query)
            records=cursor.fetchall()
            logger.info("Total extracted records {}".format(len(records)))
            records_df =pd.DataFrame(records,columns=["product_id","product_name","price","category","last_updated_time"])
            return records_df
        else:        
            select_statement = f"""select * from {table_name} where CAST({last_date} as char) < CAST(date_format(last_updated_time,"%Y%m%d%H%i%S%f") as CHAR) order by last_updated_time"""
            print(select_statement)
            cursor.execute(select_statement)
            records = cursor.fetchall()
            records_df =pd.DataFrame(records,columns=["product_id","product_name","price","category","last_updated_time"])
            return records_df
    

    def update_last_date(self,table_name):
        
        """ This function is used to update the last updated time in 
        Date.txt file"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()

        max_date_query = f"select max(last_updated_time) as last_date from {table_name}"

        cursor.execute(max_date_query)
        max_date=cursor.fetchone()
        with open("Date.txt","w") as f:
            f.write(max_date[0].strftime("%Y%m%d%H%M%S%f"))
        logger.info("Successfully written last updated date {} in the file".format(max_date[0].strftime("%Y%m%d%H%M%S%f")))
        f.close()


    def schema_generation(self,database,table_name):
        """This function is used to generate avro schema for kafka
        for given table"""


        with open("avro_schema/avro_schema.json","r") as sf:
            logger.info("loading avro schema file")
            avro_schema_file = json.load(sf)
        sf.close()

        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()

        query = f"""SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.COLUMNS where table_schema="{database}" and table_name="{table_name}" """

        print(query)

        cursor.execute(query)

        table_schema = cursor.fetchall()

        table_schema_df = pd.DataFrame(table_schema,columns=["column_name","data_type"])

        #kafka_avro_schema
        fields=[]

        for index,row in table_schema_df.iterrows():
            data_value = row.to_dict()
            #print(data_value)
            field_data = {"name": data_value["column_name"],
                          "type": avro_schema_file["avro_schema"][data_value["data_type"].upper()]
                          }
            fields.append(field_data)
        final_avro_schema = {
                             "type": "array",   
                            "items":{
                                "type": "record",
                                "name": database+"_"+table_name,
                                "fields": fields  
                                }
                            }   

        logger.info("Avro Schema: \n {}".format(final_avro_schema))
        return final_avro_schema
            

mysql_connection = MySQLConnection(host_name=os.getenv("MYSQL_HOST"),user=os.getenv("MYSQL_USER"),
                                        password=os.getenv("MYSQL_PASSWORD"),database="ecommerce",
                                        port=os.getenv("MYSQL_PORT"))
        

        



        