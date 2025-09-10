from data_ingestion.mysql_connection import mysql_connection


def table_create(table_name):
    create_table_dml =f""" CREATE TABLE {table_name} (    
                            product_id varchar(50),
                            product_name varchar(100) NOT NULL,
                            price DECIMAL(10,2) NOT NULL,
                            category varchar(100) NOT NULL,
                            last_updated_time DATETIME(6),
                            PRIMARY KEY (product_id) );"""
    created = mysql_connection.create_table(table_script=create_table_dml)
    return created