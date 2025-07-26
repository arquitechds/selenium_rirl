import pymysql
import pandas as pd
import os 
from loguru import logger
from dotenv import load_dotenv
import boto3
import requests
import re 

load_dotenv()

# Canonic DB functions

def get_retail_db_connection():
    ''' 
    Connect to any mysql db
    '''

    retrieve_db = pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        #db=os.environ["DB_NAME"]
    )
    logger.info('Succesful connection')
    return retrieve_db


def insert_data(data,table_name):
    ''' 
    Insert data to any mysql table
        inputs 
            data:(list of dicts) with data to insert
            table_name(str) table name
    '''
    if len(data) == 0:
        logger.info('no data')
        return 
    writer = get_retail_db_connection()
    writer.begin()
    try:
        with writer.cursor() as insert_cursor:
            
            sqlcmd = (
            f"REPLACE INTO {table_name} ({', '.join(data[0].keys())}) "
            f"VALUES (%({')s, %('.join(data[0].keys())})s)")
            insert_cursor.executemany(sqlcmd, data)
            inserted = insert_cursor.rowcount
    except:
        writer.rollback()
        logger.info('\aScores table insert failed. Transaction rolled back')
        raise
    writer.commit()
    logger.info('Inserted', inserted, 'row(s) into Scores table')
    writer.close()


def read_table(query):
    conn = get_retail_db_connection()
    df = pd.read_sql(query,conn)
    return df


# Function to write to S3:

def send_to_s3(name, content):
    s3 = boto3.resource(
        's3',
        region_name='us-east-2',
        aws_access_key_id= os.environ["AWS_ACCESS"],
        aws_secret_access_key=os.environ["AWS_SECRET"]
    )
    s3.Object('rirl-documents', name).put(Body=content)
    logger.info('inserted file to S3')

def send_to_s3_from_local(name, directory):
    s3 = boto3.resource(
        's3',
        region_name='us-east-2',
        aws_access_key_id= os.environ["AWS_ACCESS"],
        aws_secret_access_key=os.environ["AWS_SECRET"]
    )
    s3.Bucket('rirl-documents').upload_file(directory, name)
    logger.info('inserted file to S3')