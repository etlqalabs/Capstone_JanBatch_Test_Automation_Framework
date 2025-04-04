#1. implement execption handling for this file
import os.path

import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging
from Configuration.config import *

logging.basicConfig(
    filename='Logs/Extractprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}').connect()

oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}').connect()

def verify_expected_as_file_to_actual_as_db(file_path,file_type,db_engine,table_name):
    if file_type =='csv':
        df_expected = pd.read_csv(file_path)
    elif file_type == 'json':
        df_expected = pd.read_json(file_path)
    elif file_type == 'xml':
        df_expected = pd.read_xml(file_path,xpath=".//item")
    else:
        raise ValueError(f"unsupported file type passed {file_type}")
    logger.info(f"The expected data is the file is: {df_expected}")
    query_actual = f"select * from {table_name}"
    df_actual = pd.read_sql(query_actual, mysql_engine)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected data in {file_path} does not match with expected data in{table_name}"

# this fucntion will accept a table name directly
def verify_expected_as_db_to_actual_as_db_ext(db_engine1,table_name1,db_engine2,table_name2):
    query_expected = f"select * from {table_name1}"
    df_expected = pd.read_sql(query_expected, db_engine1)
    logger.info(f"The expected data is the database is: {df_expected}")
    query_actual = f"select * from {table_name2}"
    df_actual = pd.read_sql(query_actual, db_engine2)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected data in {table_name1} does not match with expected data in{table_name2}"


# this fucntion will accept a query  directly
def verify_expected_as_db_to_actual_as_db(db_engine_expected,query_expected,db_engine_actual,query_actual):

    # Below line of code was causing error
    # df_expected = pd.read_sql(db_engine_expected, db_engine_expected)

    df_expected = pd.read_sql(query_expected, db_engine_expected)
    logger.info(f"The expected data is the database is: {df_expected}")
    df_actual = pd.read_sql(query_actual, db_engine_actual)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected data in {query_expected} does not match with expected data in{query_actual}"


def verify_expected_as_file_to_actual_as_db(file_path,file_type,db_engine,table_name):
    if file_type =='csv':
        df_expected = pd.read_csv(file_path)
    elif file_type == 'json':
        df_expected = pd.read_json(file_path)
    elif file_type == 'xml':
        df_expected = pd.read_xml(file_path,xpath=".//item")
    else:
        raise ValueError(f"unsupported file type passed {file_type}")
    logger.info(f"The expected data is the file is: {df_expected}")
    query_actual = f"select * from {table_name}"
    df_actual = pd.read_sql(query_actual, mysql_engine)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected data in {file_path} does not match with expected data in{table_name}"


# utility function for file exists
def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File :{file_path}  does not exist {e}")


# Chck if the files are not zero byte
def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) != 0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File :{file_path} is zero byte file {e}")


# Check if any duplicates in the files across the file
def check_for_duplicates_across_the_columns(file_path,file_type):
    try:
        if file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xml':
            df = pd.read_xml(file_path,xpath="//item")
        else:
            raise ValueError(f"Unsupported file type passed {file_type}")
        logger.info(f"The expected data is: {df}")

        if df.duplicated().any():
            return False
        else:
            return True

    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")

# Check if any duplicates in the files for a specific column

def check_for_duplicates_for_specific_column(file_path,file_type,column_name):
    try:
        if file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xml':
            df = pd.read_xml(file_path,xpath="//item")
        else:
            raise ValueError(f"Unsupported file type passed {file_type}")
        logger.info(f"The expected data is: {df}")

        if df[column_name].duplicated().any():
            return False
        else:
            return True

    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")


# check if there are missing values in the file across the columns
def check_for_null_values(file_path,file_type):
    try:
        if file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xml':
            df = pd.read_xml(file_path,xpath="//item")
        else:
            raise ValueError(f"Unsupported file type passed {file_type}")
        logger.info(f"The expected data is: {df}")

        if df.isnull().values.any():
            return False
        else:
            return True

    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")


def getDataFromLinuxBox():
    try:
        logger.info("Linux  connection is being establish")
        # connect to ssh
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        # to conenct to linux server
        ssh_client.connect(hostname,username=username,password=password)
        sftp = ssh_client.open_sftp()
        # download the file from linux server to local
        sftp.get(remote_file_path,local_file_path)
        logger.info("The file from Linux is downlaoded to local")
    except Exception as e:
        logger.error(f"Error whilee connecting Linux {e}")


#### New code ###########
import boto3
import pandas as pd
from io import StringIO

# Initialize a session using Boto3
s3 = boto3.client('s3')

# Read the file from S3 and return dataframe
def read_csv_from_s3(bucket_name, file_key):
    try:
        # Fetch the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)

        # Read the content of the file and load it into a Pandas DataFrame
        csv_content = response['Body'].read().decode('utf-8')  # Decode content to string
        data = StringIO(csv_content)  # Use StringIO to simulate a file-like object

        # Read the CSV data into a Pandas DataFrame
        df = pd.read_csv(data)

        # Return the DataFrame
        return df
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return None


def verify_expected_as_S3_to_actual_as_db(db_engine_expected,query_expected,db_engine_actual,query_actual):
    bucket_name = 'bucket-upload-file-from-local-s3'  # Replace with your actual bucket name
    file_key = 'employeeData/emp_src.csv'
    # The desired path and file name in the S3 bucket
    # Call the function to read the CSV file from S3
    df_expected = read_csv_from_s3(bucket_name, file_key)
    logger.info(f"The expected data is the database is: {df_expected}")
    df_actual = pd.read_sql(query_actual, db_engine_actual)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected data in {query_expected} does not match with expected data in{query_actual}"
