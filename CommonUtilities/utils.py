#1. implement execption handling for this file

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
    df_expected = pd.read_sql(db_engine_expected, db_engine_expected)
    logger.info(f"The expected data is the database is: {df_expected}")
    df_actual = pd.read_sql(query_actual, db_engine_actual)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected data in {query_expected} does not match with expected data in{query_actual}"

