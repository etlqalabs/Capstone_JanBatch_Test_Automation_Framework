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


@pytest.fixture()
def connect_to_oracle_database():
    logger.info("oracle conenction is being established...")
    oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}').connect()
    logger.info("oracle conenction is has been established...")
    yield oracle_engine
    oracle_engine.close()
    logger.info("oracle conenction is closed established...")

@pytest.fixture()
def connect_to_mysql_database():
    logger.info("mysql conenction is being established...")
    mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}').connect()
    logger.info("mysql conenction is has been established...")
    yield mysql_engine
    mysql_engine.close()
    logger.info("mysql conenction is closed established...")

@pytest.fixture()
def print_message():
    logger.info("Before test print..")
    yield
    logger.info("after test print..")



