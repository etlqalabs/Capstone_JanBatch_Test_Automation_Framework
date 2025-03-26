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

@pytest.mark.usefixtures("print_message")
class TestDataExtraction:
    def test_DE_from_sales_data_to_staging(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for sales_data extraction has started..")
            df_expected =pd.read_csv("TestData/sales_data_Linux.csv")
            query_actual = """select * from staging_sales"""
            df_actual = pd.read_sql(query_actual, connect_to_mysql_database)
            assert df_actual.equals(df_expected),"Data extraction from sales_data.csv didn't happen correctly"
            logger.info("Test case executin for sales_data extraction has completed..")
        except Exception as e:
            logger.error(f"Test case executin for sales_data extraction has failed{e}")
            pytest.fail("Test case executin for sales_data extraction has failed")

    def test_DE_from_sales_data_to_staging1(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for sales_data extraction has started..")
            df_expected =pd.read_csv("TestData/sales_data_Linux.csv")
            query_actual = """select * from staging_sales"""
            df_actual = pd.read_sql(query_actual, connect_to_mysql_database)
            assert df_actual.equals(df_expected),"Data extraction from sales_data.csv didn't happen correctly"
            logger.info("Test case executin for sales_data extraction has completed..")
        except Exception as e:
            logger.error(f"Test case executin for sales_data extraction has failed{e}")
            pytest.fail("Test case executin for sales_data extraction has failed")





