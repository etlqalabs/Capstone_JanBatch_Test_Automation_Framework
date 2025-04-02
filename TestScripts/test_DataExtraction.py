import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging

from CommonUtilities.utils import verify_expected_as_file_to_actual_as_db, verify_expected_as_db_to_actual_as_db, \
    getDataFromLinuxBox
from Configuration.config import *

logging.basicConfig(
    filename='Logs/Extractprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


class TestDataExtraction:

    '''
    # Tets script without using common utilities
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
      '''

    @pytest.mark.regression
    def test_DE_from_sales_data_to_staging(self, connect_to_mysql_database):
        try:
            logger.info("Test case executin for sales_data extraction has started..")
            getDataFromLinuxBox()
            verify_expected_as_file_to_actual_as_db("TestData/sales_data_Linux_remote.csv","csv",connect_to_mysql_database,"staging_sales")
            logger.info("Test case executin for sales_data extraction has completed..")
        except Exception as e:
            logger.error(f"Test case executin for sales_data extraction has failed{e}")
            pytest.fail("Test case executin for sales_data extraction has failed")

    @pytest.mark.regression
    def test_DE_from_product_data_to_staging(self, connect_to_mysql_database):
        try:
            logger.info("Test case executin for product_data extraction has started..")
            verify_expected_as_file_to_actual_as_db("TestData/product_data.csv","csv",connect_to_mysql_database,"staging_product")
            logger.info("Test case executin for product_data extraction has completed..")
        except Exception as e:
            logger.error(f"Test case executin for product_data extraction has failed{e}")
            pytest.fail("Test case executin for product_data extraction has failed")

    @pytest.mark.regression
    @pytest.mark.smoke
    def test_DE_from_supplier_data_to_staging(self, connect_to_mysql_database):
        try:
            logger.info("Test case executin for supplier_data extraction has started..")
            verify_expected_as_file_to_actual_as_db("TestData/supplier_data.json","json",connect_to_mysql_database,"staging_supplier")
            logger.info("Test case executin for supplier_data extraction has completed..")
        except Exception as e:
            logger.error(f"Test case executin for supplier_data extraction has failed{e}")
            pytest.fail("Test case executin for supplier_data extraction has failed")

    def test_DE_from_inventory_data_to_staging(self, connect_to_mysql_database):
        try:
            logger.info("Test case executin for inventory_data extraction has started..")
            verify_expected_as_file_to_actual_as_db("TestData/inventory_data.xml","xml",connect_to_mysql_database,"staging_inventoy")
            logger.info("Test case executin for inventory_data extraction has completed..")
        except Exception as e:
            logger.error(f"Test case executin for inventory_data extraction has failed{e}")
            pytest.fail("Test case executin for inventory_data extraction has failed")


    def test_DE_from_oracle_to_staging_mysql(self, connect_to_oracle_database,connect_to_mysql_database):
        try:
            logger.info("Test case executin for stores_data from oracle extraction has started..")
            query_expected = """select * from stores"""
            query_actual = """select * from staging_stores"""
            verify_expected_as_db_to_actual_as_db(connect_to_oracle_database, query_expected, connect_to_mysql_database,query_actual)
            logger.info("Test case executin for stores_data from oracle extraction has completed..")
        except Exception as e:
            logger.error(f"Test case executin for stores_data from oracle extraction has failed{e}")
            pytest.fail("Test case executin for stores_data from oracle extraction has failed")
