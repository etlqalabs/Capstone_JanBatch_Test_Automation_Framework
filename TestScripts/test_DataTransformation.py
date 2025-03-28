#1. complete the remaining 5 test cases for different transformation

import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging

from CommonUtilities.utils import verify_expected_as_file_to_actual_as_db, verify_expected_as_db_to_actual_as_db
from Configuration.config import *

logging.basicConfig(
    filename='Logs/Extractprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

@pytest.mark.usefixtures("print_message")
class TestDataTransformation:

    def test_DT_Filter_check(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for filter transformation has started..")
            query_expected = """select * from staging_sales where sale_date>='2024-09-10'"""
            query_actual = """select * from filtered_sales_data"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("Test case executin for filter transformation has completed..")
        except Exception as e:
            logger.error(f"Test case executin for filter transformation has failed{e}")
            pytest.fail("Test case executin for filter transformation has failed")
