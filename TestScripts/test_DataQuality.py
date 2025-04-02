# 1. complete these DQ tests for all other files

import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging

from CommonUtilities.utils import *
from Configuration.config import *

logging.basicConfig(
    filename='Logs/Extractprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

@pytest.mark.usefixtures("print_message as fixture")
class TestDataQuality:
    # Verify the DQ checks for sales_data.csv file
    @pytest.mark.smoke
    def test_DQ_Sales_data_file_availabilty(self):
        try:
            logger.info(f"File availabilty check for  initiated...")
            assert check_file_exists("TestData/sales_data_from_Linux.csv"),"Check why file is not available in the path"
            logger.info(f"File availabilty check for completed...")
        except Exception as e:
            logger.error("Error while checking the file availablity")
            pytest.fail("test for file availability check is failed")

    # Verify the DQ checks for sales_data.csv file
    @pytest.mark.smoke
    def test_DQ_sales_data_file_size(self):
        try:
            logger.info(f"File size check for  initiated...")
            assert check_file_size("TestData/sales_data_from_Linux.csv","csv"),"Check why file is is blank"
            logger.info(f"File size check for  completed...")
        except Exception as e:
            logger.error("Error while checking the file size")
            pytest.fail("test for file size check is failed")

    @pytest.mark.smoke
    def test_DQ_Sales_data_duplication(self):
        try:
            logger.info(f"Data duplication check initiated...")
            assert check_for_duplicates_across_the_columns("TestData/sales_data_from_Linux.csv","csv"),"Check why there are duplicates"
            logger.info(f"Data duplication check completed..")
        except Exception as e:
            logger.error("Error while reading the file ")
            pytest.fail("test for duplicate  check is failed")

    @pytest.mark.smoke
    def test_DQ_Supplier_data_duplication_for_sales_id_column(self):
        try:
            logger.info(f"Data duplication check  for supplier_id initiated...")
            assert check_for_duplicates_for_specific_column("TestData/sales_data_from_Linux.csv","csv","sales_id"),"Check why there are duplicates"
            logger.info(f"Data duplication check for supplier_id completed..")
        except Exception as e:
            logger.error("Error while reading the file ")
            pytest.fail("test for duplicate  check for supplier_id is failed")

    @pytest.mark.smoke
    def test_DQ_Sales_data_missing_value_check(self):
        try:
            logger.info(f"missing data  check initiated...")
            assert check_for_null_values("TestData/sales_data_from_Linux.csv","csv"),"Check why there are missing values"
            logger.info(f"misssing data check completed..")
        except Exception as e:
            logger.error("Error while reading the file ")
            pytest.fail("test for missign data check is failed")
