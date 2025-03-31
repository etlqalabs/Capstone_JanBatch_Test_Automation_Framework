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

@pytest.mark.usefixtures("print_message as fixture")
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




    def test_DT_Router_Low_check(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for Router_Low transformation has started..")
            query_expected = """select * from filtered_sales_data where region='Low'"""
            query_actual = """select * from low_sales"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("Test case executin for Router_Low transformation has completed..")
        except Exception as e:
            logger.error(f"Test case executin for Router_Low transformation has failed{e}")
            pytest.fail("Test case executin for Router_Low transformation has failed")


    def test_DT_Router_High_check(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for Router_High transformation has started..")
            query_expected = """select * from filtered_sales_data where region='High'"""
            query_actual = """select * from high_sales"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("Test case executin for Router_High transformation has completed..")
        except Exception as e:
            logger.error(f"Test case executin for Router_High transformation has failed{e}")
            pytest.fail("Test case executin for Router_High transformation has failed")


    def test_DT_Aggregator_Sales_data_check(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for Aggregator_Sales_data transformation has started..")
            query_expected = """select product_id,month(sale_date)as month,year(sale_date) as year ,sum(quantity*price) as total_sales 
                                    from filtered_sales_data group by product_id,month(sale_date),year(sale_date);"""
            query_actual = """select * from monthly_sales_summary_source"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("Test case executin for Aggregator_Sales_data transformation has completed..")
        except Exception as e:
            logger.error(f"Test case executin for Aggregator_Sales_data transformation has failed{e}")
            pytest.fail("Test case executin for Aggregator_Sales_data transformation has failed")


    def test_DT_Aggregator_Inventory_Level_check(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for Aggregator_Inventory_Level transformation has started..")
            query_expected = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventoy group by store_id;"""
            query_actual = """select * from aggregated_inventory_level"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("Test case executin for Aggregator_Inventory_Level transformation has completed..")
        except Exception as e:
            logger.error(f"Test case executin for Aggregator_Inventory_Level transformation has failed{e}")
            pytest.fail("Test case executin for Aggregator_Inventory_Level transformation has failed")


    def test_DT_Joiner_Sales_data_check(self,connect_to_mysql_database):
        try:
            logger.info("Test case executin for Joiner_Sales_data transformation has started..")
            query_expected = """select fs.sales_id,fs.quantity,fs.sale_date,fs.price,fs.quantity*fs.price as total_sales,
                                    p.product_id,p.product_name,s.store_id,s.store_name
                                    from filtered_sales_data as fs
                                    inner join staging_product as p on p.product_id= fs.product_id
                                    inner join staging_stores as s on s.store_id= fs.store_id
                                    """
            query_actual = """select * from sales_with_details"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("Test case executin for Joiner_Sales_data transformation has completed..")
        except Exception as e:
            logger.error(f"Test case executin for Joiner_Sales_data transformation has failed{e}")
            pytest.fail("Test case executin for Joiner_Sales_data transformation has failed")
