#1. Fix all the test cases from data loading ( currently only 1 test case works)

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


class TestDataLoading:
    def test_DataLoad_sales_summary_DQ_Check(self, connect_to_mysql_database):
        try:
            logger.info("test case for sales_summary data load has started ....")
            query_expected = """select * from monthly_sales_summary_source order by product_id,month,year"""
            query_actual = """select * from monthly_sales_summary order by product_id,month,year"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database, query_expected, connect_to_mysql_database,
                                                  query_actual)
            logger.info("test case for sales_summary data load has completed ....")
        except Exception as e:
            logger.error(f"test case for sales_summary data load has failed{e}")
            pytest.fail("Data Load failed during ETL Load process")


    def test_DataLoad_fact_sales_Check(self, connect_to_mysql_database):
        try:
            logger.info("test case for fact_sales data load has started ....")
            query_expected = """select  sales_id,product_id,store_id,quantity,total_sales,sale_date from sales_with_details order by sales_id,product_id,store_id;"""
            query_actual = """select  sales_id,product_id,store_id,quantity,total_sales,sale_date from fact_sales order by sales_id,product_id,store_id;"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database, query_expected, connect_to_mysql_database,query_actual)
            logger.info("test case for fact_sales data load has completed ....")
        except Exception as e:
            logger.error(f"test case for fact_sales data load has failed{e}")
            pytest.fail("Data Load failed during ETL Load process")


    def test_DataLoad_fact_inventory_Check(self, connect_to_mysql_database):
        try:
            logger.info("test case for fact_inventory data load has started ....")
            query_expected = """select product_id,store_id,quantity_on_hand,last_updated from staging_inventory order by product_id,store_id"""
            query_actual = """select product_id,store_id,quantity_on_hand,last_updated  from fact_inventory order by product_id,store_id;"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database, query_expected, connect_to_mysql_database,
                                                  query_actual)
            logger.info("test case for fact_inventory data load has completed ....")
        except Exception as e:
            logger.error(f"test case for fact_inventory data load has failed{e}")
            pytest.fail("Data Load failed during ETL Load process")


    def test_DataLoad_inventory_level_by_store_Check(self, connect_to_mysql_database):
        try:
            logger.info("test case for inventory_level_by_store data load has started ....")
            query_expected = """select store_id,total_quantity_per_store as total_inventory  from aggregated_inventory_level order by store_id"""
            query_actual = """select store_id,cast(total_inventory as Double) as total_inventory from inventory_levels_by_store order by store_id;"""
            verify_expected_as_db_to_actual_as_db(connect_to_mysql_database, query_expected, connect_to_mysql_database,
                                                  query_actual)
            logger.info("test case for inventory_level_by_store data load has completed ....")
        except Exception as e:
            logger.error(f"test case for inventory_level_by_store data load has failed{e}")
            pytest.fail("Data Load failed during ETL Load process")
