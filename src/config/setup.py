
from src.sinks.snowflake import SnowFlake
from src.sources.app_store_api import AppStoreAPI
from src.config import SNOWFLAKE_PRD_TABLE, SNOWFLAKE_TEMP_TABLE


def create_sales_table():
    snowflake = SnowFlake()

    if not snowflake.table_exists(SNOWFLAKE_PRD_TABLE):
        snowflake.create_table(SNOWFLAKE_PRD_TABLE)

    if not snowflake.table_exists(SNOWFLAKE_TEMP_TABLE):
        snowflake.create_table(SNOWFLAKE_TEMP_TABLE)

def test_credentials():
    from datetime import datetime
    snowflake = SnowFlake()
    success = snowflake.test_credentials()

    if not success:
        raise Exception("Snowflake connection failed")
    app_store = AppStoreAPI()
    success = app_store.get_sales_by_day(datetime(2024,12,1))
    if not success:
        raise Exception("AppStoreAPI connection failed")


