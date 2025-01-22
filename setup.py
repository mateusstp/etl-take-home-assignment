from src.sinks import SnowFlake
from src.sources import AppStoreAPI
from src.config import SNOWFLAKE_TEMP_TABLE


with open("src/sql/create_temp_sales_ms.sql", "r") as f:
    ddl_query = f.read()

DDL_CREATE_TMP_TABLE = ddl_query.format(TEMP_TABLE=SNOWFLAKE_TEMP_TABLE)


def create_temp_sales_table(force_create_tables=True):
    snowflake = SnowFlake()

    if not snowflake.table_exists(SNOWFLAKE_TEMP_TABLE) or force_create_tables:
        snowflake.run_query(query=DDL_CREATE_TMP_TABLE)
    else:
        print(f"Table {SNOWFLAKE_TEMP_TABLE} already exists")


def test_credentials():
    from datetime import datetime
    snowflake_cli = SnowFlake()
    success = snowflake_cli.test_credentials()

    if not success:
        raise Exception("Snowflake connection failed")
    app_store = AppStoreAPI()
    csv_file_path = app_store.get_sales_by_day(datetime(2024, 12, 1))
    if not csv_file_path:
        raise Exception("AppStoreAPI connection failed")
    else:
        print(f"AppStoreAPI connection successful. {csv_file_path}")


def run(force_create_table=False):
    print("Testing credentials ...")
    test_credentials()
    if force_create_table:
        print("Forcing tables creation...")
    create_temp_sales_table(force_create_table)
    print("Setup completed")
