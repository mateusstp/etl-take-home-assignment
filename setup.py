from src.sinks import SnowFlake
from src.sources import AppStoreAPI
from src.config import SNOWFLAKE_PRD_TABLE, SNOWFLAKE_TEMP_TABLE

DDL_CREATE_TMP_TABLE = f"""CREATE OR REPLACE TABLE {SNOWFLAKE_TEMP_TABLE} 
            (
                Provider STRING,
                "Provider Country" STRING,
                SKU STRING,
                Developer STRING,
                Title STRING,
                Version STRING,
                "Product Type Identifier" STRING,
                Units INTEGER,
                "Developer Proceeds" FLOAT,
                "Begin Date" STRING,
                "End Date" STRING,
                "Customer Currency" STRING,
                "Country Code" STRING,
                "Currency of Proceeds" STRING,
                "Apple Identifier" INTEGER,
                "Customer Price" FLOAT,
                "Promo Code" STRING,
                "Parent Identifier" STRING,
                Subscription STRING,
                Period STRING,
                Category STRING,
                CMB STRING,
                Device STRING,
                "Supported Platforms" STRING,
                "Proceeds Reason" STRING,
                "Preserved Pricing" STRING,
                Client STRING,
                "Order Type" STRING
           );
        """

DDL_CREATE_PRD_TABLE = f""" CREATE OR REPLACE TABLE {SNOWFLAKE_PRD_TABLE} (
                                  load_timestamp  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP(),
                                  Provider STRING,
                                  Provider_Country STRING,
                                  SKU STRING,
                                  Developer STRING,
                                  Title STRING,
                                  Version STRING,
                                  Product_Type Identifier STRING,
                                  Units INTEGER,
                                  Developer_Proceeds FLOAT,
                                  Begin_Date DATE,
                                  End_Date DATE,
                                  Customer_Currency STRING,
                                  Country_Code STRING,
                                  Currency_of_Proceeds STRING,
                                  Apple_Identifier INTEGER,
                                  Customer_Price FLOAT,
                                  Promo_Code STRING,
                                  Parent_Identifier STRING,
                                  Subscription STRING,
                                  Period STRING,
                                  Category STRING,
                                  CMB STRING,
                                  Device STRING,
                                  Supported_Platforms STRING,
                                  Proceeds_Reason STRING,
                                  Preserved_Pricing STRING,
                                  Client STRING,
                                  Order_Type STRING
                   );
                """


def create_sales_tables(force_create_tables=False):
    snowflake = SnowFlake()

    if not snowflake.table_exists(SNOWFLAKE_TEMP_TABLE) or force_create_tables:
        snowflake.run_ddl_query(query=DDL_CREATE_TMP_TABLE)
    else:
        print(f"Table {SNOWFLAKE_TEMP_TABLE} already exists")

    # if not snowflake.table_exists(SNOWFLAKE_PRD_TABLE) or force_create_tables:
    #     snowflake.run_ddl_query(query=DDL_CREATE_PRD_TABLE)
    # else:
    #     print(f"Table {SNOWFLAKE_PRD_TABLE} already exists")


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


def run(force_create_tables=False):
    print("Testing credentials ...")
    test_credentials()
    if force_create_tables:
        print("Forcing tables creation...")
        create_sales_tables(force_create_tables)
    else:
        print("Creating tables if not exist...")
        create_sales_tables()

    # snowflake = SnowFlake()
    # cursor1 = snowflake.conn.cursor()
    # cursor = snowflake.conn.cursor()
    #
    # # Show schemas
    # cursor1.execute("SHOW SCHEMAS;")
    # schemas = cursor1.fetchall()
    # print("Schemas:")
    # for schema in schemas:
    #     print(schema)
    #     try:
    #         cursor.execute(f"SHOW TABLES IN SCHEMA {schema[1]};")
    #         tables = cursor.fetchall()
    #         print(f"Tables in schema {schema[1]}:")
    #         for table in tables:
    #             print(table)
    #     except Exception as e:
    #         print(f"Error: {e}")
