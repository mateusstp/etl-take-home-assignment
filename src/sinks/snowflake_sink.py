import tempfile
from datetime import datetime as dt
from dataclasses import dataclass
import pandas as pd
import os
from snowflake.connector import ProgrammingError
import snowflake.connector
from src.config import (SNOWFLAKE_USER_NAME,
                        SNOWFLAKE_PASSWORD,
                        SNOWFLAKE_ACCOUNT_NAME,
                        SNOWFLAKE_WAREHOUSE,
                        SNOWFLAKE_DATABASE,
                        SNOWFLAKE_TEMP_TABLE,
                        SNOWFLAKE_PRD_TABLE,
                        SNOWFLAKE_SCHEMA_TEMP)


@dataclass
class SnowFlake:
    def __post_init__(self):
        self.conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER_NAME,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT_NAME,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE
            # schema=SNOWFLAKE_SCHEMA_TEMP
        )
        self.temp_table = SNOWFLAKE_TEMP_TABLE
        self.prd_table = SNOWFLAKE_PRD_TABLE

    def table_exists(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return len(list(cursor)) > 0

    def run_ddl_query(self, query):
        cursor = self.conn.cursor()
        result = cursor.execute(query)
        return  result

    def load(self, file_path, table_name):
        file_name = file_path.split(os.sep)[-1]
        table_name_id = ".".join(table_name.split(".")[1:])
        print(f"Loading data into {table_name_id}...")
        cursor = self.conn.cursor()
        stg_name = "stg_" + file_name.replace("-", "_").replace(".", "_")

        # cursor.execute("USE SCHEMA")
        # user_stage_query = f"CREATE OR REPLACE STAGE {stg_name}"
        # print(user_stage_query)
        # cursor.execute(user_stage_query)

        # put_query = f"PUT file://{file_path} @%{table_name_id}"
        put_query = f"PUT file://{file_path} @~/staged/{stg_name}"
        print(put_query)
        cursor.execute(put_query)
        # copy_query = f"""
        #     COPY INTO
        #             {table_name_id}
        #     FROM
        #         @%{table_name_id}
        #      FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        # """
        copy_query = f"""
            COPY INTO
                    {table_name_id}
            FROM 
                @~/staged/{stg_name}
            FILE_FORMAT = (
                TYPE=CSV,
                SKIP_HEADER=1,
                FIELD_DELIMITER='\t'
            )
            ON_ERROR=ABORT_STATEMENT
            PURGE=TRUE
        """
        print(copy_query)
        result = cursor.execute(copy_query)
        print(result)
        # cursor.execute(f"DROP STAGE {stg_name}")
        cursor.close()

    def merge_temp_to_prod(self,):
        cursor = self.conn.cursor()
        merge_query = f"""
        MERGE INTO {self.prd_table} AS prod
        USING {self.temp_table} AS temp
        ON prod.date = temp.date
        WHEN MATCHED THEN
            UPDATE SET prod.sales = temp.sales
        WHEN NOT MATCHED THEN
            INSERT (date, sales) VALUES (temp.date, temp.sales);
        """
        cursor.execute(merge_query)
        self.conn.close()

    def test_credentials(self, ):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()
            print(f"Snowflake connection successful. Current version: {version[0]}")
            return True
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            return False
        finally:
            if self.conn:
                self.conn.close()
