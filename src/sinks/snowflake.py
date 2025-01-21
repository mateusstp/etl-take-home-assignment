import tempfile
from dataclasses import dataclass
import pandas as pd
import snowflake.connector
from src.config import (SNOWFLAKE_USER_NAME,
                        SNOWFLAKE_PASSWORD,
                        SNOWFLAKE_ACCOUNT_NAME,
                        SNOWFLAKE_WAREHOUSE,
                        SNOWFLAKE_DATABASE,
                        SNOWFLAKE_SCHEMA)


@dataclass
class SnowFlake:
    def __post_init__(self):
        self.conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER_NAME,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT_NAME,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

    def table_exists(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return len(list(cursor)) > 0

    def create_table(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"CREATE OR REPLACE TABLE {table_name} (date DATE, sales FLOAT)")
        self.conn.close()

    def load(self, data, table_name):
        cursor = self.conn.cursor()
        df = pd.DataFrame(data)
        temp_file = tempfile.NamedTemporaryFile().name
        temp_file = f"{temp_file}.csv"
        df.to_csv(temp_file, index=False)
        cursor.execute(f"PUT file://{temp_file} @%{table_name}")
        cursor.execute(
            f"COPY INTO {table_name} FROM @%{table_name} FILE_FORMAT = (type = 'CSV' field_optionally_enclosed_by='""')")
        self.conn.close()

    def merge_temp_to_prod(self, prod_table, temp_table):
        cursor = self.conn.cursor()
        merge_query = f"""
        MERGE INTO {prod_table} AS prod
        USING {temp_table} AS temp
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
