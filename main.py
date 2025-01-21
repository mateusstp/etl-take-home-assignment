# from datetime import datetime, timedelta
#
# # Main ETL process
# def etl_process():
#     jwt_token = generate_jwt()
#
#     start_date = datetime(2024, 12, 1)
#     end_date = datetime(2024, 12, 31)
#
#     current_date = start_date
#     while current_date <= end_date:
#         day_str = current_date
#         data = fetch_sales_data(jwt_token, day_str, day_str)
#         load_data_to_snowflake(data, temp_table)
#         current_date += timedelta(days=1)
#
#     merge_temp_to_prod()
from dotenv import load_dotenv
load_dotenv()

if __name__ == "__main__":
    from src.config.setup import test_credentials
    test_credentials()
