from dotenv import load_dotenv
load_dotenv()

if __name__ == "__main__":
    from datetime import datetime, timedelta
    from src.pipeline.etl import SalesReportELT
    from src.sinks import SnowFlake
    from src.sources import AppStoreAPI
    import setup
    setup.run(force_create_tables=False)
    print("Setup completed")

    elt = SalesReportELT(sink=SnowFlake(), source=AppStoreAPI())

    start_date = datetime(2024, 12, 1)
    end_date = datetime(2024, 12, 10)
    current_date = start_date
    while current_date <= end_date:
        print(f"Processing {current_date}")
        elt.run(current_date)
        current_date += timedelta(days=1)
