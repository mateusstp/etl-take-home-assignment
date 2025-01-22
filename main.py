from dotenv import load_dotenv
load_dotenv()

docs = """
  arg1: start_date
  arg2: end_date
  arg3: force_recreate_temp_table (0-False or any digit)
  Exemple: python main.py 2024-12-01 2024-12-10 1
   Exemple:  docker run --rm sales-job '2012-11-09' '2024-12-10' 1 
  """

if __name__ == "__main__":
    from datetime import datetime, timedelta
    from src.pipeline.etl import SalesReportELT
    from src.sinks import SnowFlake
    from src.sources import AppStoreAPI
    import setup
    import sys
    if len(sys.argv) != 4:
        print("Invalid number of arguments")
        print(docs)
        sys.exit(1)

    format_date = "%Y-%m-%d"
    try:
        start_date = datetime.strptime(sys.argv[1], format_date)
        end_date = datetime.strptime(sys.argv[2], format_date)
        force_recreate_temp_table = bool(int(sys.argv[3]))
        if end_date < start_date:
            print("end_date must be greater than start_date")
            print(docs)
            sys.exit(1)
    except Exception:
        print("Invalid arguments")
        print(docs)
        sys.exit(1)

    print("Running with the following parameters:")
    print("start_date:", start_date)
    print("end_date:", end_date)
    print("recreate temp table:", force_recreate_temp_table)

    setup.run(force_create_table=force_recreate_temp_table)

    elt = SalesReportELT(sink=SnowFlake(), source=AppStoreAPI())

    current_date = start_date
    while current_date <= end_date:
        print(f"Processing {current_date}")
        elt.run(current_date)
        current_date += timedelta(days=1)
    print("EL process completed")
    print("Running transformation temp to prod...")
    elt.transform()
    print("Transformation completed")
    print("Job completed")
    sys.exit(0)
