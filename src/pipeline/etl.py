from abc import ABC, abstractmethod
from src.sources.app_store_api import AppStoreAPI
from src.sinks.snowflake_sink import SnowFlake


class ELT(ABC):

    @abstractmethod
    def extract(self, *args, **kwargs):
        pass

    @abstractmethod
    def transform(self, *args, **kwargs):
        pass

    @abstractmethod
    def load(self, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass


class SalesReportELT(ELT):
    def __init__(self, source: AppStoreAPI, sink: SnowFlake):
        self.source = source
        self.sink = sink

    def extract(self, report_date):
        return self.source.get_sales_by_day(report_date)

    def load(self, csv_file_path):
        self.sink.copy_file_to_table(csv_file_path, self.sink.temp_table)

    def transform(self):
        with open("src/sql/create_prod_sales_ms.sql", "r") as f:
            ddl_query = f.read()
        create_prod_table = ddl_query.format(PRD_TABLE=self.sink.prd_table, TEMP_TABLE=self.sink.temp_table)
        self.sink.run_query(create_prod_table)

    def run(self, report_date):
        csv_file_path = self.extract(report_date)
        self.load(csv_file_path)
