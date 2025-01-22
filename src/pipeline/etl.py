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
        self.sink.load(csv_file_path, self.sink.temp_table)

    def transform(self):
        pass

    def run(self, report_date):
        csv_file_path = self.extract(report_date)
        self.load(csv_file_path)
