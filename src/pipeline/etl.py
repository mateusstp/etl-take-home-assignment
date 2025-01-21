from abc import ABC, abstractmethod
class  ETL(ABC):

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def run(self):
        pass
class SalesReportETL(ETL):
    def __init__(self, source, sink):
        self.source = source
        self.sink = sink

    def extract(self):
       pass

    def transform(self):
     pass

    def load(self):
      pass

    def run(self):
        self.extract()
        self.transform()
        self.load()

