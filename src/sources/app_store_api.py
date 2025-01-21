import os
from dataclasses import dataclass
from datetime import datetime
import time
import gzip
import shutil
import requests
import tempfile
from src.config import (APPLE_CONNECT_PRIVATE_KEY,
                        APPLE_CONNECT_ISSUER_ID,
                        APPLE_CONNECT_APP_ID,
                        APPLE_CONNECT_KEY_ID,
                        APPLE_CONNECT_VENDOR_NUMBER,
                        APPLE_BASE_URL,
                        APPLE_JWT_EXPIRATION_MINUTES)


@dataclass
class AppStoreAPI:

    def __post_init__(self):
        self.app_id = APPLE_CONNECT_APP_ID
        self.private_key = APPLE_CONNECT_PRIVATE_KEY
        self.key_id = APPLE_CONNECT_KEY_ID
        self.issuer_id = APPLE_CONNECT_ISSUER_ID
        self.vendor_number = APPLE_CONNECT_VENDOR_NUMBER
        self.expiration_time = int(time.time()) + APPLE_JWT_EXPIRATION_MINUTES * 60
        self.base_url = APPLE_BASE_URL
        self.url_sales_report = f"{self.base_url}/v1/salesReports"
        self.url_apps = self.base_url + "/v1/apps"

    def __jwt_token(self, ):
        from jwt import encode
        algorithm = "ES256"
        headers = {"alg": algorithm, "kid": self.key_id,  "typ": "JWT"}
        payload = {
            "iss": self.issuer_id,
            "exp": int(round(time.time() + (15.0 * 60.0))),
            "aud": "appstoreconnect-v1",
            "iat": int(round(time.time()))
        }
        return encode(payload, self.private_key, algorithm=algorithm, headers=headers)

    @staticmethod
    def download_report(response, report_date: str):
        try:
            gzip_path = os.path.join(tempfile.TemporaryDirectory().name, report_date + ".gz")
            output_path = os.path.join(tempfile.TemporaryDirectory().name, report_date + ".csv")
            with open(gzip_path, "wb") as file:
                file.write(response.content)

            print(f"Report downloaded at: {gzip_path}")

            print("Extracting the report...")
            with gzip.open(gzip_path, "rb") as gz_file:
                with open(output_path, "wb") as extracted_file:
                    shutil.copyfileobj(gz_file, extracted_file)
            print(f"Report extracted successfully: {output_path}")
            os.remove(gzip_path)
            return output_path
        except requests.RequestException as e:
            print(f"Error downloading report: {e}")

    # @staticmethod
    # def download_report(response, report_date: str):
    #     from io import  BytesIO
    #     if response.headers.get('content-encoding') == 'agzip':
    #         compressed_data = BytesIO(response.content)
    #         decompressed_data = gzip.GzipFile(fileobj=compressed_data).read()
    #         results_map = {}
    #         data_from_file = decompressed_data.decode('utf8')
    #         data_from_file = data_from_file.strip()
    #         data_from_file_array = data_from_file.split('\n')
    #
    #         for line in data_from_file_array[1:]:
    #             line_data = line.split('\t')
    #             if line_data[6] not in results_map:
    #                 results_map[line_data[6]] = 0
    #
    #             results_map[line_data[6]] += int(line_data[7])
    #
    #         # Product Type Identifiers for app downloads
    #         results_map.setdefault("1", 0)
    #         results_map.setdefault("1F", 0)
    #         results_map.setdefault("1T", 0)
    #         total_units = results_map["1"] + results_map["1F"] + results_map["1T"]
    #         print(f"Total units: {total_units}")
    #         return  total_units
    #     else:
    #         return 0

    def get_sales_by_day(self, report_date: datetime):
        jwt_token = self.__jwt_token()
        headers = {"Authorization": f"Bearer {jwt_token}"}
        str_report_date = report_date.strftime("%Y-%m-%d")

        params = {
            "filter[frequency]": "DAILY",
            "filter[reportType]": "SALES",
            "filter[reportSubType]": "SUMMARY",
            "filter[vendorNumber]": self.vendor_number,
            "filter[reportDate]": str_report_date
        }

        response = requests.get(self.url_sales_report, headers=headers, params=params, stream=True)
        response.raise_for_status()

        return self.download_report(response, str_report_date)

    def test_credentials(self, ):
        try:
            jwt_token = self.__jwt_token()
            headers = {"Authorization": f"Bearer {jwt_token}"}
            response = requests.get(self.url_apps, headers=headers)
            response.raise_for_status()
            print("App Store connection successful")
            print(response.json())
            return True
        except requests.RequestException as e:
            print(f"Error connecting to App Store: {e}")
            return False
