import os
from dataclasses import dataclass
from datetime import datetime
import time
import gzip
import shutil
import requests
from src.config import (APPLE_CONNECT_PRIVATE_KEY,
                        APPLE_CONNECT_ISSUER_ID,
                        APPLE_CONNECT_APP_ID,
                        APPLE_CONNECT_KEY_ID,
                        APPLE_CONNECT_VENDOR_NUMBER,
                        APPLE_BASE_URL)


@dataclass
class AppStoreAPI:

    def __post_init__(self):
        self.app_id = APPLE_CONNECT_APP_ID
        self.private_key = APPLE_CONNECT_PRIVATE_KEY
        self.key_id = APPLE_CONNECT_KEY_ID
        self.issuer_id = APPLE_CONNECT_ISSUER_ID
        self.vendor_number = APPLE_CONNECT_VENDOR_NUMBER
        self.base_url = APPLE_BASE_URL
        self.url_sales_report = f"{self.base_url}/v1/salesReports"

    def __jwt_token(self, ):
        from jwt import encode
        algorithm = "ES256"
        headers = {"alg": algorithm, "kid": self.key_id,  "typ": "JWT"}
        payload = {
            "iss": self.issuer_id,
            # The expiration time of the token is set to 15 minutes from the current time.
            "exp": int(round(time.time() + (15.0 * 60.0))),
            "aud": "appstoreconnect-v1",
            "iat": int(round(time.time()))
        }
        return encode(payload, self.private_key, algorithm=algorithm, headers=headers)

    @staticmethod
    def download_report(response, report_date: str):
        try:
            gzip_path = os.path.join(os.getcwd(), "data", f"{report_date}.gzip")
            output_path = gzip_path.replace(".gzip", ".csv")
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
            return None

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
