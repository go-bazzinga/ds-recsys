import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
from utils.config import Config

class BigQueryClient:
    def __init__(self):
        cfg = Config()
        service_cred = cfg.get('service_cred')
        service_acc_creds = json.loads(service_cred)
        credentials = service_account.Credentials.from_service_account_info(service_acc_creds)
        self.client = bigquery.Client(credentials=credentials, project="hot-or-not-feed-intelligence")

    def query(self, query):
        query_job = self.client.query(query)
        results = query_job.result()
        return self._to_dataframe(results)

    def _to_dataframe(self, results):
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)

if __name__ == '__main__':
    bq = BigQueryClient()
    print(bq)