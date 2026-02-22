import datetime
import io
import json

from google.cloud import storage

class GcsManager:
    def __init__(self,bucket_name):
        self.client = storage.Client()
        self.date = datetime.datetime.now().date()
        self.bucket_name = bucket_name

    def get_bucket(self):
        return self.client.bucket(self.bucket_name)

    def get_blob(self):
        blob = self.get_bucket().blob(f"raw_data/{self.date}.json")
        return blob

    def blob_loader(self):
        return json.loads(self.get_blob().download_as_text())

    def blob_uploader(self,df_final):
        out_blob = self.get_bucket().blob(f"processed/{self.date}.csv")
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False)
        out_blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

