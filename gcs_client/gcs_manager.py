import datetime
import io
import json
from typing import Dict, Any

from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.storage import Bucket, Blob


class GcsManager:
    """
    Manage interactions with Google Cloud Storage for raw and processed data.
    """
    RAW_PREFIX = "raw_data"
    PROCESSED_PREFIX = "processed"

    def __init__(self, bucket_name):
        """
        Initialize GCS manager.

        Args:
            bucket_name: Name of the GCS bucket.
            date: Processing date (defaults to today).
        """
        self.client = storage.Client()
        self.date = datetime.datetime.now().date()
        self.bucket_name = bucket_name

    def get_bucket(self) -> Bucket:
        """Return GCS bucket instance."""
        return self.client.bucket(self.bucket_name)

    def get_raw_blob(self) -> Blob:
        """Return raw JSON blob for the given date."""
        return self.get_bucket().blob(f"raw_data/{self.date}.json")

    def load_raw_blob(self) -> Dict[str, Any]:
        """
                Download and parse raw JSON file from GCS.

                Returns:
                    Parsed JSON content.

                Raises:
                    FileNotFoundError: If the blob does not exist.
                """
        blob = self.get_raw_blob()
        try:
            content = blob.download_as_text()
        except NotFound:
            raise FileNotFoundError(f"{blob.name} not found in bucket.")
        return json.loads(content)

    def upload_processed_csv(self, df) -> None:
        """
        Upload processed DataFrame as CSV to GCS.

        Args:
            df: Final processed DataFrame.
        """
        out_blob = self.get_bucket().blob(f"{self.PROCESSED_PREFIX}/{self.date}.csv")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        out_blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
