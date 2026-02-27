from typing import Any, Dict, Tuple

import pandas as pd
import requests

from gcs_client.gcs_manager import GcsManager
from transformer.transformer import transform_asteroid_data


def main(request: requests) -> Tuple[Dict[str, Any], int]:
    """
    Cloud Function entry point.

    Args:
        request: HTTP request object containing JSON body with bucket name.

    Returns:
        JSON response with status and row count.
    """
    request_data: Dict[str, Any] = request.get_json(silent=True) or {}
    bucket_name: str | None = request_data.get("bucket")

    if not bucket_name:
        return {"error": "Bucket name is required"}, 400

    gcs = GcsManager(bucket_name)

    if not gcs.get_raw_blob().exists():
        return {"error": "Raw JSON not found"}, 404

    raw_data = gcs.load_raw_blob()

    df_final: pd.DataFrame = transform_asteroid_data(raw_data)

    gcs.upload_processed_csv(df_final)

    return {"status": "success", "rows": len(df_final)}, 200
