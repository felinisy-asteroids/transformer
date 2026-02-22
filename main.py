from gcs_client.gcs_manager import GcsManager
import pandas as pd

from transformer.transformer import parse_response


def main(request):
    request_data = request.get_json()
    bucket_name = request_data.get("bucket")

    gcs = GcsManager(bucket_name)
    blob = gcs.get_blob()

    if not blob.exists():
        return {"error": "json not found"}, 404

    raw_data = gcs.blob_loader()

    records = parse_response(raw_data)



    df = pd.DataFrame(records)

    required_columns = [
        "asteroid_id", "neo_reference_id", "name",
        "absolute_magnitude_h", "is_potentially_hazardous_asteroid",
        "is_sentry_object", "export_date"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    df_final = df[required_columns]

    gcs.blob_uploader(df_final)

    return {"status": "success", "rows": len(df_final)}, 200

# if __name__ == '__main__':
#     class MockRequest:
#         def get_json(self):
#             return {"bucket": "asteroids-etl"}
#     main(MockRequest())

