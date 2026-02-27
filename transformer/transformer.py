from typing import List, Any, Dict
import pandas as pd

REQUIRED_COLUMNS: List[str] = [
    "asteroid_id", "neo_reference_id", "name",
    "absolute_magnitude_h", "is_potentially_hazardous_asteroid",
    "is_sentry_object", "export_date"
]

def parse_response(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse raw NASA API response into flat asteroid records.

    Args:
        data: Raw JSON response from GCS.

    Returns:
        List of flattened asteroid dictionaries.
    """
    keys = [
        "neo_reference_id",
        "name",
        "absolute_magnitude_h",
        "is_potentially_hazardous_asteroid",
        "is_sentry_object",
    ]
    records: List[Dict[str, Any]] = []

    for response in data:
        for date_key, objects in response.get("near_earth_objects", {}).items():
            for neo in objects:
                record = {
                    "asteroid_id": neo.get("id"),
                    **{k: neo.get(k) for k in keys},
                    "export_date": date_key,
                }
                records.append(record)

    return records

def transform_asteroid_data(data: Dict[str, Any]) -> pd.DataFrame:
    """
    Transform raw asteroid JSON into a cleaned DataFrame
    with required columns.

    Args:
        data: Raw JSON data from GCS.

    Returns:
        Cleaned pandas DataFrame ready for export.
    """
    records = parse_response(data)

    df = pd.DataFrame(records)

    # Ensure required columns exist
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[REQUIRED_COLUMNS]
