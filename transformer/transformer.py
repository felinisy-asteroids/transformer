def parse_response(data):
    keys = [
        "neo_reference_id", "name",
        "absolute_magnitude_h",
        "is_potentially_hazardous_asteroid",
        "is_sentry_object"
    ]
    return [
        {"asteroid_id": neo["id"], **{k: neo[k] for k in keys}, "export_date": date_key}
        for response in data
        for date_key, objects in response.get("near_earth_objects", {}).items()
        for neo in objects
    ]