# fetch_api_to_gcs.py

from google.cloud import storage
import requests
import json
from datetime import datetime

def download_and_store_json(gcs_uri):
    """
    Downloads crypto market data from CoinGecko API and stores it as JSON in GCS.

    Args:
        gcs_uri (str): Full GCS path to save the raw JSON file
                       (e.g., 'gs://your-bucket/raw/crypto_snapshot_YYYYMMDD.json')
    """
    # Step 1: Fetch data from CoinGecko API
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1,
        'sparkline': False
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    # Step 2: Parse GCS path
    if not gcs_uri.startswith("gs://"):
        raise ValueError("gcs_uri must start with 'gs://'")

    path = gcs_uri.replace("gs://", "")
    bucket_name, *blob_parts = path.split('/')
    blob_name = "/".join(blob_parts)

    # Step 3: Upload JSON to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type='application/json'
    )

    print(f"Saved API data to {gcs_uri}")
