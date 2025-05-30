# process_snapshot_data.py

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timezone
import json
import logging
from apache_beam.io.gcp.gcsio import GcsIO

class ReadGCSJson(beam.DoFn):
    def __init__(self, gcs_uri):
        self.gcs_uri = gcs_uri

    def process(self, unused_element):
        # Parse GCS path
        if not self.gcs_uri.startswith("gs://"):
            raise ValueError("gcs_uri must start with 'gs://'")

        path = self.gcs_uri.replace("gs://", "")
        bucket_name, *blob_parts = path.split('/')
        blob_path = "/".join(blob_parts)

        gcs = GcsIO()
        with gcs.open(f"{bucket_name}/{blob_path}", 'r') as f:
            data = json.load(f)

        current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        for record in data:
            yield {
                'coin_id': str(record.get('id')),
                'symbol': str(record.get('symbol')),
                'name': str(record.get('name')),
                'price': float(record.get('current_price', 0)),
                'market_cap': float(record.get('market_cap', 0)),
                'total_volume': float(record.get('total_volume', 0)),
                'price_change_percentage_24h': float(record.get('price_change_percentage_24h', 0)),
                'market_cap_change_percentage_24h': float(record.get('market_cap_change_percentage_24h', 0)),
                'high_24h': float(record.get('high_24h', 0)),
                'low_24h': float(record.get('low_24h', 0)),
                'circulating_supply': float(record.get('circulating_supply', 0)),
                'total_supply': float(record.get('total_supply', 0)),
                'max_supply': float(record.get('max_supply', 0)),
                'ath': float(record.get('ath', 0)),
                'ath_date': str(record.get('ath_date', '')),
                'atl': float(record.get('atl', 0)),
                'atl_date': str(record.get('atl_date', '')),
                'snapshot_date': current_date,
                'processing_time': current_date
            }

def run():
    gcs_input_uri = 'gs://blockpulse-data-bucket/raw/crypto_snapshot_latest.json'
    output_table = 'blockpulse-insights-project.crypto_data.crypto_market_snapshot_fact'

    schema = {
        "fields": [
            {"name": "coin_id", "type": "STRING"},
            {"name": "symbol", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "price", "type": "FLOAT"},
            {"name": "market_cap", "type": "FLOAT"},
            {"name": "total_volume", "type": "FLOAT"},
            {"name": "price_change_percentage_24h", "type": "FLOAT"},
            {"name": "market_cap_change_percentage_24h", "type": "FLOAT"},
            {"name": "high_24h", "type": "FLOAT"},
            {"name": "low_24h", "type": "FLOAT"},
            {"name": "circulating_supply", "type": "FLOAT"},
            {"name": "total_supply", "type": "FLOAT"},
            {"name": "max_supply", "type": "FLOAT"},
            {"name": "ath", "type": "FLOAT"},
            {"name": "ath_date", "type": "STRING"},
            {"name": "atl", "type": "FLOAT"},
            {"name": "atl_date", "type": "STRING"},
            {"name": "snapshot_date", "type": "STRING"},
            {"name": "processing_time", "type": "STRING"}
        ]
    }

    options = PipelineOptions(
        runner='DataflowRunner',
        project='blockpulse-insights-project',
        region='us-central1',
        temp_location='gs://blockpulse-data-bucket/temp/',
        staging_location='gs://blockpulse-data-bucket/staging/',
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Start' >> beam.Create([None])
            | 'ReadJSON' >> beam.ParDo(ReadGCSJson(gcs_input_uri))
            | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                table=output_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location='gs://blockpulse-data-bucket/temp/'
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
