import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
import json
import logging
from datetime import datetime, timezone

class ReadAssetJsonFromGCS(beam.DoFn):
    def __init__(self, gcs_uri):
        self.gcs_uri = gcs_uri

    def process(self, unused_element):
        if not self.gcs_uri.startswith("gs://"):
            raise ValueError("gcs_uri must start with 'gs://'")

        path = self.gcs_uri.replace("gs://", "")
        bucket_name, *blob_parts = path.split('/')
        blob_path = "/".join(blob_parts)

        gcs = GcsIO()
        with gcs.open(f"{bucket_name}/{blob_path}", 'r') as f:
            asset_data = json.load(f)

        current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        for record in asset_data:
            yield {
                'coin_id': str(record.get('id')),
                'symbol': str(record.get('symbol')),
                'name': str(record.get('name')),
                'asset_platform_id': str(record.get('asset_platform_id', '')),
                'block_time_in_minutes': int(record.get('block_time_in_minutes', 0)),
                'hashing_algorithm': str(record.get('hashing_algorithm', '')),
                'categories': ', '.join(record.get('categories', [])),
                'public_notice': str(record.get('public_notice', '')),
                'genesis_date': str(record.get('genesis_date', '')),
                'homepage': ', '.join(record.get('links', {}).get('homepage', [])),
                'snapshot_date': current_date
            }

def run():
    gcs_uri = 'gs://blockpulse-data-bucket/raw/crypto_assets_metadata.json'
    output_table = 'blockpulse-insights-project.crypto_data.crypto_asset_dim'

    schema = {
        "fields": [
            {"name": "coin_id", "type": "STRING"},
            {"name": "symbol", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "asset_platform_id", "type": "STRING"},
            {"name": "block_time_in_minutes", "type": "INTEGER"},
            {"name": "hashing_algorithm", "type": "STRING"},
            {"name": "categories", "type": "STRING"},
            {"name": "public_notice", "type": "STRING"},
            {"name": "genesis_date", "type": "STRING"},
            {"name": "homepage", "type": "STRING"},
            {"name": "snapshot_date", "type": "STRING"}
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
            | 'ReadAssets' >> beam.ParDo(ReadAssetJsonFromGCS(gcs_uri))
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
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
