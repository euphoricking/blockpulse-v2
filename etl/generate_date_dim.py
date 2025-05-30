import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timedelta
import logging

class GenerateDateRows(beam.DoFn):
    def __init__(self, start_date, end_date):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")

    def process(self, element):
        current_date = self.start_date
        while current_date <= self.end_date:
            yield {
                'date_id': int(current_date.strftime('%Y%m%d')),
                'date': current_date.strftime('%Y-%m-%d'),
                'day': current_date.day,
                'month': current_date.month,
                'year': current_date.year,
                'quarter': (current_date.month - 1) // 3 + 1,
                'day_of_week': current_date.isoweekday(),
                'day_name': current_date.strftime('%A'),
                'month_name': current_date.strftime('%B'),
                'is_weekend': current_date.weekday() >= 5
            }
            current_date += timedelta(days=1)

def run():
    output_table = 'blockpulse-insights-project.crypto_data.date_dim'

    schema = {
        "fields": [
            {"name": "date_id", "type": "INTEGER"},
            {"name": "date", "type": "DATE"},
            {"name": "day", "type": "INTEGER"},
            {"name": "month", "type": "INTEGER"},
            {"name": "year", "type": "INTEGER"},
            {"name": "quarter", "type": "INTEGER"},
            {"name": "day_of_week", "type": "INTEGER"},
            {"name": "day_name", "type": "STRING"},
            {"name": "month_name", "type": "STRING"},
            {"name": "is_weekend", "type": "BOOLEAN"}
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
            | 'CreateSeed' >> beam.Create([None])
            | 'GenerateDates' >> beam.ParDo(GenerateDateRows(start_date="2020-01-01", end_date="2030-12-31"))
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table=output_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location='gs://blockpulse-data-bucket/temp/'
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
