import apache_beam as beam
from apache_beam.io import ReadFromText


def read_data_from_gcs(pipeline, gcs_bucket, label="Read from GCS Bucket"):
    return (pipeline | label >> ReadFromText(gcs_bucket))


def read_data_from_pubsub(pipeline,
                          input_subscription,
                          label="Read from PubSub"):
    return (pipeline | label >> beam.io.ReadFromPubSub(
        subscription=input_subscription, timestamp_attribute='ts'))


def write_to_sink(pcollection, bq_table_ref, label):
    _ = (pcollection | label >> beam.io.WriteToBigQuery(
        bq_table_ref,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.
        RETRY_ON_TRANSIENT_ERROR))
