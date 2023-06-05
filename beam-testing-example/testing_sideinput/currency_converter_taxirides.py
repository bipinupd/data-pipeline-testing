import json
import argparse
import apache_beam as beam
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def read_data_from_pubsub(pipeline, input_topic):
  return (pipeline | "ReadFromPubSub" >> ReadFromPubSub(topic=input_topic))

def read_currency_rate(pipeline, bigquery_table):
    # from google.cloud import bigquery
    # bigquery_client = bigquery.Client()
    # query_job = bigquery_client.query(bigquery_table)
    # rows = query_job.result(timeout=60)
    # content = [row.values() for row in rows]
    # return content
    return [
        ("EUR", 0.91),
        ("CHF", 0.96),
    ]
    return pipeline | "Rates" >> beam.Create(rates)

def change_currency(row, ratios):
    new_row = row.copy()
    for key in ratios:
        new_row[f"meter_reading_{key}"] = new_row["meter_reading"] * ratios[key]

    logging.info(f"New row {new_row}")
    return [new_row]

def write_to_sink(pcoll, output_topic):
    return  pcoll \
          | 'encode' >> beam.Map(lambda x: json.dumps(x).encode('utf-8')).with_output_types(bytes) \
          | "Write to Sink" >> beam.io.WriteToPubSub(output_topic)

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        default="projects/pubsub-public-data/topics/taxirides-realtime",
        help='Input topic')
    parser.add_argument(
        '--output_topic',
        dest='output_topic',
        help='Output topic')
    parser.add_argument(
        '--side_input_table',
        dest='side_input_table',
        help='Side input table')
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with Pipeline(options=pipeline_options) as p:
        rates_pc = read_currency_rate(p, known_args.side_input_table)
        pubsub_raw = read_data_from_pubsub(p, known_args.input_topic)
        pubsub =  ( pubsub_raw | "Json Loads" >> beam.Map(json.loads)
                    | "Filter" >> beam.Filter(lambda x: x["ride_status"] == "dropoff")
                    | "Parse" >> beam.Map(lambda x: {"ride_id": x["ride_id"], "meter_reading": x["meter_reading"]})
                )
        join = pubsub | ParDo(change_currency, ratios=beam.pvalue.AsDict(rates_pc))
        write_to_pubsub(join, known_args.output_topic)

if __name__ == "__main__":
    run()