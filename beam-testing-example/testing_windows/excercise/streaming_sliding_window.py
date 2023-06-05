from apache_beam import CombinePerKey
from apache_beam import Map
from apache_beam import WindowInto
import logging
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import Pipeline
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms import window
from datetime import datetime
import json
from apache_beam.io.gcp.pubsub import ReadFromPubSub


class ParseMessages(beam.DoFn):

    def process(self, element):
        parsed = json.loads(element.decode("utf-8"))
        if parsed["ride_status"].lower() = "finished":
            ride_status = parsed["ride_status"]
            passenger_count = parsed["passenger_count"]
            yield (ride_status, passenger_count)


def read_data_from_streaming_source(pipeline, input_subscription):
    return pipeline | "ReadFromPubSub" >> ReadFromPubSub(subscription=input_subscription)


def apply_transformation(pcoll, window_size, slide_window, label=""):
    return (pcoll | "ParseMessages" + label >> beam.ParDo(ParseMessages())
            | "ApplySlidingWindow" + label >> WindowInto(
                window.SlidingWindows(window_size, slide_window))
            | "SumPerKey" + label >> CombinePerKey(sum))


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help=
        'Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<ToPIC>."'
    )
    parser.add_argument(
        "--window_size",
        help='window_size"',
        default=30
    )
    parser.add_argument(
        "--dbtable",
        help='BQ table name',
    )
    parser.add_argument("--slide_size", 
                        help='Slide size',
                        default = 20)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    # Defining our pipeline and its steps
    with Pipeline(options=pipeline_options) as p:
        raw_data = read_data_from_streaming_source(p, known_args.input_subscription)
        transformed_data = apply_transformation(raw_data,
                                                known_args.window_size,
                                                known_args.slide_size)
        transformed_data | "Format" >> Map(lambda ele: {"record_insert_date": datetime.now().strftime("%Y-%m-%d-%H:%M:%S"), 
                                                        "ride_status":  ele[0], 
                                                        "total_passengers": ele[1]}) \
            | "Write to Sink" >> beam.io.WriteToBigQuery(
                                known_args.dbtable,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.
                                RETRY_ON_TRANSIENT_ERROR)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
