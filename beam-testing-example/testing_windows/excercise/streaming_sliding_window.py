from apache_beam import WindowInto
import logging
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import Pipeline
from apache_beam.transforms import window
import json
from apache_beam.io.gcp.pubsub import ReadFromPubSub


def read_data_from_streaming_source(pipeline, input_subscription):
    return pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        subscription=input_subscription)


def apply_transformation(pcoll, window_size, slide_window, label=""):
    return (pcoll | "Add timestamps" + label >>
            beam.Map(lambda x: window.TimestampedValue(x, x["timestamp"])) |
            "To KV" + label >> beam.Map(lambda x: (x["player"], x["score"])) |
            "ApplySlidingWindow" + label >> WindowInto(
                window.SlidingWindows(window_size, slide_window)) |
            "Total Per Key" + label >> beam.CombinePerKey(sum))


def write_to_sink(pcoll, output_topic):
    return  pcoll \
          | 'encode' >> beam.Map(lambda x: json.dumps(x).encode('utf-8')).with_output_types(bytes) \
          | "Write to Sink" >> beam.io.WriteToPubSub(output_topic)


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help=
        'Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<ToPIC>."'
    )
    parser.add_argument("--window_size", help='window_size"', default=30)
    parser.add_argument(
        "--output_topic",
        help='BQ table name',
    )
    parser.add_argument("--slide_size", help='Slide size', default=20)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    # Defining our pipeline and its steps
    with Pipeline(options=pipeline_options) as p:
        raw_data = read_data_from_streaming_source(
            p, known_args.input_subscription)
        agg_data = apply_transformation(raw_data, known_args.window_size,
                                        known_args.slide_size)
        write_to_sink(agg_data, known_args.output_topic)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
