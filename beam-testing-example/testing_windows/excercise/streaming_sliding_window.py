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
import json


class ParseMessages(beam.DoFn):

    def process(self, element):
        parsed = json.loads(element.decode("utf-8"))
        if parsed["ride_status"].lower() != "enroute":
            ride_status = parsed["ride_status"]
            passenger_count = parsed["passenger_count"]
            yield (ride_status, passenger_count)


def read_data_from_streaming_source(pipeline, INPUT_TOPIC):
    return pipeline | "ReadFromPubSub" >> ReadFromPubSub(topic=INPUT_TOPIC)


def apply_transformation(pcoll, window_size, slide_window, label=""):
    return (pcoll | "ParseMessages" + label >> beam.ParDo(ParseMessages())
            | "ApplySlidingWindow" + label >> WindowInto(
                window.SlidingWindows(window_size, slide_window))
            | "SumPerKey" + label >> CombinePerKey(sum))


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help=
        'Input PubSub topic of the form "projects/<PROJECT>/topics/<ToPIC>."',
        default=INPUT_TOPIC,
    )
    parser.add_argument(
        "--window_size",
        help='window_size"',
    )
    parser.add_argument("--slide_size", help='Slide size"')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    int()
    # Defining our pipeline and its steps
    with Pipeline(options=pipeline_options) as p:
        raw_data = read_data_from_streaming_source(p)
        transformed_data = apply_transformation(raw_data,
                                                int(options.window_size),
                                                int(options.slide_window))
        transformed_data | "Print elements" >> Map(lambda ele: print(
            'Ride Status ', ele[0], "Total Passengers: ", ele[1]))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
