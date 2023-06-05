from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam import WindowInto
import logging
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import Pipeline
from apache_beam.transforms.window import FixedWindows


def read_data_from_streaming_source(pipeline):
    from apache_beam.testing.test_stream import TestStream
    from apache_beam.transforms.window import TimestampedValue
    stream = (TestStream().add_elements([
        TimestampedValue("item-at-0", timestamp=0),
        TimestampedValue("item-at-1", timestamp=1),
        TimestampedValue("item-at-3", timestamp=3),
        TimestampedValue("item-at-5", timestamp=5),
        TimestampedValue("item-at-6", timestamp=6)
    ]))
    return (pipeline | stream)


def apply_transformation(pcoll, window_size, label=""):
    return (pcoll | "Assign elements within pane same key" + label >>
            Map(lambda e: ("key", e))
            | "Window into FixedWindows" + label >> WindowInto(
                FixedWindows(size=window_size))
            | "Combine elements within pane by key" + label >> GroupByKey())


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    window_size = 4
    # Defining our pipeline and its steps
    with Pipeline(options=pipeline_options) as p:
        lines = read_data_from_streaming_source(p)
        transformed_data = apply_transformation(lines, window_size)
        transformed_data | "Print elements" >> Map(
            lambda ele: print('TRIGGER ', ele[1]))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
