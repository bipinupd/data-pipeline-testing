import logging
import unittest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import streaming_fixed_window
from apache_beam import Map


class MyDataflow_UnitTest(unittest.TestCase):

    def test_fixed_window_for_ws_2_and_4(self):
        stream = (TestStream().add_elements([
            TimestampedValue("a0", timestamp=0),
            TimestampedValue("b1", timestamp=1),
            TimestampedValue("b3", timestamp=3),
            TimestampedValue("c5", timestamp=5),
            TimestampedValue("d6", timestamp=6)
        ]))
        expected_output_ws_2 = [("key", ["a0", "b1"]), ("key", ["b3"]),
                                ("key", ["c5"]), ("key", ["d6"])]
        expected_output_ws_4 = [('key', ['a0', 'b1', 'b3']),
                                ('key', ['c5', 'd6'])]

        options = PipelineOptions(streaming=True)
        with TestPipeline(options=options) as pipeline:
            elements = (pipeline | stream)
            transformed_result_window_size_2 = streaming_fixed_window.apply_transformation(
                elements, 2, label="ws 2")
            transformed_result_window_size_4 = streaming_fixed_window.apply_transformation(
                elements, 4, label="ws 3")
            assert_that(transformed_result_window_size_2,
                        equal_to(expected_output_ws_2),
                        label="Output with window size 2")
            assert_that(transformed_result_window_size_4,
                        equal_to(expected_output_ws_4),
                        label="Output with window size 4")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
