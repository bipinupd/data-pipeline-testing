import logging
import unittest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import streaming_fixed_window_with_lateness


class Streaming_Fixed_Window_With_Lateness_UnitTest(unittest.TestCase):

    def test_fixed_window_for_ws_2_and_4(self):
        stream = (TestStream().add_elements([
            TimestampedValue("a0", timestamp=0)
        ]).advance_watermark_to(10).add_elements([
            TimestampedValue("b1", timestamp=11)
        ]).advance_watermark_to(20).add_elements([
            TimestampedValue("b3", timestamp=15),
            TimestampedValue("c5", timestamp=23),
            TimestampedValue("d6", timestamp=60)
        ]).advance_watermark_to_infinity())
        expected_output_ws_2 = [('key', ['a0']), ('key', ['b1']),
                                ('key', ['b1', 'b3']), ('key', ['c5']),
                                ('key', ['d6'])]

        options = PipelineOptions(streaming=True)
        with TestPipeline(options=options) as pipeline:
            elements = (pipeline | stream)
            transformed_result_window_size_2 = streaming_fixed_window_with_lateness.apply_transformation(
                elements, 10, label="ws 2")
            assert_that(transformed_result_window_size_2,
                        equal_to(expected_output_ws_2),
                        label="Output with window size 2")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
