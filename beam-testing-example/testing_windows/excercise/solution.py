import logging
import unittest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import streaming_sliding_window
from apache_beam import Map


class MyDataflow_UnitTest(unittest.TestCase):

    def test_sliding_window(self):
        options = PipelineOptions(streaming=True)
        stream = (
            TestStream().add_elements([
                TimestampedValue(
                    '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:32:51.48098-04:00", "passenger_count": 3}'
                    .encode("utf-8"),
                    timestamp=0),
                TimestampedValue(
                    '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:33:52.48098-04:00", "passenger_count": 1}'
                    .encode("utf-8"),
                    timestamp=4),
                TimestampedValue(
                    '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:33:58.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=6)
            ]).advance_watermark_to(60)  # move watermark to epoch sec 6
            .advance_processing_time(60)  # move processed time to epoch sec 6
            .add_elements([
                TimestampedValue(
                    '{"ride_status": "finished", "timestamp": "2020-03-27T21:34:51.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=120),
                TimestampedValue(
                    '{"ride_status": "finished", "timestamp": "2020-03-28T22:02:20.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=320)
            ]).advance_watermark_to(120).advance_processing_time(180).
            add_elements([
                TimestampedValue(
                    '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T22:33:51.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=480),
                TimestampedValue(
                    '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T22:50:59.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=590)
            ]).advance_watermark_to_infinity())
        with TestPipeline(options=options) as test_pipeline:
            OUTPUT_180_120 = [('finished', 13), ('finished', 13),
                              ('finished', 13)]
            OUTPUT_600_480 = [('finished', 26)]
            lines = test_pipeline | stream
            pcoll_180_120 = streaming_sliding_window.apply_transformation(
                lines, 180, 120, label="ws 180_120")
            pcoll = streaming_sliding_window.apply_transformation(
                lines, 600, 480, label="ws 600_480")
            assert_that(pcoll,
                        equal_to(OUTPUT_600_480),
                        label="Output ws 600_480")
            assert_that(pcoll_180_120,
                        equal_to(OUTPUT_180_120),
                        label="Output ws 180_120")

    def test_sliding_window_no_enroute_rides_included(self):
        options = PipelineOptions(streaming=True)
        stream = (TestStream().add_elements([
            TimestampedValue(
                '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:32:51.48098-04:00", "passenger_count": 3}'
                .encode("utf-8"),
                timestamp=0),
            TimestampedValue(
                '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:33:52.48098-04:00", "passenger_count": 1}'
                .encode("utf-8"),
                timestamp=4),
            TimestampedValue(
                '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:34:59.48098-04:00", "passenger_count": 13}'
                .encode("utf-8"),
                timestamp=6)
        ]).advance_watermark_to(60).advance_processing_time(60).add_elements([
            TimestampedValue(
                '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T22:33:51.48098-04:00", "passenger_count": 13}'
                .encode("utf-8"),
                timestamp=480),
            TimestampedValue(
                '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T22:50:59.48098-04:00", "passenger_count": 13}'
                .encode("utf-8"),
                timestamp=590)
        ]).advance_watermark_to_infinity())
        with TestPipeline(options=options) as test_pipeline:
            OUTPUT = []
            lines = test_pipeline | stream
            pcoll = streaming_sliding_window.apply_transformation(
                lines, 120, 60)
            assert_that(pcoll, equal_to(OUTPUT), label="Output")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
