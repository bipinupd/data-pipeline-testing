import logging
from apache_beam.options.pipeline_options import PipelineOptions
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from fxn import pubsub_to_pubsub_taxirides


class MyDataflow_UnitTest(unittest.TestCase):

    def test_apply_transformation(self):
        options = PipelineOptions(streaming=True)
        stream = (
            TestStream().add_elements([
                TimestampedValue(
                    '{"ride_status": "finished", "timestamp": "2020-03-27T21:32:51.48098-04:00", "passenger_count": 3}'
                    .encode("utf-8"),
                    timestamp=0),
                TimestampedValue(
                    '{"ride_status": "finished", "timestamp": "2020-03-27T21:32:52.48098-04:00", "passenger_count": 1}'
                    .encode("utf-8"),
                    timestamp=4),
                TimestampedValue(
                    '{"ride_status": "finished", "timestamp": "2020-03-27T21:32:59.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=6)
            ]).advance_watermark_to(6)  # move watermark to epoch sec 6
            .advance_processing_time(6)  # move processed time to epoch sec 6
            .add_elements([
                TimestampedValue(
                    '{"ride_status": "finished", "timestamp": "2020-03-27T21:31:51.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=8),
                TimestampedValue(
                    '{"ride_status": "finished", "timestamp": "2020-03-27T21:31:59.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=9)
            ]).advance_watermark_to(11).advance_processing_time(11).
            add_elements([
                TimestampedValue(
                    '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:33:51.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=29),
                TimestampedValue(
                    '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:33:59.48098-04:00", "passenger_count": 13}'
                    .encode("utf-8"),
                    timestamp=19)
            ]).advance_watermark_to_infinity())
        with TestPipeline(options=options) as test_pipeline:
            OUTPUT = [{
                'ride_status': 'finished',
                'passenger_count': 43,
                'min_timestamp': '2020-03-27T21:31:51.48098-04:00',
                'max_timestamp': '2020-03-27T21:32:59.48098-04:00'
            }, {
                'ride_status': 'ENROUTE',
                'passenger_count': 26,
                'min_timestamp': '2020-03-27T21:33:51.48098-04:00',
                'max_timestamp': '2020-03-27T21:33:59.48098-04:00'
            }, {
                'ride_status': 'finished',
                'passenger_count': 43,
                'min_timestamp': '2020-03-27T21:31:51.48098-04:00',
                'max_timestamp': '2020-03-27T21:32:59.48098-04:00'
            }, {
                'ride_status': 'ENROUTE',
                'passenger_count': 26,
                'min_timestamp': '2020-03-27T21:33:51.48098-04:00',
                'max_timestamp': '2020-03-27T21:33:59.48098-04:00'
            }]
            OUTPUT_ERR = []
            lines = test_pipeline | stream
            pcoll, err = pubsub_to_pubsub_taxirides.apply_transformations(lines)
            assert_that(pcoll, equal_to(OUTPUT), label="Output")
            assert_that(err, equal_to(OUTPUT_ERR), label="Output Error")

    def test_apply_transformation_without_passenger_count_bad_records(self):
        options = PipelineOptions(streaming=True)
        stream = (TestStream().add_elements([
            TimestampedValue(
                '{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:32:51.48098-04:00",}'
                .encode("utf-8"),
                timestamp=0),
        ]).advance_watermark_to_infinity())
        with TestPipeline(options=options) as test_pipeline:
            OUTPUT = []
            ERR_OUTPUT = [{
                'payload':
                    b'{"ride_status": "ENROUTE", "timestamp": "2020-03-27T21:32:51.48098-04:00",}',
                'error':
                    'Expecting property name enclosed in double quotes: line 1 column 75 (char 74)',
                'error_step_id':
                    'ERROR_IN_STEP_ParseMessages'
            }]
            lines = test_pipeline | stream
            pcoll, err = pubsub_to_pubsub_taxirides.apply_transformations(lines)
            assert_that(pcoll, equal_to(OUTPUT), label="Output")
            assert_that(err, equal_to(ERR_OUTPUT), label="Error Output")

    def test_apply_transformation_without_timestamp_bad_records(self):
        options = PipelineOptions(streaming=True)
        stream = (TestStream().add_elements([
            TimestampedValue(
                '{"ride_status": "ENROUTE", "passenger_count": 34}'.encode(
                    "utf-8"),
                timestamp=0),
        ]).advance_watermark_to_infinity())
        with TestPipeline(options=options) as test_pipeline:
            OUTPUT = []
            ERR_OUTPUT = [{
                'payload': b'{"ride_status": "ENROUTE", "passenger_count": 34}',
                'error': "'timestamp'",
                'error_step_id': 'ERROR_IN_STEP_ParseMessages'
            }]
            lines = test_pipeline | stream
            pcoll, err = pubsub_to_pubsub_taxirides.apply_transformations(lines)
            assert_that(pcoll, equal_to(OUTPUT), label="Output")
            assert_that(err, equal_to(ERR_OUTPUT), label=" Error Output")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
