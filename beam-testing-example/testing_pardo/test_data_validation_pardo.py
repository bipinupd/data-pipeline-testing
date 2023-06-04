import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from data_validation_pardo import ParseAndValidate


class DataValidation_ParDoFn_Test(unittest.TestCase):

    def test_data_validation_By_ParDoFn(self):
        INPUT = [
            "2003-11-10T06:44:34,123,s1,p1,6,100",
            "2003-11-10T06:44:34,123,s1,p2,6,100",
            "2003-12-10T06:44:34,456,s1,p1,3",
            "2003-9-10T06:44:34,789,s3,p1,2,10.9a",
        ]
        ERROR_RECORD = [{
            'payload': '2003-12-10T06:44:34,456,s1,p1,3',
            'error': 'Invalid data. Does not match the number of elements',
            'error_step_id': 'error_at_stage_ParseAndValidate'
        }, {
            'payload': '2003-9-10T06:44:34,789,s3,p1,2,10.9a',
            'error': "could not convert string to float: '10.9a'",
            'error_step_id': 'error_at_stage_ParseAndValidate'
        }]
        VALID_RECORD = [('s1', ('2003-11-10T06:44:34', 'p1', 6, 100.0)),
                        ('s1', ('2003-11-10T06:44:34', 'p2', 6, 100.0))]
        with TestPipeline() as p:
            lines = p | beam.Create(INPUT)
            output = lines | 'Parse and Validate' >> beam.ParDo(
                ParseAndValidate()).with_outputs("main", "error")
            assert_that(output.main, equal_to(VALID_RECORD), label="main ..")
            assert_that(output.error, equal_to(ERROR_RECORD), label="err ..")
