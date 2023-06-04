import logging

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from store_app.fxns.parse_and_validate import ParseAndValidate


class ParseAndValidate_StoreInfo_UnitTest(unittest.TestCase):

    def test_apply_parse_validate_transformation_with_bytes(self):
        INPUT = [
            "2003-11-10T06:44:34,123,s1,p1,6,100".encode("utf-8"),
            "2003-11-10T06:44:34,123,s1,p2,6,100".encode("utf-8"),
            "2003-12-10T06:44:34,456,s1,p1,3,100".encode("utf-8"),
            "2003-9-10T06:44:34,789,s3,p1,2,10.9a".encode("utf-8"),
        ]
        EXPECTED_VALID_OUTPUT = [
            ('s1', ('2003-11-10T06:44:34', 'p1', 6, 100.0)),
            ('s1', ('2003-11-10T06:44:34', 'p2', 6, 100.0)),
            ('s1', ('2003-12-10T06:44:34', 'p1', 3, 100.0))
        ]
        EXPECTED_BAD_RECORD_OUTPUT = [{
            'payload':
            b'2003-9-10T06:44:34,789,s3,p1,2,10.9a',
            'error':
            "could not convert string to float: '10.9a'",
            'error_step_id':
            'error_at_stage_ParseAndValidate'
        }]
        with TestPipeline() as p:
            lines = p | beam.Create(INPUT)
            data, err = lines | "parse and validate" >> beam.ParDo(
                ParseAndValidate()).with_outputs(ParseAndValidate.ERR_REC,
                                                 main='main')
            assert_that(data, equal_to(EXPECTED_VALID_OUTPUT))
            assert_that(err,
                        equal_to(EXPECTED_BAD_RECORD_OUTPUT),
                        label="Error Output")

    def test_apply_parse_validate_transformation_with_str(self):
        INPUT = [
            "2003-11-10T06:44:34,123,s1,p1,6,100",
            "2003-11-10T06:44:34,123,s1,p2,6,100",
            "2003-12-10T06:44:34,456,s1,p1,3,100",
            "2003-9-10T06:44:34,789,s3,p1,2,10.9a"
        ]
        EXPECTED_VALID_OUTPUT = [
            ('s1', ('2003-11-10T06:44:34', 'p1', 6, 100.0)),
            ('s1', ('2003-11-10T06:44:34', 'p2', 6, 100.0)),
            ('s1', ('2003-12-10T06:44:34', 'p1', 3, 100.0))
        ]
        EXPECTED_BAD_RECORD_OUTPUT = [{
            'payload':
            '2003-9-10T06:44:34,789,s3,p1,2,10.9a',
            'error':
            "could not convert string to float: '10.9a'",
            'error_step_id':
            'error_at_stage_ParseAndValidate'
        }]
        with TestPipeline() as p:
            lines = p | beam.Create(INPUT)
            data, err = lines | "parse and validate" >> beam.ParDo(
                ParseAndValidate()).with_outputs(ParseAndValidate.ERR_REC,
                                                 main='main')
            assert_that(data, equal_to(EXPECTED_VALID_OUTPUT))
            assert_that(err,
                        equal_to(EXPECTED_BAD_RECORD_OUTPUT),
                        label="Error Output")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
