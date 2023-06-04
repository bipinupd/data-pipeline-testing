import logging

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from store_app.fxns.parse_and_validate import TupToDictStoreInfo 
from store_app.store_info_batch_pipeline import apply_transformations

class MyDataflow_UnitTest(unittest.TestCase):
    def test_apply_transformation(self):
        INPUT = ["2003-11-10T06:44:34,123,s1,p1,6,100","2003-11-10T06:44:34,123,s1,p2,6,100","2003-12-10T06:44:34,456,s1,p1,3,100","2003-9-10T06:44:34,789,s3,p1,2,10.9a",]
        EXPECTED_VALID_OUTPUT = [{'store_id': 's1', 'sales': 300.0, 'start_date': '2003-11-10T06:44:34', 'end_date': '2003-12-10T06:44:34', 'products': [{'product_name': 'p1', 'qty': 9}, {'product_name': 'p2', 'qty': 6}]}]
        EXPECTED_BAD_RECORD_OUTPUT = [{"payload": "2003-9-10T06:44:34,789,s3,p1,2,10.9a", "error": "could not convert string to float: '10.9a'", "error_step_id": "error_at_stage_ParseAndValidate"}]
        with TestPipeline() as p:
            data, err = apply_transformations(p | beam.Create(INPUT))
            data_table = data | 'Convert to Dict' >> beam.ParDo(TupToDictStoreInfo())
            assert_that(data_table, equal_to(EXPECTED_VALID_OUTPUT))
            assert_that(err, equal_to(EXPECTED_BAD_RECORD_OUTPUT), label="Error Output")
    def test_array_out_of_bound_goes_to_error_table(self):
        INPUT = ["2003-11-10T06:44:34,123,s1,p1,6"]
        EXPECTED_BAD_RECORD_OUTPUT = [{'payload': '2003-11-10T06:44:34,123,s1,p1,6', 'error': 'list index out of range', 'error_step_id': 'error_at_stage_ParseAndValidate'}]
        with TestPipeline() as p:
            data, err = apply_transformations(p | beam.Create(INPUT))
            assert_that(err, equal_to(EXPECTED_BAD_RECORD_OUTPUT), label="Error Output")

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()