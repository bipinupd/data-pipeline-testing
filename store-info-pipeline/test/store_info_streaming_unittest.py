import logging

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from store_app.fxns.parse_and_validate import TupToDictStoreInfo 
from store_app.store_info_streaming_pipeline import apply_transformations

# class StoreInfoStreaming_UnitTest(unittest.TestCase):
#     def test_apply_transformation(self):
#         INPUT = ["2003-11-10T06:44:34,123,s1,p1,6,100","2003-11-10T06:44:34,123,s1,p2,6,100","2003-12-10T06:44:34,456,s1,p1,3,100","2003-9-10T06:44:34,789,s3,p1,2,10.9a",]
#         EXPECTED_VALID_OUTPUT = [{'store_id': 's1', 'sales': 300.0, 'start_date': '2003-11-10T06:44:34', 'end_date': '2003-12-10T06:44:34', 'products': [{'product_name': 'p1', 'qty': 9}, {'product_name': 'p2', 'qty': 6}]}]
#         EXPECTED_BAD_RECORD_OUTPUT = [{"payload": "2003-9-10T06:44:34,789,s3,p1,2,10.9a", "error": "could not convert string to float: '10.9a'", "error_step_id": "error_at_stage_ParseAndValidate"}]
#         with TestPipeline() as p:
#             data, err = apply_transformations(p | beam.Create(INPUT))
#             data | beam.Map(print)
#             # data_table = data | 'Convert to Dict' >> beam.ParDo(TupToDictStoreInfo())
#             # assert_that(data_table, equal_to(EXPECTED_VALID_OUTPUT))
#             # assert_that(err, equal_to(EXPECTED_BAD_RECORD_OUTPUT), label="Error Output")

# if __name__ == '__main__':
#   logging.getLogger().setLevel(logging.DEBUG)
#   unittest.main()
from apache_beam import Pipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

stream = (
    TestStream().add_elements([
    TimestampedValue("2003-11-10T06:44:34,123,s1,p1,6,100".encode("utf-8"), timestamp=0),
    TimestampedValue("2003-11-10T06:44:3,123,s1,p2,6,100".encode("utf-8"), timestamp=1),
    TimestampedValue("2003-9-10T06:44:34,789,s3,p1,2,10.9".encode("utf-8"), timestamp=3),
    TimestampedValue("2003-12-10T06:44:34,456,s3,p1,3,100".encode("utf-8"), timestamp=5),
])
)
with Pipeline() as p:
    data, err = apply_transformations(p | stream)
    data | "adsdf" >> beam.Map(print)
    err | "pp" >> beam.Map(print)