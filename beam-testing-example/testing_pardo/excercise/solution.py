import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from divisible_by_pardo import DivisibleByDoFn


class Divisible_By_ParDoFn_Test(unittest.TestCase):

    def test_Divisible_By_ParDoFn(self):
        INITIAL_ARRY = [3, 7, 8, 9, 12, 14, 16, "1a"]
        DIVISIBLE_OUTPUT_5 = []
        NOT_DIVISIBLE_OUTPUT_5 = [3, 7, 8, 9, 12, 14, 16]
        DIVISIBLE_OUTPUT_6 = [12]
        NOT_DIVISIBLE_OUTPUT_6 = [3, 7, 8, 9, 14, 16]
        DIVISIBLE_OUTPUT_7 = [7, 14]
        NOT_DIVISIBLE_OUTPUT_7 = [3, 8, 9, 12, 16]
        ERROR = [{
            'payload': '1a',
            'error': 'not all arguments converted during string formatting',
            'error_step_id': 'DivisibleByDoFn'
        }]
        # Create a test pipeline.
        with TestPipeline() as p:
            input = p | beam.Create(INITIAL_ARRY)
            output_5 = input | "Check divisible by 5" >> beam.ParDo(
                DivisibleByDoFn(5)).with_outputs("divisible_by",
                                                 "not_divisible_by", "error")
            output_6 = input | "Check divisible by 6" >> beam.ParDo(
                DivisibleByDoFn(6)).with_outputs("divisible_by",
                                                 "not_divisible_by", "error")
            output_7 = input | "Check divisible by 7" >> beam.ParDo(
                DivisibleByDoFn(7)).with_outputs("divisible_by",
                                                 "not_divisible_by", "error")
            assert_that(output_5.divisible_by,
                        equal_to(DIVISIBLE_OUTPUT_5),
                        label="divisble_by 5")
            assert_that(output_5.not_divisible_by,
                        equal_to(NOT_DIVISIBLE_OUTPUT_5),
                        label="not divisble_by 5")
            assert_that(output_5.error,
                        equal_to(ERROR),
                        label="error on divisble_by 5")
            assert_that(output_6.divisible_by,
                        equal_to(DIVISIBLE_OUTPUT_6),
                        label="divisble_by 6")
            assert_that(output_6.not_divisible_by,
                        equal_to(NOT_DIVISIBLE_OUTPUT_6),
                        label="not divisble_by 6")
            assert_that(output_7.divisible_by,
                        equal_to(DIVISIBLE_OUTPUT_7),
                        label="divisble_by 7")
            assert_that(output_7.not_divisible_by,
                        equal_to(NOT_DIVISIBLE_OUTPUT_7),
                        label="not divisble_by 7")
