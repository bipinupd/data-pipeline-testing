import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from divisible_by_pardo import DivisibleByDoFn


class Divisible_By_ParDoFn_Test(unittest.TestCase):

    def test_Divisible_By_ParDoFn(self):
        INITIAL_ARRY = [1, 3, 100, 8, 900]
        EVEN_OUTPUT = [100, 8, 900]
        ODD_OUTPUT = [1, 3]
        # Create a test pipeline.
        with TestPipeline() as p:
            input = p | beam.Create(INITIAL_ARRY)
            output = input | beam.ParDo(DivisibleByDoFn()).with_outputs(
                "divisible_by", "not_divisible_by")
            assert_that(output.not_divisible_by,
                        equal_to(ODD_OUTPUT),
                        label="divisble_by 2")
            assert_that(output.divisible_by,
                        equal_to(EVEN_OUTPUT),
                        label="not divisble_by 2")

    def test_Divisible_By_ParDoFn_For_Odd_Numbers_Only(self):
        INITIAL_ARRY = [1, 3, 5, 7]
        # Create a test pipeline.
        with TestPipeline() as p:
            input = p | beam.Create(INITIAL_ARRY)
            output = input | beam.ParDo(DivisibleByDoFn()).with_outputs(
                "divisible_by", "not_divisible_by")
            assert_that(output.divisible_by,
                        is_empty(),
                        label="No elements in INITIAL_ARRY divisble_by 2")
            assert_that(output.not_divisible_by, equal_to(INITIAL_ARRY))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
