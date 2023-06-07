import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from custom_transform import CustomCountByKey


class Custom_Transform_Test(unittest.TestCase):

    def test_Custom_Tranfrom_Fxn(self):
        
        EXPECTED_OUTPUT = [('Beam', 2), ('Example', 1), ('open', 1)]
        # Create a test pipeline.
        with TestPipeline() as pipeline:
            average = pipeline | 'Create plant counts' >> beam.Create([
                "Beam", "Example", "open", "Beam"
            ]) | CustomCountByKey()

            assert_that(average, equal_to(EXPECTED_OUTPUT))