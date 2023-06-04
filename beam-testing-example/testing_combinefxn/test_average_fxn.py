import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from average_fxn import AverageFn

class Average_Fxn_Test(unittest.TestCase):
        
    def test_Average_Fxn(self):
        EXPECTED_OUTPUT = [('carrot', 2.5),('apple', 1.0),('tomato', 4.0)]
        # Create a test pipeline.
        with TestPipeline() as pipeline:
            average = pipeline | 'Create plant counts' >> beam.Create([
                        ('carrot', 3),
                        ('carrot', 2),
                        ('apple', 1),
                        ('tomato', 4),
                        ('tomato', 5),
                        ('tomato', 3),
                    ]) | 'Average' >> beam.CombinePerKey(AverageFn())

            assert_that(average, equal_to(EXPECTED_OUTPUT))
