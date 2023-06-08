import logging
import unittest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import streaming_sliding_window
import apache_beam as beam


class Streaming_Sliding_Window_Test(unittest.TestCase):

    def test_sliding_window(self):
        options = PipelineOptions(streaming=True)
        scores = [
            {
                "player": "Cristina",
                "score": 2000,
                "timestamp": 10
            },
            {
                "player": "Cristina",
                "score": 2000,
                "timestamp": 50
            },
            {
                "player": "Marina",
                "score": 3000,
                "timestamp": 110
            },
            {
                "player": "Juan",
                "score": 2000,
                "timestamp": 90
            },
            {
                "player": "Cristina",
                "score": 2000,
                "timestamp": 80
            },
            {
                "player": "Juan",
                "score": 1000,
                "timestamp": 100
            },
        ]
        OUTPUT_70_60 = [('Marina', 3000), ('Cristina', 4000),
                        ('Cristina', 2000), ('Juan', 3000)]
        with TestPipeline(options=options) as test_pipeline:
            pcoll = test_pipeline | beam.Create(scores)
            pcoll_70_60 = streaming_sliding_window.apply_transformation(
                pcoll, 70, 60, label="ws 70_60")
            assert_that(pcoll_70_60,
                        equal_to(OUTPUT_70_60),
                        label="Output ws 70_60")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
