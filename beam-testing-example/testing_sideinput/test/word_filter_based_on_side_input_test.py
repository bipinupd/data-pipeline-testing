import apache_beam as beam
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from fxn.word_filter_based_on_side_input import apply_transformations
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class WordExtractingDoFn_Pipeline_Test(unittest.TestCase):
    def test_data_validation_By_ParDoFn(self):
        INPUT = [
            "Apache Beam is an open source, unified model for defining both batch and streaming data-parallel processing pipelines.",
            "Using one of the open source Beam SDKs, you build a program that defines the pipeline.",
            "The pipeline is then executed by one of Beam’s supported distributed processing back-ends, which include Apache Flink, Apache Spark, and Google Cloud Dataflow.",
           "Beam is particularly useful for embarrassingly parallel data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel.",
            "You can also use Beam for Extract, Transform, and Load (ETL) tasks and pure data integration. These tasks are useful for moving data between different storage media and data sources, transforming data into a more desirable format, or loading data onto a new system."
        ]

        VALID_RECORD_SMALL = [('Beam', 5), ('open', 2), ('model', 1), ('both', 1), ('batch', 1), ('data', 8), ('Using', 1), ('SDKs', 1), ('build', 1), ('that', 2), ('then', 1), ('back', 1), ('ends', 1), ('which', 2), ('Flink', 1), ('Spark', 1), ('Cloud', 1), ('tasks', 3), ('into', 2), ('many', 1), ('also', 1), ('Load', 1), ('pure', 1), ('These', 1), ('media', 1), ('more', 1), ('onto', 1)]
        VALID_RECORD_LARGE = [('Apache', 3), ('source', 2), ('unified', 1), ('defining', 1), ('streaming', 1), ('parallel', 3), ('processing', 3), ('pipelines', 1), ('program', 1), ('defines', 1), ('pipeline', 2), ('executed', 1), ('supported', 1), ('distributed', 1), ('include', 1), ('Google', 1), ('Dataflow', 1), ('particularly', 1), ('useful', 2), ('embarrassingly', 1), ('problem', 1), ('decomposed', 1), ('smaller', 1), ('bundles', 1), ('processed', 1), ('independently', 1), ('Extract', 1), ('Transform', 1), ('integration', 1), ('moving', 1), ('between', 1), ('different', 1), ('storage', 1), ('sources', 1), ('transforming', 1), ('desirable', 1), ('format', 1), ('loading', 1), ('system', 1)]
        with TestPipeline() as p:
            lines = p | beam.Create(INPUT)
            small_words, larger_than_average = apply_transformations(lines,4,5)
            assert_that(small_words, equal_to(VALID_RECORD_SMALL), label="main ..")
            assert_that(larger_than_average, equal_to(VALID_RECORD_LARGE), label="err ..")

