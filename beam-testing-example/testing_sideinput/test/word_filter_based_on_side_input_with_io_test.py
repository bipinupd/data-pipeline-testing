import logging
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from fxn import word_filter_based_on_side_input 
import os
class WordExtractingDoFn_Pipeline_WithIO_Test(unittest.TestCase):
    
    def setUp(self):
        self.pipeline_name = "word_extraction_pipeline"
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.project = self.test_pipeline.get_option('project')
        self.test_bucket = self.test_pipeline.get_option('test_bucket')

    def tearDown(self):
        os.system(
            f"gsutil rm -rf {self.test_bucket}/{self.pipeline_name}/{self._testMethodName}/output/"
        )
    def test_end_to_end_happy_path(self):
        extra_opts = {
            'project': self.project,
            'input':
            f"{self.test_bucket}/{self.pipeline_name}/{self._testMethodName}/input/*.txt",
            'output':
            f"{self.test_bucket}/{self.pipeline_name}/{self._testMethodName}/output"
        }
        word_filter_based_on_side_input.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=True)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
