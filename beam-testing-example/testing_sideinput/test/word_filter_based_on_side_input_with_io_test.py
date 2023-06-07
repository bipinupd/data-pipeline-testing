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

    def _getcrc32_for_objects(self,bucket_name, prefix_name):
        from google.cloud import storage
        client = storage.Client()
        if (bucket_name.startswith('gs://')):
            bucket_name=bucket_name.replace('gs://', '')
        blob_list = client.list_blobs(bucket_name, prefix=prefix_name)
        compute_output_crc = []
        for blob in blob_list:
            if blob.name != prefix_name:
                compute_output_crc.append(blob.crc32c)
        return compute_output_crc
    
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
            f"{self.test_bucket}/{self.pipeline_name}/{self._testMethodName}/output",
            'runner': "TestDataflowRunner"
        }
        word_filter_based_on_side_input.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=True)
        
        crc_output = self._getcrc32_for_objects(self.test_bucket,f"{self.pipeline_name}/{self._testMethodName}/output/")
        crc_expected_output = self._getcrc32_for_objects(self.test_bucket,f"{self.pipeline_name}/{self._testMethodName}/expected_output/")
        assert(crc_output.sort() == crc_expected_output.sort())

