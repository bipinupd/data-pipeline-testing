import logging

import unittest
from apache_beam.testing.test_pipeline import TestPipeline
import os
from test.utils.test_utils import get_count
import store_app.store_info_batch_pipeline as my_dataflow
from nose.plugins.attrib import attr
import uuid
from datetime import datetime


class GCSToBQ_StoreInfo_ITTest(unittest.TestCase):
    PIPELINE_NAME = "store_info_batch_pipeline"

    def setUp(self):
        curr_dt = datetime.now()
        timestamp = int(round(curr_dt.timestamp()))
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.dataset_name = f"GCSToBQ_StoreInfo_ITTest_{timestamp}"
        self.project_id = self.test_pipeline.get_option('project')
        self.temp_location = self.test_pipeline.get_option('temp_location')
        self.staging_location = self.test_pipeline.get_option(
            'staging_location')
        self.test_gcs_bucket = self.test_pipeline.get_option('test_bucket')
        os.system(
            f"bq mk -f=true --dataset {self.project_id}:{self.dataset_name}")
        os.system(
            f"bq mk -f=true --table {self.project_id}:{self.dataset_name}.{self._testMethodName} schema/bq_curated.json"
        )
        os.system(
            f"bq mk -f=true --table {self.project_id}:{self.dataset_name}.{self._testMethodName}_err schema/bq_error.json"
        )
        os.system(
            f"bq query --nouse_legacy_sql 'truncate table `{self.project_id}`.{self.dataset_name}.{self._testMethodName}_err'"
        )
        os.system(
            f"bq query --nouse_legacy_sql 'truncate table `{self.project_id}`.{self.dataset_name}.{self._testMethodName}'"
        )

    def tearDown(self):
        os.system(
            f"bq rm -f {self.project_id}:{self.dataset_name}.{self._testMethodName}"
        )
        os.system(
            f"bq rm -f {self.project_id}:{self.dataset_name}.{self._testMethodName}_err"
        )
        os.system(
            f"bq rm -f=true --dataset {self.project_id}:{self.dataset_name}")

    @attr('IT')
    def test_e2e_happypath(self):
        extra_opts = {}
        extra_opts[
            'input'] = f"{self.test_gcs_bucket}/{self.PIPELINE_NAME}/{self._testMethodName}/input/*.csv"
        extra_opts[
            'output_table'] = f"{self.project_id}:{self.dataset_name}.{self._testMethodName}"
        extra_opts[
            'err_table'] = f"{self.project_id}:{self.dataset_name}.{self._testMethodName}_err"
        extra_opts[
            'temp_location'] = f"{self.temp_location}/{self._testMethodName}/{str(uuid.uuid4())}"
        extra_opts[
            'staging_location'] = f"{self.staging_location}/{self._testMethodName}/{str(uuid.uuid4())}"
        extra_opts['runner'] = 'TestDataflowRunner'
        my_dataflow.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=False,
        )

        valid_rec_count = get_count(
            f"SELECT count(*) as count_records FROM `{self.project_id}`.{self.dataset_name}.{self._testMethodName}",
            'count_records')
        self.assertEqual(valid_rec_count, 21)
        error_rec_count = get_count(
            f"SELECT count(*) as count_records FROM `{self.project_id}`.{self.dataset_name}.{self._testMethodName}_err",
            'count_records')
        self.assertEqual(error_rec_count, 0)

    @attr('IT')
    def test_e2e_with_invalid_records(self):
        extra_opts = {}
        extra_opts[
            'input'] = f"{self.test_gcs_bucket}/{self.PIPELINE_NAME}/{self._testMethodName}/input/*.csv"
        extra_opts[
            'output_table'] = f"{self.project_id}:{self.dataset_name}.{self._testMethodName}"
        extra_opts[
            'err_table'] = f"{self.project_id}:{self.dataset_name}.{self._testMethodName}_err"
        extra_opts[
            'temp_location'] = f"{self.temp_location}/{self._testMethodName}"
        extra_opts[
            'staging_location'] = f"{self.staging_location}/{self._testMethodName}"
        extra_opts['runner'] = 'TestDataflowRunner'

        my_dataflow.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=False,
        )

        valid_rec_count = get_count(
            f"SELECT count(*) as count_records FROM `{self.project_id}`.{self.dataset_name}.{self._testMethodName}",
            'count_records')
        self.assertEqual(valid_rec_count, 21)
        error_rec_count = get_count(
            f"SELECT count(*) as count_records FROM `{self.project_id}`.{self.dataset_name}.{self._testMethodName}_err",
            'count_records')
        self.assertEqual(error_rec_count, 1)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
