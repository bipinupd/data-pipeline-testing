import logging

import unittest
import os
import test.utils.generate_records_for_pubsub as pubsubutil
import store_app.store_info_streaming_pipeline as my_dataflow
import uuid
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from hamcrest.core.core.allof import all_of
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.runners.runner import PipelineState


class PUBSUB_TO_BQ_STOREINFO_ITTest(unittest.TestCase):

    def setUp(self):
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.DATASET_NAME = "PUBSUBTOBQ_STORE_INFO"
        self.PIPELINE_NAME = "pubsub_to_bq_sales_record"
        self.PROJECT_ID = self.test_pipeline.get_option('project')
        self.TEMP_LOCATION = self.test_pipeline.get_option('temp_location')
        self.STAGING_LOCATION = self.test_pipeline.get_option(
            'staging_location')
        self.TEST_GCS_BUCKET = self.test_pipeline.get_option('input')
        os.system(
            f"bq mk -f=true --dataset {self.PROJECT_ID}:{self.DATASET_NAME}")
        os.system(
            f"bq mk -f=true --table {self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName} schema/bq_curated.json"
        )
        os.system(
            f"bq mk -f=true --table {self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}_err schema/bq_error.json"
        )
        os.system(
            f"bq query --nouse_legacy_sql 'truncate table {self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}_err'"
        )
        os.system(
            f"bq query --nouse_legacy_sql 'truncate table {self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}'"
        )
        pubsubutil.create_topic(self.PROJECT_ID, self._testMethodName)
        pubsubutil.create_subscription(self.PROJECT_ID, self._testMethodName,
                                       self._testMethodName + "_subscription")
        pubsubutil.main(
            self.PROJECT_ID, self._testMethodName, self.TEST_GCS_BUCKET,
            self.PIPELINE_NAME + "/" + self._testMethodName + "/file_0.csv")

    def tearDown(self):
        os.system(
            f"bq rm -f {self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}"
        )
        os.system(
            f"bq rm -f {self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}_err"
        )

    def test_e2e_pubsub_to_bq_happypath(self):
        EXPECTED_BQ_CHECKSUM = 'rDR41po8gfpi5g9cNpYWWk5easQ='
        state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
        validation_query = f'SELECT CAST(count(*) as STRING) FROM {self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}'
        bq_sessions_verifier = BigqueryMatcher(self.PROJECT_ID,
                                               validation_query,
                                               EXPECTED_BQ_CHECKSUM)
        extra_opts = {
            'streaming':
            True,
            'project':
            self.PROJECT_ID,
            'input_subscription':
            f"projects/{self.PROJECT_ID}/subscriptions/{self._testMethodName}_subscription",
            'output_table':
            f"{self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}",
            'err_table':
            f"{self.PROJECT_ID}:{self.DATASET_NAME}.{self._testMethodName}_err",
            'on_success_matcher':
            all_of(state_verifier, bq_sessions_verifier),
            'temp_location':
            f"{self.TEMP_LOCATION}/{self._testMethodName}/{str(uuid.uuid4())}",
            'staging_location':
            f"{self.STAGING_LOCATION}/{self._testMethodName}/{str(uuid.uuid4())}",
        }
        my_dataflow.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
