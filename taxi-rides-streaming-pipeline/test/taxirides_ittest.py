import logging
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from hamcrest.core.core.allof import all_of
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher

import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing import test_utils
from fxn import pubsub_to_pubsub_taxirides
from nose.plugins.attrib import attr
import os
from datetime import datetime


class TaxiRidesApp_ITTest(unittest.TestCase):

    def setUp(self):
        curr_dt = datetime.now()
        timestamp = int(round(curr_dt.timestamp()))
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.dataset_name = f"taxi_rides_app_{timestamp}"
        self.project_id = self.test_pipeline.get_option('project')
        self.pipeline = "taxi_rides_pipeline"
        self.input_bucket_obj = f"{self.test_pipeline.get_option('test_bucket')}/{self.pipeline}/{self._testMethodName}/input/input.json"
        self.input_topic_name = f"{self._testMethodName}_{timestamp}_input"
        self.input_topic_sub_name = f"{self._testMethodName}_{timestamp}_input_sub"
        self.output_topic_sub_name = f"{self._testMethodName}_{timestamp}_output_sub"
        self.output_topic_name = f"{self._testMethodName}_{timestamp}_output"
        # Set up PubSub environment.
        from google.cloud import pubsub
        self.pub_client = pubsub.PublisherClient()
        self.input_topic = self.pub_client.create_topic(
            name=self.pub_client.topic_path(self.project_id,
                                            self.input_topic_name))
        self.output_topic = self.pub_client.create_topic(
            name=self.pub_client.topic_path(self.project_id,
                                            self.output_topic_name))
        self.sub_client = pubsub.SubscriberClient()
        self.input_sub = self.sub_client.create_subscription(
            name=self.sub_client.subscription_path(self.project_id,
                                                   self.input_topic_sub_name),
            topic=self.input_topic.name)
        self.output_sub = self.sub_client.create_subscription(
            name=self.sub_client.subscription_path(self.project_id,
                                                   self.output_topic_sub_name),
            topic=self.output_topic.name,
            ack_deadline_seconds=60)
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

    def _inject_messages(self, topic):
        from google.cloud import storage
        import json
        client = storage.Client()
        bucket_name = self.input_bucket_obj.split("/")[2]
        object_name = "/".join(self.input_bucket_obj.split("/")[3:])
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        content = blob.download_as_bytes()
        list_items = content.decode('utf-8').split("\n")
        for msg in list_items:
            if (msg != ""):
                self.pub_client.publish(topic,
                                        f'{msg}'.encode('utf-8'),
                                        ts=json.loads(msg)["timestamp"])

    def tearDown(self):
        test_utils.cleanup_subscriptions(self.sub_client,
                                         [self.input_sub, self.output_sub])
        test_utils.cleanup_topics(self.pub_client,
                                  [self.input_topic, self.output_topic])
        os.system(
            f"bq rm -f {self.project_id}:{self.dataset_name}.{self._testMethodName}"
        )
        os.system(
            f"bq rm -f {self.project_id}:{self.dataset_name}.{self._testMethodName}_err"
        )
        os.system(f"bq rm -r -f -d {self.project_id}:{self.dataset_name}")

    @attr('IT')
    def test_taxi_rides_end_to_end_happy_path(self):
        self._inject_messages(
            f"projects/{self.project_id}/topics/{self.input_topic_name}")
        state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
        pubsub_msg_verifier = PubSubMessageMatcher(self.project_id,
                                                   self.output_sub.name,
                                                   expected_msg_len=8,
                                                   timeout=180)
        extra_opts = {
            'streaming':
                True,
            'project':
                self.project_id,
            'runner':
                'TestDataflowRunner',
            'output_table':
                f"{self.project_id}:{self.dataset_name}.{self._testMethodName}",
            'output_err_table':
                f"{self.project_id}:{self.dataset_name}.{self._testMethodName}_err",
            'input_subscription':
                f"projects/{self.project_id}/subscriptions/{self.input_topic_sub_name}",
            'output_topic':
                f"projects/{self.project_id}/topics/{self.output_topic_name}",
            'on_success_matcher':
                all_of(state_verifier, pubsub_msg_verifier),
        }
        pubsub_to_pubsub_taxirides.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=True)

    @attr('IT')
    def test_taxi_rides_end_to_end_with_bad_records(self):
        ERROR_DATA_CHECKSUM = "00405d2cbda8ffab943fb6ed701056d29c026473"
        OUTPUT_DATA_CHECKSUM = "ba175fc2146ae1d0707aa0f6d43cfc8bd3dde037"
        self._inject_messages(
            f"projects/{self.project_id}/topics/{self.input_topic_name}")
        state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
        error_data_checksum = BigqueryMatcher(
            project=self.project_id,
            query=
            f"select * from {self.dataset_name}.{self._testMethodName}_err order by error_step_id, error, payload",
            checksum=ERROR_DATA_CHECKSUM)
        output_data_checksum = BigqueryMatcher(
            project=self.project_id,
            query=
            f"select * from {self.dataset_name}.{self._testMethodName} order by ride_status, passenger_count, min_timestamp, max_timestamp",
            checksum=OUTPUT_DATA_CHECKSUM)
        extra_opts = {
            'streaming':
                True,
            'project':
                self.project_id,
            'runner':
                'TestDataflowRunner',
            'output_table':
                f"{self.project_id}:{self.dataset_name}.{self._testMethodName}",
            'output_err_table':
                f"{self.project_id}:{self.dataset_name}.{self._testMethodName}_err",
            'input_subscription':
                f"projects/{self.project_id}/subscriptions/{self.input_topic_sub_name}",
            'output_topic':
                f"projects/{self.project_id}/topics/{self.output_topic_name}",
            'on_success_matcher':
                all_of(state_verifier, error_data_checksum,
                       output_data_checksum),
        }
        pubsub_to_pubsub_taxirides.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
