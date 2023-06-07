import logging
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from hamcrest.core.core.allof import all_of

import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing import test_utils
import json
from fxn import pubsub_to_pubsub_taxirides
from nose.plugins.attrib import attr


class TaxiRidesApp_ITTest(unittest.TestCase):

    def setUp(self):
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.pipeline = "taxi_rides_pipeline"
        self.input_bucket_obj = f"{self.test_pipeline.get_option('test_bucket')}/{self.pipeline}/{self._testMethodName}/input/input.json" 
        self.project = self.test_pipeline.get_option('project')
        INPUT_TOPIC = self._testMethodName + "_input"
        OUTPUT_TOPIC = self._testMethodName + "_output"
        # Set up PubSub environment.
        from google.cloud import pubsub
        self.pub_client = pubsub.PublisherClient()
        self.input_topic = self.pub_client.create_topic(
            name=self.pub_client.topic_path(self.project, INPUT_TOPIC))
        self.output_topic = self.pub_client.create_topic(
            name=self.pub_client.topic_path(self.project, OUTPUT_TOPIC))
        self.sub_client = pubsub.SubscriberClient()
        self.input_sub = self.sub_client.create_subscription(
            name=self.sub_client.subscription_path(
                self.project, self._testMethodName + "_input" + "_sub"),
            topic=self.input_topic.name)
        self.output_sub = self.sub_client.create_subscription(
            name=self.sub_client.subscription_path(
                self.project, self._testMethodName + "_output" + "_sub"),
            topic=self.output_topic.name,
            ack_deadline_seconds=60)

    def _inject_messages(self, topic):
        from google.cloud import storage
        client = storage.Client()
        bucket_name = self.input_bucket_obj.split("/")[2]
        object_name = "/".join(self.input_bucket_obj.split("/")[3:])
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        content = blob.download_as_bytes()
        list_items = content.decode('utf-8').split("/n")
        for msg in list_items:
            self.pub_client.publish(topic,
                                    f'{msg}'.encode('utf-8'))

    def tearDown(self):
        test_utils.cleanup_subscriptions(self.sub_client,
                                         [self.input_sub, self.output_sub])
        test_utils.cleanup_topics(self.pub_client,
                                  [self.input_topic, self.output_topic])

    @attr('IT')
    def test_taxi_rides_end_to_end_happy_path(self):
        self._inject_messages(
            f"projects/{self.project}/topics/{self._testMethodName}_input")
        state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
        pubsub_msg_verifier = PubSubMessageMatcher(
            self.project,
            self.output_sub.name,
            expected_msg_len=2,
            timeout=400,
            strip_attributes=['id', 'timestamp'])
        extra_opts = {
            'streaming': True,
            'project': self.project,
            'runner': 'TestDataflowRunner',
            'input_subscription':
            f"projects/{self.project}/subscriptions/{self._testMethodName}_input_sub",
            'output_topic':
            f"projects/{self.project}/topics/{self._testMethodName}_output",
            'on_success_matcher': all_of(state_verifier, pubsub_msg_verifier),
        }
        pubsub_to_pubsub_taxirides.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts),
            save_main_session=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
