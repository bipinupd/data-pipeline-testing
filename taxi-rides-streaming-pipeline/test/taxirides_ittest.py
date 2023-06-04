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


class MyDataflow_ITTest(unittest.TestCase):

    def setUp(self):
        self.test_pipeline = TestPipeline(is_integration_test=True)
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
        self.list_msg = [
            {
                "ride_status": "finished",
                "timestamp": "2020-03-27T21:32:51.48098-04:00",
                "passenger_count": 3
            },
            {
                "ride_status": "enroute",
                "timestamp": "2020-03-27T21:34:51.48098-04:00",
                "passenger_count": 3
            },
        ]
        for msg in self.list_msg:
            self.pub_client.publish(topic,
                                    f'{json.dumps(msg)}'.encode('utf-8'))

    def tearDown(self):
        test_utils.cleanup_subscriptions(self.sub_client,
                                         [self.input_sub, self.output_sub])
        test_utils.cleanup_topics(self.pub_client,
                                  [self.input_topic, self.output_topic])

    @attr('IT')
    def test_apply_transformation(self):
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
