from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
from google.pubsub_v1.types import Encoding
from google.cloud import pubsub_v1
from faker import Faker
from google.cloud import storage


# Resolve the publish future in a separate thread.
def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    future.result()


def create_topic(project_id, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    try:
        topic = publisher.create_topic(request={"name": topic_path})
    except:
        print("Topic already exits")


def create_subscription(project_id, topic_id, subscription_id):
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id,
                                                     subscription_id)
    try:
        with subscriber:
            subscription = subscriber.create_subscription(request={
                "name": subscription_path,
                "topic": topic_path
            })
        print(f"Subscription created: {subscription}")
    except:
        print(f"Subscription already created")


def main(project_id, topic_id, gcs_bucket, object_file):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(object_file)
    data_string = str(blob.download_as_bytes().decode())
    lines = data_string.split('\n')
    for line in lines:
        if line.strip() != "":
            record = line.encode("utf-8")
            publisher.publish(topic_path, record)
