import json
import glob
import os
from google.cloud import pubsub_v1

project_id = "steam-collector-485722-i5"
subscription_id = "designTopic-sub"

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message):
    # 1. Deserialize: Convert the received bytes back into a Dictionary
    message_data = json.loads(message.data.decode('utf-8'))

    # 2. Print the values
    print(f"Received: {message_data}")

    # 3. Acknowledge
    message.ack()


print(f"Listening for messages on {subscription_id}...")

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback)
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
