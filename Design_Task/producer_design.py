import csv
import json
import glob
import os
from google.cloud import pubsub_v1

project_id = "steam-collector-485722-i5"
topic_id = "designTopic"

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

print(f"Reading data from Labels.csv...")

# 1. Open the CSV file
with open('Labels.csv', mode='r') as csv_file:
    # 2. Convert each row into a Dictionary (Key:Value pair)
    csv_reader = csv.DictReader(csv_file)

    for row in csv_reader:
        # 3. Serialize: Turn the dictionary into a generic text format (JSON)
        message_json = json.dumps(row)
        message_bytes = message_json.encode('utf-8')

        # 4. Publish the message
        publish_future = publisher.publish(topic_path, message_bytes)
        publish_future.result()  # Wait for it to confirm sending

        print(f"Published message: {message_json}")

print("All data sent successfully!")
