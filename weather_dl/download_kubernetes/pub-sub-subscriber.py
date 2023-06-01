# -*- coding: utf-8 -*-
"""Pub/Sub pull example on Google Kubernetes Engine.

This program pulls messages from a Cloud Pub/Sub topic and
download & upload the ECMWF data into GCS.
"""

import datetime
from google.cloud import pubsub_v1
from os import path
import yaml
import json
import uuid
from kubernetes import client, config

# [START gke_pubsub_pull]
# [START container_pubsub_pull]

def main():
    """Continuously pull messages from subsciption"""
    while True:
        subscriber = pubsub_v1.SubscriberClient()
        # `projects/{project_id}/subscriptions/{subscription_id}`
        subscription_path = "XXXXXXXXXXXXXXXXXXX"
        MAX_NUM_MESSAGES = 1    
        print(f"Listening for messages on {subscription_path}..\n")
        # Wrap the subscriber in a 'with' block to automatically call close() to
        # close the underlying gRPC channel when done.
        with subscriber:
            # The subscriber pulls a specific number of messages. The actual
            # number of messages pulled may be smaller than max_messages.
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": MAX_NUM_MESSAGES},
            )

            if len(response.received_messages) == 0:
                continue

            for received_message in response.received_messages:
                print("[{0}] Received message: ID={1} Data={2}".format(
                    datetime.datetime.now(),
                    received_message.message.message_id,
                    received_message.message.data))
                
                # Acknowledges the received messages so they will not be sent again.
                subscriber.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
                )
                # TODO : Check if this message acknowledgement approach works
                # received_message.message.ack()
                
                process(received_message.message.data)


def process(message):
    parsed_message = json.loads(message)
    config_name, dataset, selection, user_id, url, target_path = parsed_message.values()
    selection = str(selection).replace(" ", "")
    config.load_config()

    with open(path.join(path.dirname(__file__), "downloader.yaml")) as f:
        dep = yaml.safe_load(f)
        uid = uuid.uuid4()
        dep['metadata']['name'] = f'downloader-job-id-{uid}'
        # d = target_path.rsplit('/')[-1]
        # dep['metadata']['name'] = f'a{d}a'
        dep['spec']['template']['spec']['containers'][0]['command'] = ['python', 'downloader.py', config_name, dataset, selection, user_id, url, target_path]
        batch_api = client.BatchV1Api()
        batch_api.create_namespaced_job(body=dep, namespace='default')

# [END container_pubsub_pull]
# [END gke_pubsub_pull]

if __name__ == '__main__':
    main()