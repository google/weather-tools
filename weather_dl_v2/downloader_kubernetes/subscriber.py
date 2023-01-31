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
import asyncio
import redis.asyncio as redis

USE_REDIS = True

async def reader(channel: redis.client.PubSub):
    print(f"Listening for messages ...\n")
    while True:
        message = await channel.get_message(ignore_subscribe_messages=True)
        if message is not None:
            print(f"(Reader) Message Received: {message}")
            process(message["data"])


async def redis_main():
    """Continuously pull messages from redis subsciption."""
    r = redis.from_url("redis://<redis-master-service-ip>")

    async with r.pubsub() as pubsub:
        await pubsub.subscribe("channel:1")

        future = asyncio.create_task(reader(pubsub))
        await future


def main():
    """Continuously pull messages from subsciption."""
    while True:
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = "XXXXXXXXXXXXXXX"
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
    # message = b'{"selection": {"class": "od", "type": "pf", "stream": "enfo", "expver": "0001", "levtype": "pl", "levelist": "100", "param": "129.128", "date": ["2019-07-26"], "time": "0000", "step": ["0", "1", "2"], "number": ["1", "2"], "grid": "F640"}, "user_id": "mahrsee", "url": "https://XXXXXXX", "target_path": "gs://XXXXXXX.gb"}'
    parsed_message = json.loads(message)
    selection, user_id, url, target_path = parsed_message.values()
    selection = str(selection).replace(" ", "")
    # Configs can be set in Configuration class directly or using helper
    # utility. If no argument provided, the config will be loaded from
    # default location.
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()

    with open(path.join(path.dirname(__file__), "downloader.yaml")) as f:
        dep = yaml.safe_load(f)
        uid = uuid.uuid4()
        dep['metadata']['name'] = f'downloader-job-id-{uid}'
        dep['spec']['template']['spec']['containers'][0]['command'] = ['python', 'downloader.py', selection, user_id, url, target_path]
        batch_api = client.BatchV1Api()
        batch_api.create_namespaced_job(body=dep, namespace='default')


if __name__ == '__main__':
    if USE_REDIS:
        asyncio.run(redis_main())
    else:
        main()