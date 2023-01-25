import sys
import logging
import asyncio
import aiohttp
import json
import os
import redis.asyncio as redis

from concurrent import futures
from config import Config, optimize_selection_partition
from parsers import prepare_target_name
from google.cloud import pubsub_v1

API_URL = "https://api.ecmwf.int/v1/services/mars/requests"
USE_REDIS = False

sem = asyncio.Semaphore(2)
  
logger = logging.getLogger(__name__)


def build_header(config: Config) -> dict[str, str]:
  """
  Reads the environment variable and create MARS specific header
  """

  api_key=config.kwargs.get('api_key', os.environ.get("MARSAPI_KEY", None)),
  api_email=config.kwargs.get('api_email', os.environ.get("MARSAPI_EMAIL", None)),

  if api_key and api_email:
    return {
        "Accept": "application/json",
        "From": api_email[0],
        "X-ECMWF-KEY": api_key[0],
    }
  else:
    print(
        "Couldn't find the API_EMAIL and API_KEY exiting the program")
    sys.exit()


async def publish_on_redis(config, result):
  r = redis.from_url("redis://localhost")
  target = prepare_target_name(config)
  res = {
        'selection': config.selection,
        'user_id': config.user_id,
        'url': result['href'],
        'target_path': target
      }
  data_str = json.dumps(res)
  await r.publish("channel:1", data_str)

  
def publish_on_pub_sub(config, result):
  target = prepare_target_name(config)

  publisher = pubsub_v1.PublisherClient()
  # The `topic_path` method creates a fully qualified identifier
  # in the form `projects/{project_id}/topics/{topic_id}`
  topic_path = "XXXXXXXXXX"

  res = {
          'selection': config.selection,
          'user_id': config.user_id,
          'url': result['href'],
          'target_path': target
        }
  data_str = json.dumps(res)
  print(data_str)
  # Data must be a bytestring
  data = data_str.encode("utf-8")
  # When you publish a message, the client returns a future.
  future = publisher.publish(topic_path, data)
  print(f"Published message: {res} to {topic_path}.")
  # Wait for the publish futures to resolve before exiting.
  futures.wait([future], return_when=futures.ALL_COMPLETED)

      
async def check_status(name: str, session: aiohttp.ClientSession, headers: dict,
                       event: asyncio.Event) -> None:
  """
  Monitors the status of the given download till it's state changes to 
  active -> complete
  """
  url = f"https://api.ecmwf.int/v1/services/mars/requests/{name}"
  while True:
    async with session.get(url, headers=headers,
                           allow_redirects=False) as response:
      response = await response.json()

      if response["code"] == 200 and response["status"] == "complete":
        event.set()
        if isinstance(response, dict) and "result" in response:
          response = response["result"]
        break
      if response["code"] in [303]:
        event.set()
        break
    await asyncio.sleep(60)
  
  return response


async def submit(partition: Config) -> None:
  """
  submits the download job requests and waits till it becomes ready to
  download
  """
  async with aiohttp.ClientSession() as session:
    async with sem:
      headers = build_header(partition)
      selection_ = optimize_selection_partition(partition.selection)
      print("submitting the request %s", selection_)
      async with session.post(API_URL, data=json.dumps(selection_),
                            headers=headers) as response:
        response = await response.json()

        # name = "639ae4e6922564a69134324c"
        name = response.get("name")
        event = asyncio.Event()
        loop = asyncio.get_event_loop()

        task = loop.create_task(check_status(name, session, headers, event))
        
        await event.wait()
      
        res = task.result()
        
        task.cancel()

        if USE_REDIS:
          await publish_on_redis(partition, res)
        else:
          publish_on_pub_sub(partition, res)


async def reader(partitions: list):
  # Limit enforced by MARS that per license only 2 submits can be active
  tasks = [asyncio.ensure_future(submit(partition=p)) for p in partitions]
  await asyncio.gather(*tasks)
    
