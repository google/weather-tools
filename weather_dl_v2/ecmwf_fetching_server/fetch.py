import sys
import logging
import asyncio
import aiohttp
import json
import os
import typing as t

from config import Config, optimize_selection_partition
from parsers import prepare_target_name
from job_creator import create_download_job

API_URL = "https://api.ecmwf.int/v1/services/mars/requests"

sem = asyncio.Semaphore(2)
  
logger = logging.getLogger(__name__)


def build_header(config: Config) -> t.Dict[str, str]:
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


def create_job(config, result):
  target = prepare_target_name(config)
  res = {
          'selection': config.selection,
          'user_id': config.user_id,
          'url': result['href'],
          'target_path': target
        }
  data_str = json.dumps(res)
  create_download_job(data_str)

      
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

        create_job(partition, res)


async def reader(partitions: list):
  # Limit enforced by MARS that per license only 2 submits can be active
  tasks = [asyncio.ensure_future(submit(partition=p)) for p in partitions]
  await asyncio.gather(*tasks)
    
