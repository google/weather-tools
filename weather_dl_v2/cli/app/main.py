# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging

import requests
import typer

from app.cli_config import get_config
from app.subcommands import config, download, license, queue
from app.utils import Loader

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="weather-dl-v2 is a cli tool for communicating with FastAPI server."
)

app.add_typer(download.app, name="download", help="Manage downloads.")
app.add_typer(queue.app, name="queue", help="Manage queues.")
app.add_typer(license.app, name="license", help="Manage licenses.")
app.add_typer(config.app, name="config", help="Configurations for cli.")


@app.command("ping", help="Check if FastAPI server is live and rechable.")
def ping():
    uri = f"{get_config().BASE_URI}/"
    try:
        with Loader("Sending request..."):
            x = requests.get(uri)
    except Exception as e:
        print(f"error {e}")
        return
    print(x.text)


if __name__ == "__main__":
    app()
