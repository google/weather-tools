# Copyright 2021 Google LLC
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


import typer
import json
import os
from typing_extensions import Annotated
from app.cli_config import get_config
from app.utils import Validator

app = typer.Typer()


class ConfigValidator(Validator):
    pass


@app.command("show-ip", help="See the current server IP address.")
def show_server_ip():
    print(f"Current pod IP: {get_config().pod_ip}")


@app.command("set-ip", help="Update the server IP address.")
def update_server_ip(
    new_ip: Annotated[
        str, typer.Argument(help="New IP address. (Do not add port or protocol).")
    ],
):
    file_path = os.path.join(os.getcwd(), "cli_config.json")
    cli_config = {}
    with open(file_path, "r") as file:
        cli_config = json.load(file)

    old_ip = cli_config["pod_ip"]
    cli_config["pod_ip"] = new_ip

    with open(file_path, "w") as file:
        json.dump(cli_config, file)

    validator = ConfigValidator(valid_keys=["pod_ip", "port"])

    try:
        cli_config = validator.validate_json(file_path=file_path)
    except Exception as e:
        print(f"payload error: {e}")
        return

    print(f"Pod IP Updated {old_ip} -> {new_ip} .")
