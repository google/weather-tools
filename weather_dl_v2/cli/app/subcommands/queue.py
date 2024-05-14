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


import typer
from typing_extensions import Annotated

from app.services.queue_service import queue_service
from app.utils import Validator, as_table, confirm_action

app = typer.Typer()


class QueueValidator(Validator):
    pass


queue_key_order = ['license_id', 'client_name', 'queue']


@app.command("list", help="List all the license queues.")
def get_all_license_queue(
    filter: Annotated[
        str, typer.Option(help="Filter by some value. Format: filter_key=filter_value")
    ] = None
):
    if filter:
        validator = QueueValidator(valid_keys=["client_name"])

        try:
            data = validator.validate(filters=[filter])
            client_name = data["client_name"]
        except Exception as e:
            print(f"filter error: {e}")
            return

        print(as_table(queue_service._get_license_queue_by_client_name(client_name), queue_key_order))
        return

    print(as_table(queue_service._get_all_license_queues(), queue_key_order))


@app.command("get", help="Get queue of particular license.")
def get_license_queue(license: Annotated[str, typer.Argument(help="License ID")]):
    print(as_table(queue_service._get_queue_by_license(license), queue_key_order))


@app.command(
    "edit",
    help="Edit existing license queue. Queue can edited via a priority"
    "file or my moving a single config to a given priority.",
)
def modify_license_queue(
    license: Annotated[str, typer.Argument(help="License ID.")],
    empty: Annotated[
        bool,
        typer.Option(
            "--empty",
            help="""Empties the license queue. If this is passed, other options are ignored."""
        )
    ] = False,
    save_dir_path: Annotated[
        str,
        typer.Option(
            "--save_and_empty",
            help="""Saves the license queue to a file and empties the queue."""
            """ Pass in path of directory. File will be saved as <license_id>.json ."""
        )
    ] = None,
    file: Annotated[
        str,
        typer.Option(
            "--file",
            "-f",
            help="""File path of priority json file. Example json: {"priority": ["c1.cfg", "c2.cfg",...]}""",
        ),
    ] = None,
    config: Annotated[
        str, typer.Option("--config", "-c", help="Config name for absolute priority.")
    ] = None,
    priority: Annotated[
        int,
        typer.Option(
            "--priority",
            "-p",
            help="Absolute priority for the config in a license queue."
            " Priority decreases in ascending order with 0 having highest priority.",
        ),
    ] = None,
    auto_confirm: Annotated[bool, typer.Option("-y", help="Automically confirm any promt.")] = False
):

    if empty and save_dir_path:
        print("Both --empty and --save_and_empty can't be passed. Use only one.")
        return

    if empty:
        if not auto_confirm:
            confirm_action(f"Are you sure you want to empty queue for {license}?")
        print("Emptying license queue...")
        print(queue_service._edit_license_queue(license, []))
        return

    if save_dir_path:
        if not auto_confirm:
            confirm_action(f"Are you sure you want to empty queue for {license}?")
        print("Saving and Emptying license queue...")
        file_path = queue_service._save_queue_to_file(license, save_dir_path)
        print(f"Queue saved at {file_path}.")
        print(queue_service._edit_license_queue(license, []))
        return

    if file is None and (config is None and priority is None):
        print("Priority file or config name with absolute priority must be passed.")
        return

    if file is not None and (config is not None or priority is not None):
        print("--config & --priority can't be used along with --file argument.")
        return

    if file is not None:
        validator = QueueValidator(valid_keys=["priority"])

        try:
            data = validator.validate_json(file_path=file)
            priority_list = data["priority"]
        except Exception as e:
            print(f"key error: {e}")
            return
        if not auto_confirm:
            confirm_action(f"Are you sure you want to edit {license} queue priority?")
        print(queue_service._edit_license_queue(license, priority_list))
        return
    elif config is not None and priority is not None:
        if priority < 0:
            print("Priority can not be negative.")
            return
        if not auto_confirm:
            confirm_action(f"Are you sure you want to edit {license} queue priority?")
        print(queue_service._edit_config_absolute_priority(license, config, priority))
        return
    else:
        print("--config & --priority arguments should be used together.")
        return
