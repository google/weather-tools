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


from typing import List

import typer
from typing_extensions import Annotated

from app.services.download_service import download_service
from app.utils import Validator, as_table, confirm_action

app = typer.Typer(rich_markup_mode="markdown")


class DowloadFilterValidator(Validator):
    pass


download_key_order = [
    'config_name', 'client_name', 'partitioning_status', 'scheduled_shards', 'in-progress_shards',
    'downloaded_shards', 'failed_shards', 'total_shards'
]


@app.command("list", help="List out all the configs.")
def get_downloads(
    filter: Annotated[
        List[str],
        typer.Option(
            help="""Filter by some value. Format: filter_key=filter_value. Available filters """
            """[key: client_name, values: cds, mars, ecpublic] """
            """[key: status, values: completed, failed, in-progress]"""
        ),
    ] = []
):
    if len(filter) > 0:
        validator = DowloadFilterValidator(valid_keys=["client_name", "status"])

        try:
            filter_dict = validator.validate(filters=filter, allow_missing=True)
        except Exception as e:
            print(f"filter error: {e}")
            return

        print(as_table(download_service._list_all_downloads_by_filter(filter_dict), download_key_order))
        return

    print(as_table(download_service._list_all_downloads(), download_key_order))


# TODO: Add support for submitting multiple configs using *.cfg notation.
@app.command("add", help="Submit new config to download.")
def submit_download(
    file_path: Annotated[
        str, typer.Argument(help="File path of config to be uploaded.")
    ],
    license: Annotated[List[str], typer.Option("--license", "-l", help="License ID.")],
    force_download: Annotated[
        bool,
        typer.Option(
            "-f",
            "--force-download",
            help="Force redownload of partitions that were previously downloaded.",
        ),
    ] = False,
    priority: Annotated[
        int,
        typer.Option(
            "-p",
            "--priority",
            help="Set the priority for submitted config in ALL licenses. If not added, the config is added" \
                "at the end of the queue. Priority decreases in ascending order with 0 having highest priority.",
        ),
    ] = None,
):
    print(download_service._add_new_download(file_path, license, force_download, priority))


@app.command("get", help="Get a particular config.")
def get_download_by_config(
    config_name: Annotated[str, typer.Argument(help="Config file name.")]
):
    print(as_table(download_service._get_download_by_config(config_name), download_key_order))


@app.command("show", help="Show contents of a particular config.")
def show_config(
    config_name: Annotated[str, typer.Argument(help="Config file name.")]
):
    print(download_service._show_config_content(config_name))


@app.command("remove", help="Remove existing config.")
def remove_download(
    config_name: Annotated[str, typer.Argument(help="Config file name.")],
    auto_confirm: Annotated[bool, typer.Option("-y", help="Automically confirm any promt.")] = False
):
    if not auto_confirm:
        confirm_action(f"Are you sure you want to remove {config_name}?")
    print(download_service._remove_download(config_name))


@app.command(
    "refetch", help="Reschedule all partitions of a config that are not successful."
)
def refetch_config(
    config_name: Annotated[str, typer.Argument(help="Config file name.")],
    license: Annotated[List[str], typer.Option("--license", "-l", help="License ID.")],
    only_failed: Annotated[bool, typer.Option("--only_failed", help="Only refetch failed partitions.")] = False,
    auto_confirm: Annotated[bool, typer.Option("-y", help="Automically confirm any promt.")] = False
):
    if not auto_confirm:
        confirm_action(f"Are you sure you want to refetch {config_name}?")
    print(download_service._refetch_config_partitions(config_name, license, only_failed))
