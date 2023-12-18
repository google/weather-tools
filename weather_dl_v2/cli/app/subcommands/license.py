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

from app.services.license_service import license_service
from app.utils import Validator, as_table

app = typer.Typer()


class LicenseValidator(Validator):
    pass


@app.command("list", help="List all licenses.")
def get_all_license(
    filter: Annotated[
        str, typer.Option(help="Filter by some value. Format: filter_key=filter_value")
    ] = None
):
    if filter:
        validator = LicenseValidator(valid_keys=["client_name"])

        try:
            data = validator.validate(filters=[filter])
            client_name = data["client_name"]
        except Exception as e:
            print(f"filter error: {e}")
            return

        print(as_table(license_service._get_all_license_by_client_name(client_name)))
        return

    print(as_table(license_service._get_all_license()))


@app.command("get", help="Get a particular license by ID.")
def get_license(license: Annotated[str, typer.Argument(help="License ID.")]):
    print(as_table(license_service._get_license_by_license_id(license)))


@app.command("add", help="Add new license.")
def add_license(
    file_path: Annotated[
        str,
        typer.Argument(
            help="""Input json file. Example json for new license-"""
            """{"license_id" : <str>, "client_name" : <str>, "number_of_requests" : <int>, "secret_id" : <str>}"""
            """\nNOTE: license_id is case insensitive and has to be unique for each license."""
        ),
    ],
):
    validator = LicenseValidator(
        valid_keys=["license_id", "client_name", "number_of_requests", "secret_id"]
    )

    try:
        license_dict = validator.validate_json(file_path=file_path)
    except Exception as e:
        print(f"payload error: {e}")
        return

    print(license_service._add_license(license_dict))


@app.command("remove", help="Remove a license.")
def remove_license(license: Annotated[str, typer.Argument(help="License ID.")]):
    print(license_service._remove_license(license))


@app.command("update", help="Update existing license.")
def update_license(
    license: Annotated[str, typer.Argument(help="License ID.")],
    file_path: Annotated[
        str,
        typer.Argument(
            help="""Input json file. Example json for updated license- """
            """{"license_id": <str>, "client_name" : <str>, "number_of_requests" : <int>, "secret_id" : <str>}"""
        ),
    ],  # noqa
):
    validator = LicenseValidator(
        valid_keys=["license_id", "client_name", "number_of_requests", "secret_id"]
    )
    try:
        license_dict = validator.validate_json(file_path=file_path)
    except Exception as e:
        print(f"payload error: {e}")
        return

    print(license_service._update_license(license, license_dict))


@app.command("redeploy",
             help="""Redeploy licenses."""
             """ CAUTION: Redeploying will cause licenses to stop whatever they are doing."""
             """ This can cause queues to be filled with stray requests from previous deployments."""
             )
def redeploy_license(
    license_id: Annotated[
        str,
        typer.Option(
            "--license_id",
            help="""Mention license_id of license to redeploy."""
            """ Send 'all' if want to redeploy all licenses."""
            )
        ] = None,
    client_name: Annotated[
        str,
        typer.Option(
            "--client_name",
            help="Redeploy all licenses of a particular client."
            )
        ] = None
):
    if license_id is not None and client_name is not None:
        print("Can't pass both license_id and client_name. Please pass only one.")
        return

    if license_id is None and client_name is None:
        print("Please pass --license_id or --client_name.")
        return

    if license_id is not None:
        print(license_service._redeploy_license_by_license_id(license_id))
        return

    if client_name is not None:
        print(license_service._redeploy_licenses_by_client(client_name))
        return
