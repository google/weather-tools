import typer
from typing_extensions import Annotated
from app.services.license_service import license_service
from app.utils import Validator

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

        print(license_service._get_all_license_by_client_name(client_name))
        return

    print(license_service._get_all_license())


@app.command("get", help="Get a particular license by ID.")
def get_license(license: Annotated[str, typer.Argument(help="License ID.")]):
    print(license_service._get_license_by_license_id(license))


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
            """{"client_name" : <str>, "number_of_requests" : <int>, "secret_id" : <str>}"""
        ),
    ],  # noqa
):
    validator = LicenseValidator(
        valid_keys=["client_name", "number_of_requests", "secret_id"]
    )
    try:
        license_dict = validator.validate_json(file_path=file_path)
    except Exception as e:
        print(f"payload error: {e}")
        return

    print(license_service._update_license(license, license_dict))
