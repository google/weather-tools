import typer
from typing_extensions import Annotated
from app.services.license_service import license_service
from app.utils import Validator
from typing import List


app = typer.Typer()


class LicenseValidator(Validator):
    pass

@app.command(help="List all licenses.")
def list(
        filter: Annotated[str, typer.Option(help="Filter by some value. Format: filter_key=filter_value")] = None
    ):
    if filter:
        validator = LicenseValidator(valid_keys=["client_name"])

        try:
            data = validator.validate(filters=[filter])
            client_name = data['client_name']
        except Exception as e:
            print(f"filter error: {e}")
            return
        
        print(license_service._get_all_license_by_client_name(client_name))
        return
    
    print(license_service._get_all_license())

@app.command("get", help="Get a particular license by ID.")
def get_license(
    license: Annotated[str, typer.Argument(help="License ID.")]
    ):
    print(license_service._get_license_by_license_id(license))

@app.command(help="Add new license.")
def add(
        file_path: Annotated[str, typer.Argument(help='''Input json file. Example json for new license- {"client_name" : <str>, "number_of_requests" : <int>, "api_key" : <str>, "api_url" : <str>}''')],
    ):
    validator = LicenseValidator(
        valid_keys=[
            "client_name",
            "number_of_requests",
            "api_key",
            "api_url",
            "api_email"
        ]
    )

    try:
        license_dict = validator.validate_json(file_path=file_path)
    except Exception as e:
        print(f"payload error: {e}")
        return
    
    print(license_service._add_license(license_dict))

@app.command(help="Remove a license")
def remove(
        license: Annotated[str, typer.Argument( help="License ID.")]
    ):
    print(license_service._remove_license(license))

@app.command(help="Update existing license.")
def update(
        license: Annotated[str, typer.Argument(help="License ID.")],
        file_path: Annotated[str, typer.Argument(help='''Input json file. Example json for new license- {"client_name" : <str>, "number_of_requests" : <int>, "api_key" : <str>, "api_url" : <str>}''')]
    ):
    validator = LicenseValidator(
        valid_keys=[
            "client_name",
            "number_of_requests",
            "api_key",
            "api_url",
            "api_email"
        ]
    )
    try:
        license_dict = validator.validate_json(file_path=file_path)
    except Exception as e:
        print(f"payload error: {e}")
        return

    print(license_service._update_license(license, license_dict))

