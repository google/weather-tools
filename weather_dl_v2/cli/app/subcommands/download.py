import typer
from typing_extensions import Annotated
from app.services.download_service import download_service
from app.utils import Validator, parse_output
from typing import List

app = typer.Typer(rich_markup_mode="markdown")


class DowloadFilterValidator(Validator):
    pass


@app.command("list", help="List out all the configs.")
def get_downloads(
    filter: Annotated[
        List[str],
        typer.Option(
            help="""Filter by some value. Format: filter_key=filter_value. Available filters """
            """[key: client_name, values: cds, mars, ecpublic] """
            """[key: status, values: completed, failed, in-progress]"""
        ),
    ] = [],
    table: Annotated[
        bool, typer.Option("--table", "-t", help="Show the data as a table.")
    ] = False,
):
    if len(filter) > 0:
        validator = DowloadFilterValidator(valid_keys=["client_name", "status"])

        try:
            filter_dict = validator.validate(filters=filter, allow_missing=True)
        except Exception as e:
            print(f"filter error: {e}")
            return

        print(
            parse_output(
                download_service._list_all_downloads_by_filter(filter_dict), table=table
            )
        )
        return

    print(parse_output(download_service._list_all_downloads(), table=table))


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
):
    print(download_service._add_new_download(file_path, license, force_download))


@app.command("get", help="Get a particular config.")
def get_download_by_config(
    config_name: Annotated[str, typer.Argument(help="Config file name.")],
    table: Annotated[
        bool, typer.Option("--table", "-t", help="Show the data as a table.")
    ] = False,
):
    print(
        parse_output(download_service._get_download_by_config(config_name), table=table)
    )


@app.command("remove", help="Remove existing config.")
def remove_download(
    config_name: Annotated[str, typer.Argument(help="Config file name.")]
):
    print(download_service._remove_download(config_name))


@app.command(
    "refetch", help="Reschedule all partitions of a config that are not successful."
)
def refetch_config(
    config_name: Annotated[str, typer.Argument(help="Config file name.")],
    license: Annotated[List[str], typer.Option("--license", "-l", help="License ID.")],
):
    print(download_service._refetch_config_partitions(config_name, license))
