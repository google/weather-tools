import typer
import logging
from app.cli_config import get_config
import requests
from app.subcommands import download, queue, license
from app.utils import Loader

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="weather-dl-v2 is a cli tool for communicating with FastAPI server."
)

app.add_typer(download.app, name="download", help="Manage downloads.")
app.add_typer(queue.app, name="queue", help="Manage queues.")
app.add_typer(license.app, name="license", help="Manage licenses.")


@app.command("ping", help="Check if FastAPI server is live and rechable.")
def ping():
    uri = f"{get_config().BASE_URI}/"
    print("uri ", uri)
    try:
        with Loader("Sending request..."):
            x = requests.get(uri)
    except Exception as e:
        print(f"error {e}")
        return
    print(x.text)


if __name__ == "__main__":
    app()
