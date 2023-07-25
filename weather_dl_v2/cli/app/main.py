import typer
import logging
from app.config import Config
import requests
from app.subcommands import download, queue, license

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="weather-dl-v2 is a cli tool for communicating with FastAPI server."
)

app.add_typer(download.app, name="download", help="Manage downloads.")
app.add_typer(queue.app, name="queue", help="Manage queues.")
app.add_typer(license.app, name="license", help="Manage licenses.")


@app.command("ping", help="Check if FastAPI server is live and rechable.")
def ping():
    uri = f"{Config().BASE_URI}/"

    try:
        x = requests.get(uri)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    logger.info(x.text)


if __name__ == "__main__":
    app()
