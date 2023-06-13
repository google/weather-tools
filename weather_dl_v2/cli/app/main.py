import typer
from app.config import Config
import requests
from app.subcommands import download, queue


app = typer.Typer(help="weather-dl-v2 is a cli tool for communicating with FastAPI server.")

app.add_typer(download.app, name="download", help="Manage downloads.")
app.add_typer(queue.app, name="queue", help="Manage queues.")



@app.command(help="Check if FastAPI server is live and rechable")
def ping():
    uri = f"{Config().BASE_URI}"

    try:
        x = requests.get(uri)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


    print(x.text)

if __name__ == "__main__":
    app()