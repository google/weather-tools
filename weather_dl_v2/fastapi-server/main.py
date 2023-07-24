import logging
import os
import logging.config
from contextlib import asynccontextmanager
from fastapi import FastAPI
from routers import license, download, queues
from database.license_handler import get_license_handler
from license_dep.deployment_creator import create_license_deployment
from routers.license import get_create_deployment

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# set up logger.
logging.config.fileConfig("logging.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def create_pending_license_deployments():
    """Creates license deployments for Licenses whose deployments does not exist."""
    license_handler = get_license_handler()
    create_deployment = get_create_deployment()
    license_list = license_handler._get_license_without_deployment()

    for license in license_list:
        try:
            logger.info(f"Creating license deployment for {license}")
            create_deployment(license)
        except Exception as e:
            logger.error(f"License deployment failed for {license}. Exception: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Started FastAPI server")
    # Boot up
    # TODO: Replace hard-coded collection name by read a server config.

    # Retrieve license information & create license deployment if needed.
    create_pending_license_deployments()

    yield
    # Clean up


app = FastAPI(lifespan=lifespan)

app.include_router(license.router)
app.include_router(download.router)
app.include_router(queues.router)


@app.get("/")
async def main():
    return {"msg": "Greetings from weather-dl v2 !!"}
