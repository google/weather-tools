import logging
import os
import logging.config
from contextlib import asynccontextmanager
from fastapi import FastAPI
from routers import license, download, queues

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# set up logger.
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Started FastAPI server")
    # Boot up
    # TODO: Replace hard-coded collection name by read a server config.
    logger.info("Create database if not already exists.")
    logger.info("Retrieve license information & create license deployment if needed.")
    yield
    # Clean up

app = FastAPI(lifespan=lifespan)

app.include_router(license.router)
app.include_router(download.router)
app.include_router(queues.router)


@app.get("/")
async def main():
    return {"msg": "Greetings from weather-dl v2 !!"}
