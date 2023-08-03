from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile, Depends
from config_processing.pipeline import start_processing_config
from database.download_handler import DownloadHandler, get_download_handler
from database.queue_handler import QueueHandler, get_queue_handler
from database.manifest_handler import ManifestHandler, get_manifest_handler
import shutil
import os
import asyncio

router = APIRouter(
    prefix="/download",
    tags=["download"],
    responses={404: {"description": "Not found"}},
)


async def fetch_config_stats(
    config_name: str, client_name: str, manifest_handler: ManifestHandler
):
    """Get all the config stats parallely."""

    success_coroutine = manifest_handler._get_download_success_count(config_name)
    scheduled_coroutine = manifest_handler._get_download_scheduled_count(config_name)
    failure_coroutine = manifest_handler._get_download_failure_count(config_name)
    inprogress_coroutine = manifest_handler._get_download_inprogress_count(config_name)
    total_coroutine = manifest_handler._get_download_total_count(config_name)

    (
        success_count,
        scheduled_count,
        failure_count,
        inprogress_count,
        total_count,
    ) = await asyncio.gather(
        success_coroutine,
        scheduled_coroutine,
        failure_coroutine,
        inprogress_coroutine,
        total_coroutine,
    )

    return {
        "config_name": config_name,
        "client_name": client_name,
        "downloaded_shards": success_count,
        "scheduled_shards": scheduled_count,
        "failed_shards": failure_count,
        "in-progress_shards": inprogress_count,
        "total_shards": total_count,
    }


def get_fetch_config_stats():
    return fetch_config_stats


def get_fetch_config_stats_mock():
    def fetch_config_stats(
        config_name: str, client_name: str, manifest_handler: ManifestHandler
    ):
        return {
            "config_name": config_name,
            "client_name": client_name,
            "downloaded_shards": 0,
            "scheduled_shards": 0,
            "failed_shards": 0,
            "in-progress_shards": 0,
            "total_shards": 0,
        }

    return fetch_config_stats


def get_upload():
    def upload(file: UploadFile):
        dest = os.path.join(os.getcwd(), "config_files", file.filename)
        with open(dest, "wb+") as dest_:
            shutil.copyfileobj(file.file, dest_)
        return dest

    return upload


def get_upload_mock():
    def upload(file: UploadFile):
        return f"{os.getcwd()}/tests/test_data/{file.filename}"

    return upload


# Can submit a config to the server.
@router.post("/")
async def submit_download(
    file: UploadFile | None = None,
    licenses: list = [],
    background_tasks: BackgroundTasks = BackgroundTasks(),
    download_handler: DownloadHandler = Depends(get_download_handler),
    upload=Depends(get_upload),
):
    if not file:
        raise HTTPException(status_code=404, detail="No upload file sent.")
    else:
        if await download_handler._check_download_exists(file.filename):
            raise HTTPException(
                status_code=400,
                detail=f"Please stop the ongoing download of the config file '{file.filename}' "
                "before attempting to start a new download.",
            )
        try:
            dest = upload(file)
            # Start processing config.
            background_tasks.add_task(start_processing_config, dest, licenses)
            return {
                "message": f"file '{file.filename}' saved at '{dest}' successfully."
            }
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to save file '{file.filename} due to {e}'."
            )


# Can check the current status of the submitted config.
# List status for all the downloads + handle filters
@router.get("/")
async def get_downloads(
    client_name: str | None = None,
    download_handler: DownloadHandler = Depends(get_download_handler),
    manifest_handler: ManifestHandler = Depends(get_manifest_handler),
    fetch_config_stats=Depends(get_fetch_config_stats),
):
    downloads = await download_handler._get_downloads(client_name)
    coroutines = []

    for download in downloads:
        coroutines.append(
            fetch_config_stats(
                download["config_name"], download["client_name"], manifest_handler
            )
        )

    return await asyncio.gather(*coroutines)


# Get status of particular download
@router.get("/{config_name}")
async def get_download_by_config_name(
    config_name: str,
    download_handler: DownloadHandler = Depends(get_download_handler),
    manifest_handler: ManifestHandler = Depends(get_manifest_handler),
    fetch_config_stats=Depends(get_fetch_config_stats),
):
    config = await download_handler._get_download_by_config_name(config_name)

    if config is None:
        raise HTTPException(
            status_code=404, detail=f"Download config {config_name} not found in weather-dl v2."
        )

    return await fetch_config_stats(
        config["config_name"], config["client_name"], manifest_handler
    )


# Stop & remove the execution of the config.
@router.delete("/{config_name}")
async def delete_download(
    config_name: str,
    download_handler: DownloadHandler = Depends(get_download_handler),
    queue_handler: QueueHandler = Depends(get_queue_handler),
):
    if not await download_handler._check_download_exists(config_name):
        raise HTTPException(
            status_code=404, detail="No such download config to stop & remove."
        )

    await download_handler._stop_download(config_name)
    await queue_handler._update_queues_on_stop_download(config_name)
    return {
        "config_name": config_name,
        "message": "Download config stopped & removed successfully.",
    }
