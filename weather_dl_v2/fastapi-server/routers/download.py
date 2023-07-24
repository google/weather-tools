from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile, Depends
from config_processing.pipeline import start_processing_config
from database.download_handler import DownloadHandler, get_download_handler
from database.queue_handler import QueueHandler, get_queue_handler
import shutil
import os

router = APIRouter(
    prefix="/download",
    tags=["download"],
    responses={404: {"description": "Not found"}},
)


def get_upload():
    def upload(file: UploadFile):
        dest = f"./config_files/{file.filename}"
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
def submit_download(file: UploadFile | None = None, licenses: list = [],
                    background_tasks: BackgroundTasks = BackgroundTasks(),
                    download_handler: DownloadHandler = Depends(get_download_handler),
                    upload = Depends(get_upload)
                    ):
    if not file:
        raise HTTPException(status_code=404, detail="No upload file sent.")
    else:
        if download_handler._check_download_exists(file.filename):
            raise HTTPException(status_code=400,
                                detail=f"Please stop the ongoing download of the config file '{file.filename}' "
                                "before attempting to start a new download.")
        try:
            dest = upload(file)
            # Start processing config.
            background_tasks.add_task(start_processing_config, dest, licenses)
            return {"message": f"file '{file.filename}' saved at '{dest}' successfully."}
        except Exception:
            raise HTTPException(status_code=500, detail=f"Failed to save file '{file.filename}'.")


# Can check the current status of the submitted config.
# List status for all the downloads + handle filters
@router.get("/")
async def get_downloads(client_name: str | None = None):
    # Get this kind of response by querying download collection + manifest collection.
    if client_name:
        result = {"config_name": "config_3", "client_name": client_name, "total_shards": 10000,
                  "scheduled_shards": 4990, "downloaded_shards": 5000, "failed_shards": 0}
    else:
        result = [
                {"config_name": "config_1", "client_name": "MARS", "total_shards": 10000, "scheduled_shards": 4990,
                 "downloaded_shards": 5000, "failed_shards": 0},
                {"config_name": "config_2", "client_name": "MARS", "total_shards": 10000, "scheduled_shards": 4990,
                 "downloaded_shards": 5000, "failed_shards": 0},
                {"config_name": "config_3", "client_name": "CDS", "total_shards": 10000, "scheduled_shards": 4990,
                 "downloaded_shards": 5000, "failed_shards": 0}
        ]
    return result


# Get status of particular download
@router.get("/{config_name}")
async def get_download(config_name: str, download_handler: DownloadHandler = Depends(get_download_handler)):
    if not download_handler._check_download_exists(config_name):
        raise HTTPException(status_code=404, detail="Download config not found in weather-dl v2.")

    # Get this kind of response by querying fake_manifest_db.
    result = {"config_name": config_name, "client_name": "MARS", "total_shards": 10000, "scheduled_shards": 4990,
              "downloaded_shards": 5000, "failed_shards": 0}
    return result


# Stop & remove the execution of the config.
@router.delete("/{config_name}")
async def delete_download(config_name: str,
                          download_handler: DownloadHandler = Depends(get_download_handler),
                          queue_handler: QueueHandler = Depends(get_queue_handler)):
    if not download_handler._check_download_exists(config_name):
        raise HTTPException(status_code=404, detail="No such download config to stop & remove.")

    download_handler._stop_download(config_name)
    queue_handler._update_queues_on_stop_download(config_name)
    return {"config_name": config_name, "message": "Download config stopped & removed successfully."}
