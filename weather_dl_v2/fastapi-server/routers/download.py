from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile
from db_service.database import FirestoreClient
from config_processing.pipeline import start_processing_config
import shutil

db_client = FirestoreClient()

router = APIRouter(
    prefix="/download",
    tags=["download"],
    responses={404: {"description": "Not found"}},
)


def upload(file: UploadFile):
    dest = f"./config_files/{file.filename}"
    with open(dest, "wb+") as dest_:
        shutil.copyfileobj(file.file, dest_)
    return dest


# Can submit a config to the server.
@router.post("/")
def submit_download(file: UploadFile | None = None, licenses: list = [], background_tasks: BackgroundTasks = BackgroundTasks()):
    if not file:
        return {"message": "No upload file sent."}
    else:
        if db_client._check_download_exists(file.filename):
                raise HTTPException(status_code=400,
                                    detail=f"Please stop the ongoing download of the config file '{file.filename}' "
                                            "before attempting to start a new download.")
        try:
            dest = upload(file)
            # Start processing config.
            background_tasks.add_task(start_processing_config, dest, licenses)
            return {"message": f"file '{file.filename}' saved at '{dest}' successfully."}
        except:
            return {"message": f"Failed to save file '{file.filename}'."}

    
# Can check the current status of the submitted config.
# List status for all the downloads + handle filters
@router.get("/")
async def get_downloads(client_name: str | None = None):
    # Get this kind of response by querying fake_download_db + fake_manifest_db.
    if client_name:
        result = { "config_name": "config_3", "client_name": client_name,"total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 }
    else:
        result = [
                { "config_name": "config_1", "client_name": "MARS","total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 },
                { "config_name": "config_2", "client_name": "MARS","total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 },
                { "config_name": "config_3", "client_name": "CDS","total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 }
        ]
    return result


# Get status of particular download
@router.get("/{config_name}")
async def get_download(config_name: str):
    if not db_client._check_download_exists(config_name):
        raise HTTPException(status_code=404, detail="Download config not found in weather-dl v2.")

    # Get this kind of response by querying fake_manifest_db.
    result = { "config_name": config_name, "client_name": "MARS", "total_shards": 10000, "scheduled_shards": 4990, 
            "downloaded_shards": 5000, "failed_shards": 0 }              
    return result


# Stop & remove the execution of the config.
@router.delete("/{config_name}")
async def delete_download(config_name: str):
    if not db_client._check_download_exists(config_name):
        raise HTTPException(status_code=404, detail="No such download config to stop & remove.")

    db_client._stop_download(config_name)
    db_client._update_queues_on_stop_download(config_name)
    return {"config_name": config_name, "message": "Download config stopped & removed successfully."}
