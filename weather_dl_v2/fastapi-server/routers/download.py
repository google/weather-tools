from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile
from firestore_db.db import fake_download_db, fake_license_priority_db
from config_processing.pipeline import start_processing_config
import shutil

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
        if file.filename in fake_download_db:
                raise HTTPException(status_code=409,
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
        res = { "config_name": "config_3", "client_name": client_name,"total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 }
    else:
        res = [
                { "config_name": "config_1", "client_name": "MARS","total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 },
                { "config_name": "config_2", "client_name": "MARS","total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 },
                { "config_name": "config_3", "client_name": "CDS","total_shards": 10000, "scheduled_shards": 4990, 
                "downloaded_shards": 5000, "failed_shards": 0 }
        ]
    return res


# Get status of particular download
@router.get("/{config_name}")
async def get_download(config_name: str):
    if config_name not in fake_download_db:
        raise HTTPException(status_code=404, detail="Download config not found in weather-dl v2.")

    # Get this kind of response by querying fake_manifest_db.
    res = { "config_name": config_name, "client_name": "MARS", "total_shards": 10000, "scheduled_shards": 4990, 
            "downloaded_shards": 5000, "failed_shards": 0 }              
    return res


# Stop & remove the execution of the config.
@router.delete("/{config_name}")
async def delete_download(config_name: str):
    if config_name not in fake_download_db:
        raise HTTPException(status_code=404, detail="No such download config to stop & remove.")

    del fake_download_db[config_name]
    for k, v in fake_license_priority_db.items():
        fake_license_priority_db[k] = [value for value in v if value != config_name]
    
    return {"config_name": config_name, "message": "Download config stopped & removed successfully."}
