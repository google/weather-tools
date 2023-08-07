import logging

from fastapi import APIRouter, HTTPException, Depends
from database.queue_handler import QueueHandler, get_queue_handler
from database.license_handler import LicenseHandler, get_license_handler
from database.download_handler import DownloadHandler, get_download_handler

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/queues",
    tags=["queues"],
    responses={404: {"description": "Not found"}},
)


# Users can change the execution order of config per license basis.
# List the licenses priority + {client_name} filter
@router.get("/")
async def get_all_license_queue(
    client_name: str | None = None,
    queue_handler: QueueHandler = Depends(get_queue_handler),
):
    if client_name:
        result = await queue_handler._get_queue_by_client_name(client_name)
    else:
        result = await queue_handler._get_queues()
    return result


# Get particular license priority
@router.get("/{license_id}")
async def get_license_queue(
    license_id: str, queue_handler: QueueHandler = Depends(get_queue_handler)
):
    result = await queue_handler._get_queue_by_license_id(license_id)
    if not result:
        logger.error(f"License priority for {license_id} not found.")
        raise HTTPException(status_code=404, detail=f"License priority for {license_id} not found.")
    return result


# Change priority queue of particular license
@router.post("/{license_id}")
async def modify_license_queue(
    license_id: str,
    priority_list: list | None = [],
    queue_handler: QueueHandler = Depends(get_queue_handler),
    license_handler: LicenseHandler = Depends(get_license_handler),
    download_handler: DownloadHandler = Depends(get_download_handler)
):
    if not await license_handler._check_license_exists(license_id):
        logger.error(f"License {license_id} not found.")
        raise HTTPException(status_code=404, detail=f"License {license_id} not found.")
    
    for config_name in priority_list:
        config = await download_handler._get_download_by_config_name(config_name)
        if config is None:
            logger.error(f"Download config {config_name} not found in weather-dl v2.")
            raise HTTPException(
                status_code=404, detail=f"Download config {config_name} not found in weather-dl v2."
            )
    try:
        await queue_handler._update_license_queue(license_id, priority_list)
        return {"message": f"'{license_id}' license priority updated successfully."}
    except Exception as e:
        logger.error(f"Failed to update '{license_id}' license priority due to {e}.")
        raise HTTPException(status_code=404, detail=f"Failed to update '{license_id}' license priority.")


# Change config's priority in particular license
@router.put("/priority/{license_id}")
async def modify_config_priority_in_license(
    license_id: str,
    config_name: str,
    priority: int,
    queue_handler: QueueHandler = Depends(get_queue_handler),
    license_handler: LicenseHandler = Depends(get_license_handler),
    download_handler: DownloadHandler = Depends(get_download_handler)
):
    if not await license_handler._check_license_exists(license_id):
        logger.error(f"License {license_id} not found.")
        raise HTTPException(status_code=404, detail=f"License {license_id} not found.")
    
    config = await download_handler._get_download_by_config_name(config_name)
    if config is None:
        logger.error(f"Download config {config_name} not found in weather-dl v2.")
        raise HTTPException(
            status_code=404, detail=f"Download config {config_name} not found in weather-dl v2."
        )

    try:
        await queue_handler._update_config_priority_in_license(
            license_id, config_name, priority
        )
        return {
            "message": f"'{license_id}' license -- '{config_name}' priority updated successfully."
        }
    except Exception as e:
        logger.error(f"Failed to update '{license_id}' license priority due to {e}.")
        raise HTTPException(status_code=404, detail=f"Failed to update '{license_id}' license priority.")
