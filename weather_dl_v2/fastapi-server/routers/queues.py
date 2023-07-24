from fastapi import APIRouter, HTTPException, Depends
from database.queue_handler import QueueHandler, get_queue_handler
from database.license_handler import LicenseHandler, get_license_handler

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
        result = queue_handler._get_queue_by_client_name(client_name)
    else:
        result = queue_handler._get_queues()
    return result


# Get particular license priority
@router.get("/{license_id}")
async def get_license_queue(
    license_id: str, queue_handler: QueueHandler = Depends(get_queue_handler)
):
    result = queue_handler._get_queue_by_license_id(license_id)
    if not result:
        raise HTTPException(status_code=404, detail="License's priority not found.")
    return result


# Change priority queue of particular license
@router.post("/{license_id}")
def modify_license_queue(
    license_id: str,
    priority_list: list | None = [],
    queue_handler: QueueHandler = Depends(get_queue_handler),
    license_handler: LicenseHandler = Depends(get_license_handler),
):
    if not license_handler._check_license_exists(license_id):
        raise HTTPException(status_code=404, detail="License's priority not found.")
    try:
        queue_handler._update_license_queue(license_id, priority_list)
        return {"message": f"'{license_id}' license priority updated successfully."}
    except Exception:
        return {"message": f"Failed to update '{license_id}' license priority."}


# Change config's priority in particular license
@router.put("/priority/{license_id}")
def modify_config_priority_in_license(
    license_id: str,
    config_name: str,
    priority: int,
    queue_handler: QueueHandler = Depends(get_queue_handler),
    license_handler: LicenseHandler = Depends(get_license_handler),
):
    if not license_handler._check_license_exists(license_id):
        raise HTTPException(status_code=404, detail="License's priority not found.")
    try:
        queue_handler._update_config_priority_in_license(
            license_id, config_name, priority
        )
        return {
            "message": f"'{license_id}' license '{config_name}' priority updated successfully."
        }
    except Exception as e:
        return {"message": f"Failed to update '{license_id}' license priority."}
