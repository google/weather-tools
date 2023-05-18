from fastapi import APIRouter, HTTPException
from db_service.database import FirestoreClient

db_client = FirestoreClient()

router = APIRouter(
    prefix="/queues",
    tags=["queues"],
    responses={404: {"description": "Not found"}},
)

# Users can change the execution order of config per license basis.

# List the licenses priority + {client_name} filter
@router.get("/")
async def get_all_license_queue(client_name: str | None = None):
    if client_name:
        result = db_client._get_queue_by_client_name(client_name)
    else:
        result = db_client._get_queues()
    return result


# Get particular license priority
@router.get("/{license_id}")
async def get_license_queue(license_id: str):
    result = db_client._get_queue_by_license_id(license_id)
    if not result:
        raise HTTPException(status_code=404, detail="License's priority not found.")
    return result
    

# Change config's priority on particular license
@router.post("/{license_id}")
def modify_license_queue(license_id: str, priority_list: list | None = []):
    if not db_client._check_license_exists(license_id):
        raise HTTPException(status_code=404, detail="License's priority not found.")
    try:
        db_client._update_license_queue(license_id, priority_list)
        return {"message": f"'{license_id}' license priority updated successfully."}
    except:
        return {"message": f"Failed to update '{license_id}' license priority."}