from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from db_service.database import FirestoreClient
from license_dep.deployment_creator import create_license_deployment, terminate_license_deployment

db_client = FirestoreClient()


# TODO: Make use of google secret manager.
# REF: https://cloud.google.com/secret-manager.
class License(BaseModel):
    client_name: str
    number_of_requests: int
    api_key: str
    api_url: str
    api_email: str


class LicenseInternal(License):
    k8s_deployment_id: str


# Can perform CRUD on license table -- helps in handling API KEY expiry.
router = APIRouter(
    prefix="/license",
    tags=["license"],
    responses={404: {"description": "Not found"}},
)


# List all the license + handle filters of {client_name}
@router.get("/")
async def get_licenses(client_name: str | None = None):
    if client_name:
        result = db_client._get_license_by_client_name(client_name)
    else:
        result = db_client._get_licenses()
    return result


# Get particular license
@router.get("/{license_id}")
async def get_license_by_license_id(license_id: str):
    result = db_client._get_license_by_license_id(license_id)
    if not result:
        raise HTTPException(status_code=404, detail="License not found.")
    return result


# Update existing license
@router.put("/{license_id}")
async def update_license(license_id: str, license: License):
    if not db_client._check_license_exists(license_id):
        raise HTTPException(status_code=404, detail="No such license to update.")

    license_dict = license.dict()
    db_client._update_license(license_id, license_dict)

    terminate_license_deployment(license_id)
    create_deployment(license_id)
    return {"license_id": license_id, "name": "License updated successfully."}


# Add/Update k8s deployment ID for existing license (intenally).
def update_license_internal(license_id: str, k8s_deployment_id: str):
    if not db_client._check_license_exists(license_id):
        raise HTTPException(status_code=404, detail="No such license to update.")
    license_dict = {"k8s_deployment_id": k8s_deployment_id}

    db_client._update_license(license_id, license_dict)
    return {"license_id": license_id, "message": "License updated successfully."}


def create_deployment(license_id: str):
    k8s_deployment_id = create_license_deployment(license_id)
    update_license_internal(license_id, k8s_deployment_id)


# Add new license
@router.post("/")
async def add_license(license: License, background_tasks: BackgroundTasks = BackgroundTasks()):
    license_dict = license.dict()
    license_dict['k8s_deployment_id'] = ""
    license_id = db_client._add_license(license_dict)
    db_client._create_license_queue(license_id, license_dict['client_name'])
    background_tasks.add_task(create_deployment, license_id)
    return {"license_id": license_id, "message": "License added successfully."}


# Remove license
@router.delete("/{license_id}")
async def delete_license(license_id: str, background_tasks: BackgroundTasks = BackgroundTasks()):
    if not db_client._check_license_exists(license_id):
        raise HTTPException(status_code=404, detail="No such license to delete.")
    db_client._delete_license(license_id)
    db_client._remove_license_queue(license_id)
    background_tasks.add_task(terminate_license_deployment, license_id)
    return {"license_id": license_id, "message": "License removed successfully."}
