import logging

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel
from license_dep.deployment_creator import create_license_deployment, terminate_license_deployment
from database.license_handler import LicenseHandler, get_license_handler
from database.queue_handler import QueueHandler, get_queue_handler

logger = logging.getLogger(__name__)


# TODO: Make use of google secret manager.
# REF: https://cloud.google.com/secret-manager.
class License(BaseModel):
    license_id: str
    client_name: str
    number_of_requests: int
    secret_id: str


class LicenseInternal(License):
    k8s_deployment_id: str


# Can perform CRUD on license table -- helps in handling API KEY expiry.
router = APIRouter(
    prefix="/license",
    tags=["license"],
    responses={404: {"description": "Not found"}},
)


# Add/Update k8s deployment ID for existing license (intenally).
async def update_license_internal(
    license_id: str,
    k8s_deployment_id: str,
    license_handler: LicenseHandler,
):
    if not await license_handler._check_license_exists(license_id):
        logger.info(f"No such license {license_id} to update.")
        raise HTTPException(
            status_code=404, detail=f"No such license {license_id} to update."
        )
    license_dict = {"k8s_deployment_id": k8s_deployment_id}

    await license_handler._update_license(license_id, license_dict)
    return {"license_id": license_id, "message": "License updated successfully."}


def get_create_deployment():
    async def create_deployment(license_id: str, license_handler: LicenseHandler):
        k8s_deployment_id = create_license_deployment(license_id)
        await update_license_internal(license_id, k8s_deployment_id, license_handler)

    return create_deployment


def get_create_deployment_mock():
    def create_deployment_mock(license_id: str, license_handler: LicenseHandler):
        logger.info("create deployment mocked")

    return create_deployment_mock


def get_terminate_license_deployment():
    return terminate_license_deployment


def get_terminate_license_deployment_mock():
    def get_terminate_license_deployment_mock(license_id):
        logger.info(f"terminating license deployment for {license_id}")

    return get_terminate_license_deployment_mock


# List all the license + handle filters of {client_name}
@router.get("/")
async def get_licenses(
    client_name: str | None = None,
    license_handler: LicenseHandler = Depends(get_license_handler),
):
    if client_name:
        result = await license_handler._get_license_by_client_name(client_name)
    else:
        result = await license_handler._get_licenses()
    return result


# Get particular license
@router.get("/{license_id}")
async def get_license_by_license_id(
    license_id: str, license_handler: LicenseHandler = Depends(get_license_handler)
):
    result = await license_handler._get_license_by_license_id(license_id)
    if not result:
        logger.info(f"License {license_id} not found.")
        raise HTTPException(status_code=404, detail=f"License {license_id} not found.")
    return result


# Update existing license
@router.put("/{license_id}")
async def update_license(
    license_id: str,
    license: License,
    license_handler: LicenseHandler = Depends(get_license_handler),
    create_deployment=Depends(get_create_deployment),
    terminate_license_deployment=Depends(get_terminate_license_deployment),
):
    if not await license_handler._check_license_exists(license_id):
        logger.error(f"No such license {license_id} to update.")
        raise HTTPException(
            status_code=404, detail=f"No such license {license_id} to update."
        )

    license_dict = license.dict()
    await license_handler._update_license(license_id, license_dict)

    terminate_license_deployment(license_id)
    await create_deployment(license_id, license_handler)
    return {"license_id": license_id, "name": "License updated successfully."}


# Add new license
@router.post("/")
async def add_license(
    license: License,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    license_handler: LicenseHandler = Depends(get_license_handler),
    queue_handler: QueueHandler = Depends(get_queue_handler),
    create_deployment=Depends(get_create_deployment),
):
    license_id = license.license_id.lower()
    if await license_handler._check_license_exists(license_id):
        logger.error(f"License with license_id {license_id} already exist.")
        raise HTTPException(
            status_code=409, detail=f"License with license_id {license_id} already exist."
        )

    license_dict = license.dict()
    license_dict["k8s_deployment_id"] = ""
    license_id = await license_handler._add_license(license_dict)
    await queue_handler._create_license_queue(license_id, license_dict["client_name"])
    background_tasks.add_task(create_deployment, license_id, license_handler)
    return {"license_id": license_id, "message": "License added successfully."}


# Remove license
@router.delete("/{license_id}")
async def delete_license(
    license_id: str,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    license_handler: LicenseHandler = Depends(get_license_handler),
    queue_handler: QueueHandler = Depends(get_queue_handler),
    terminate_license_deployment=Depends(get_terminate_license_deployment),
):
    if not await license_handler._check_license_exists(license_id):
        logger.error(f"No such license {license_id} to delete.")
        raise HTTPException(
            status_code=404, detail=f"No such license {license_id} to delete."
        )
    await license_handler._delete_license(license_id)
    await queue_handler._remove_license_queue(license_id)
    background_tasks.add_task(terminate_license_deployment, license_id)
    return {"license_id": license_id, "message": "License removed successfully."}
