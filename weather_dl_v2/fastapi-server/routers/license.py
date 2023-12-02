# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import re
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel
from license_dep.deployment_creator import create_license_deployment, terminate_license_deployment
from database.license_handler import LicenseHandler, get_license_handler
from database.queue_handler import QueueHandler, get_queue_handler

logger = logging.getLogger(__name__)


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
    async def create_deployment_mock(license_id: str, license_handler: LicenseHandler):
        logger.info("create deployment mock.")

    return create_deployment_mock


def get_terminate_license_deployment():
    return terminate_license_deployment


def get_terminate_license_deployment_mock():
    def get_terminate_license_deployment_mock(license_id):
        logger.info(f"terminating license deployment for {license_id}.")

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
    queue_handler: QueueHandler = Depends(get_queue_handler),
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
    await queue_handler._update_client_name_in_license_queue(
        license_id, license_dict["client_name"]
    )

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

    # Check if license id is in correct format.
    LICENSE_REGEX = re.compile(
        r"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*"
    )
    if not bool(LICENSE_REGEX.fullmatch(license_id)):
        logger.error(
            """Invalid format for license_id. License id must consist of lower case alphanumeric"""
            """ characters, '-' or '.', and must start and end with an alphanumeric character"""
        )
        raise HTTPException(
            status_code=400,
            detail="""Invalid format for license_id. License id must consist of lower case alphanumeric"""
            """ characters, '-' or '.', and must start and end with an alphanumeric character""",
        )

    if await license_handler._check_license_exists(license_id):
        logger.error(f"License with license_id {license_id} already exist.")
        raise HTTPException(
            status_code=409,
            detail=f"License with license_id {license_id} already exist.",
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

@router.patch("/redeploy")
async def redeploy_licenses(
    license_id: str = None,
    client_name: str = None,
    license_handler: LicenseHandler = Depends(get_license_handler),
    terminate_license_deployment=Depends(get_terminate_license_deployment),
    create_deployment=Depends(get_create_deployment),
):
    licenses = []
    if license_id is not None:
        if license_id == "all":
            licenses = await license_handler._get_licenses()
        else:
            license = await license_handler._get_license_by_license_id(license_id)
            licenses = [license]

    if client_name is not None:
        licenses = await license_handler._get_license_by_client_name(client_name)

    if len(licenses) == 0:
        return {"message": "No license found."}

    for license in licenses:
        license_id = license['license_id']
        logger.info(f"Terminating deployment {license['k8s_deployment_id']}")
        try:
            terminate_license_deployment(license_id)
        except Exception as e:
            logger.error(f"Couldn't terminate Deployment. Error: {e}")
        logger.info(f"Creating deployment for {license_id}")
        await create_deployment(license_id, license_handler)

    return {"message": "Licenses redeployed."}


# TODO: Add route to re-deploy license deployments.
