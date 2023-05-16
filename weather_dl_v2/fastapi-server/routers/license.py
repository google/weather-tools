from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from firestore_db.db import fake_licenses_db, fake_license_priority_db

class License(BaseModel):
    client_name: str
    number_of_requests: int
    api_key: str
    api_url: str


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
        licenses = {k: v for k, v in fake_licenses_db.items() if v["client_name"] == client_name}
        return licenses
    else:
        return fake_licenses_db


# Get particular license
@router.get("/{license_id}")
async def get_license(license_id: str):
    if license_id not in fake_licenses_db:
        raise HTTPException(status_code=404, detail="License not found")
    return {"license_id": license_id, "client_name": fake_licenses_db[license_id]["client_name"]}


# Update existing license
@router.put("/{license_id}")
async def update_license(license_id: str, license: License):
    try:
        license_dict = license.dict()
        fake_licenses_db[license_id].update(license_dict)
        return {"license_id": license_id, "name": "License updated successfully."}
    except:
        return {"license_id": license_id, "message": "No such license to update."}


# Add/Update k8s deployment ID for existing license (intenally)
@router.put("/server/{license_id}")
async def update_license_internal(license_id: str, license: LicenseInternal):
    try:
        license_dict = license.dict()
        fake_licenses_db[license_id].update(license_dict)
        return {"license_id": license_id, "name": "License updated successfully."}
    except:
        return {"license_id": license_id, "message": "No such license to update."}
   
 
# Add new license
@router.post("/")
async def add_license(license: License):
    license_dict = license.dict()
    license_dict['k8s_deployment_id'] = ""
    license_id = "l4"
    fake_licenses_db[license_id] = license_dict
    fake_license_priority_db[license_id] = {}
    return {"license_id": license_id, "message": "License removed successfully."}


# Remove license
@router.delete("/{license_id}")
async def delete_license(license_id: str):
    if license_id not in fake_licenses_db:
        raise HTTPException(status_code=404, detail="No such license to delete.")

    del fake_licenses_db[license_id]
    del fake_license_priority_db[license_id]
    return {"license_id": license_id, "message": "License removed successfully."}