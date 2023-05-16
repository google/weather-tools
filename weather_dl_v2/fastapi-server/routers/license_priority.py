from fastapi import APIRouter, HTTPException, UploadFile
from firestore_db.db import fake_license_priority_db
import shutil
import tempfile
import json

router = APIRouter(
    prefix="/license-priority",
    tags=["license-priority"],
    responses={404: {"description": "Not found"}},
)

# Users can change the execution order of config per license basis.
# List the licenses priority
@router.get("/")
async def get_all_license_priority():
    return fake_license_priority_db


# Get particular license priority
@router.get("/{license_id}")
async def get_license_priority(license_id: str):
    if license_id not in fake_license_priority_db:
        raise HTTPException(status_code=404, detail="License's priority not found.")
    return {"license_id": license_id, "priority": fake_license_priority_db[license_id]}


def parse_config(file) -> dict:
    """Reads `*.json` files."""
    try:
        return json.load(file)
    except json.JSONDecodeError:
        pass

    
# Change config's priority on particular license
@router.post("/{license_id}")
def submit_download(license_id: str, file: UploadFile | None = None):
    if license_id not in fake_license_priority_db:
        raise HTTPException(status_code=404, detail="License's priority not found.")
    if not file:
        return {"message": "No upload file sent."}
    else:
        try:
            with tempfile.NamedTemporaryFile() as dest_file:
                shutil.copyfileobj(file.file, dest_file)
                priority_dict = parse_config(dest_file)
                fake_license_priority_db[license_id].update(priority_dict)
            return {"message": f"'{license_id}' license priority updated successfully."}
        except:
            return {"message": f"Failed to update '{license_id}' license priority."}