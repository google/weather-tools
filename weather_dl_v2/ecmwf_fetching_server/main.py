from fastapi import BackgroundTasks, FastAPI, UploadFile
from fastapi.responses import HTMLResponse
from pipeline import start_processing_config
import shutil

app = FastAPI()

def upload(file: UploadFile):
    dest = f"./config_files/{file.filename}"
    with open(dest, "wb+") as dest_:
        shutil.copyfileobj(file.file, dest_)
    return dest
            
@app.post("/uploadfile/")
def create_upload_file(file: UploadFile | None = None, background_tasks: BackgroundTasks = BackgroundTasks()):
    if not file:
        return {"message": "No upload file sent"}
    else:
        dest = upload(file)
        background_tasks.add_task(start_processing_config, dest)
        return {"info": f"file '{file.filename}' saved at '{dest}'"}

@app.get("/")
async def main():
    content = """
<body>
<form action="/uploadfile/" enctype="multipart/form-data" method="post">
<input name="file" type="file">
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)