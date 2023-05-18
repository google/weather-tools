from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from routers import license, download, queues

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Boot up
    print("Create database if not already exists.")
    print("Retrieve license information & create license deployment if needed.")
    yield
    # Clean up

app = FastAPI(lifespan=lifespan)

app.include_router(license.router)
app.include_router(download.router)
app.include_router(queues.router)

@app.get("/")
async def main():
    content = """
<body>
Greetings from weather-dl v2 !!
</body>
    """
    return HTMLResponse(content=content)