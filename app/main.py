from fastapi import FastAPI
from routes.resource_routes import router as resource_router
from routes.health import router as health_router

app = FastAPI()

app.include_router(resource_router, prefix="/resources", tags=["Resources"])
app.include_router(health_router, tags=["Health"])


@app.get("/")
def read_root():
    return {"message": "Hello from questions service!"}
