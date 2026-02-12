from fastapi import APIRouter
from .health import router as health_router
from .resource_routes import router as resource_router
from .wrapper_routes import router as wrapper_router
from .file_routes import router as file_router

router = APIRouter()

router.include_router(resource_router, prefix="/resources", tags=["Resources"])
router.include_router(health_router, prefix="/health", tags=["Health"])
router.include_router(wrapper_router, prefix="/resources/wrappers", tags=["Wrappers"])
router.include_router(file_router, prefix="/resources/wrappers/files", tags=["File Upload"])
