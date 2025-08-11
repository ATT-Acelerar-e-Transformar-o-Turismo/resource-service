from fastapi import APIRouter
from .health import router as health_router
from .resource_routes import router as resource_router

router = APIRouter()
router.include_router(resource_router, prefix="/resources", tags=["Resources"])
router.include_router(health_router, prefix="/health", tags=["Health"])
