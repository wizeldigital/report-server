from fastapi import APIRouter
from .endpoints import reports, flows

api_router = APIRouter()

api_router.include_router(reports.router, prefix="/reports", tags=["reports"])
api_router.include_router(flows.router, prefix="/flows", tags=["flows"])