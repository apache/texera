from fastapi import APIRouter

from core.classifier import list_scenarios

router = APIRouter()


@router.get("/scenarios")
async def scenarios():
    return {"scenarios": list_scenarios()}
