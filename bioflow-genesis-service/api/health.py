from fastapi import APIRouter

router = APIRouter()

VERSION = "0.2.0"


@router.get("/health")
async def health():
    return {"status": "ok", "version": VERSION}
