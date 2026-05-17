import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from api import analyze, build, health, instantiate, scenarios, upload
from core.workflow_builder import classification_insight_output_column_count

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

app = FastAPI(title="BioFlow Genesis Service", version="0.2.0")

app.include_router(health.router, prefix="/api/genesis")
app.include_router(upload.router, prefix="/api/genesis")
app.include_router(analyze.router, prefix="/api/genesis")
app.include_router(instantiate.router, prefix="/api/genesis")
app.include_router(build.router, prefix="/api/genesis")
app.include_router(scenarios.router, prefix="/api/genesis")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"error": str(exc)})


@app.on_event("startup")
async def on_startup():
    n = classification_insight_output_column_count()
    logging.getLogger(__name__).info(
        "[BOOT] workflow_builder loaded. Classification insight schema: %s columns",
        n,
    )
    print("🧬 BioFlow Genesis Service ready on http://localhost:9099")
