import threading

from fastapi import FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware

from core.application import Application
from core.debug_mapper_service import DebugMapperService
from log_manager.logger_manager import LoggerManager

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
)

logger = LoggerManager("CoreMain").get_logger()
_pipeline_app: Application | None = None
_pipeline_thread: threading.Thread | None = None
_pipeline_lock = threading.Lock()


def _pipeline_bootstrap():
    global _pipeline_app
    try:
        _pipeline_app = Application()
        _pipeline_app.start_application()
        _pipeline_app.run_pipeline()
    except Exception as exc:
        logger.error(f"Pipeline bootstrap failed in server mode: {exc}")


@app.on_event("startup")
def startup_pipeline():
    global _pipeline_thread
    with _pipeline_lock:
        if _pipeline_thread is not None and _pipeline_thread.is_alive():
            logger.info("Pipeline thread already running")
            return
        _pipeline_thread = threading.Thread(target=_pipeline_bootstrap, daemon=True, name="pipeline-bootstrap")
        _pipeline_thread.start()
        logger.info("Pipeline bootstrap thread started")


@app.get("/")
async def root():
    return {"message": "Debug server is running"}


@app.get("/health")
async def health():
    alive = _pipeline_thread is not None and _pipeline_thread.is_alive()
    return {
        "server": "up",
        "pipeline_thread_alive": alive,
        "pipeline_initialized": _pipeline_app is not None,
    }


def _get_debug_service() -> DebugMapperService:
    if _pipeline_app is None:
        raise HTTPException(status_code=503, detail="Pipeline is not initialized yet.")
    return DebugMapperService(_pipeline_app.get_all_datasources(), _pipeline_app.db_instance)


@app.get("/debug/mappers")
async def debug_mappers():
    service = _get_debug_service()
    return {
        "mappers": service.list_endpoints(),
        "targets": ["staging", "enrichment", "mapping"],
    }


@app.get("/debug/mappers/{mapper_endpoint}/{target}")
async def debug_mapper_data(mapper_endpoint: str, target: str, limit: int = 100):
    try:
        service = _get_debug_service()
        return service.fetch(mapper_endpoint=mapper_endpoint, target=target, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
