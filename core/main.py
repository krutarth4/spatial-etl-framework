import threading

from fastapi import Body, FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware

from communication.comm_service import CommService
from core.application import Application
from core.debug_mapper_service import DebugMapperService
from log_manager.logger_manager import LoggerManager, setup_file_logging
from main_core.core_config import CoreConfig
from utils.execution_time import measure_time

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_methods=["*"],
    allow_headers=["*"],
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
    setup_file_logging(CoreConfig().get_config().get("logging") or {})
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


def _get_comm_service() -> CommService:
    if _pipeline_app is None or _pipeline_app.comm_service is None:
        raise HTTPException(status_code=503, detail="Communication service is not initialized.")
    return _pipeline_app.comm_service


@app.get("/comm/tasks")
async def comm_list_tasks():
    service = _get_comm_service()
    return {"tasks": service.list_tasks()}


@app.get("/comm/tasks/{task_key}")
async def comm_get_task(task_key: str):
    service = _get_comm_service()
    task = service.get_task_status(task_key)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task '{task_key}' not found")
    return task


@app.post("/comm/tasks/{task_key}")
async def comm_upsert_task(task_key: str, payload: dict = Body(default={})):
    service = _get_comm_service()
    body = payload or {}
    if not service.get_task_status(task_key):
        service.ensure_task(
            task_key,
            owner=body.get("owner"),
            current_status=str(body.get("current_status", "idle")),
            is_completed=bool(body.get("is_completed", False)),
        )
    updated = service.update_status(
        task_key,
        owner=body.get("owner"),
        current_status=body.get("current_status"),
        last_run_status=body.get("last_run_status"),
        last_run_message=body.get("last_run_message"),
        success=bool(body.get("success", False)),
        is_completed=body.get("is_completed"),
    )
    if updated is None:
        raise HTTPException(status_code=500, detail="Failed to update task")
    return updated


@app.post("/comm/tasks/{task_key}/complete")
async def comm_complete_task(task_key: str, payload: dict = Body(default={})):
    service = _get_comm_service()
    body = payload or {}
    updated = service.update_status(
        task_key,
        owner=body.get("owner"),
        current_status=body.get("current_status", "idle"),
        last_run_status=body.get("last_run_status", "success"),
        last_run_message=body.get("last_run_message"),
        success=True,
        is_completed=True,
    )
    if updated is None:
        raise HTTPException(status_code=500, detail="Failed to complete task")
    return updated


def _get_debug_service() -> DebugMapperService:
    if _pipeline_app is None:
        raise HTTPException(status_code=503, detail="Pipeline is not initialized yet.")
    metadata_schema = None
    if _pipeline_app.metadata_service is not None and _pipeline_app.metadata_service.metadata_conf is not None:
        metadata_schema = _pipeline_app.metadata_service.metadata_conf.table_schema
    return DebugMapperService(_pipeline_app.get_all_datasources(), _pipeline_app.db_instance, metadata_schema)


@app.get("/debug/datasources")
async def debug_datasources():
    service = _get_debug_service()
    return {
        "datasources": service.list_datasources(),
    }


@app.get("/debug/datasources/{mapper_endpoint}")
@measure_time("debug_datasource_dashboard")
async def debug_datasource_dashboard(mapper_endpoint: str):
    try:
        service = _get_debug_service()
        return service.fetch_datasource_dashboard(mapper_endpoint=mapper_endpoint)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/debug/mappers")
async def debug_mappers():
    service = _get_debug_service()
    return {
        "mappers": service.list_endpoints(),
        "targets": ["staging", "enrichment", "mapping"],
    }


@app.get("/debug/mappers/{mapper_endpoint}/mapping-visualization")
async def debug_mapping_visualization(mapper_endpoint: str, limit: int = 100, way_id: int | None = None):
    try:
        service = _get_debug_service()
        return service.fetch_mapping_visualization(mapper_endpoint=mapper_endpoint, limit=limit, way_id=way_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/debug/mappers/{mapper_endpoint}/nearest-way")
async def debug_nearest_way(mapper_endpoint: str, lat: float, lng: float):
    try:
        service = _get_debug_service()
        return service.fetch_nearest_way(mapper_endpoint=mapper_endpoint, lat=lat, lng=lng)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/debug/mappers/{mapper_endpoint}/way-inspector")
async def debug_way_inspector(mapper_endpoint: str, way_id: int | None = None):
    try:
        service = _get_debug_service()
        return service.fetch_way_inspector(mapper_endpoint=mapper_endpoint, way_id=way_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/debug/mappers/{mapper_endpoint}/{target}")
async def debug_mapper_data(mapper_endpoint: str, target: str, limit: int = 100):
    try:
        service = _get_debug_service()
        return service.fetch(mapper_endpoint=mapper_endpoint, target=target, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
