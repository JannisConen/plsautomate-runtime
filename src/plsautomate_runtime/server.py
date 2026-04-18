"""FastAPI application factory and route registration."""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile

from plsautomate_runtime import __version__
from plsautomate_runtime.auth import APIKeyAuth
from plsautomate_runtime.config import AppConfig, ProcessConfig
from plsautomate_runtime.db import (
    close_db,
    create_execution,
    get_execution,
    get_last_execution_time,
    get_session_factory,
    init_db,
    list_executions,
    trigger_ref_exists,
    update_execution,
)
from plsautomate_runtime.executor import Executor
from plsautomate_runtime.files import process_uploaded_file, resolve_file_refs
from plsautomate_runtime.observability import get_execution_stats
from plsautomate_runtime.pipeline import Pipeline
from plsautomate_runtime.scheduler import Scheduler
from plsautomate_runtime.storage import StorageBackend, create_storage

logger = logging.getLogger(__name__)

_start_time: float = 0.0


def create_app(config: AppConfig) -> FastAPI:
    """Create and configure the FastAPI application."""

    # Resolve database URL based on logging backend
    if config.logging_config.backend == "postgres":
        db_url = os.environ.get("LOGGING_DATABASE_URL", config.database.url)
    else:
        db_url = config.database.url

    # Initialize subsystems
    storage = create_storage(config.storage)

    # Initialize webhook logging backend if configured
    webhook_backend = None
    if config.logging_config.backend == "webhook" and config.logging_config.webhook_url:
        from plsautomate_runtime.db import WebhookLoggingBackend

        webhook_secret = os.environ.get("WEBHOOK_SECRET") if config.logging_config.webhook_auth else None
        webhook_backend = WebhookLoggingBackend(
            url=config.logging_config.webhook_url,
            secret=webhook_secret,
        )
    executor = Executor(config.llm.model, observability=config.observability)
    auth_dep = APIKeyAuth(config.auth)
    scheduler = Scheduler()
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _start_time
        _start_time = time.monotonic()

        # Init DB
        await init_db(db_url)

        # Secrets come from environment variables
        secrets = dict(os.environ)

        # Initialize connectors
        connectors = _init_connectors(config, secrets, storage)
        for cname, conn in connectors.items():
            try:
                await conn.validate()
                logger.info(f"Connector '{cname}' validated")
            except Exception as e:
                logger.error(f"Connector '{cname}' validation failed: {e}")

        # Initialize pipeline
        pipeline = Pipeline(config, executor, storage, secrets)
        pipeline.discover_modules()
        app.state.pipeline = pipeline

        # Concurrency limiter
        max_concurrent = config.max_concurrent_executions
        semaphore = asyncio.Semaphore(max_concurrent)
        app.state.execution_semaphore = semaphore
        logger.info("Max concurrent executions: %d", max_concurrent)

        # Start scheduler and register cron jobs
        await scheduler.start()
        _register_scheduled_jobs(config, scheduler, pipeline, connectors, semaphore, storage)

        # Register review expiration job (every minute)
        if any(p.review.enabled for p in config.processes.values()):
            scheduler.add_cron_job(
                job_id="_review_expiration",
                cron_expression="* * * * *",
                func=pipeline.expire_reviews,
            )

        yield

        await scheduler.stop()
        await close_db()

    app = FastAPI(
        title="PlsAutomate Runtime",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.pipeline = None  # Set during lifespan, or injected in tests

    # Mount Gradio demo UI (if enabled and gradio is installed).
    # MUST be outside lifespan — Gradio's queue worker starts via ASGI
    # startup events, which only fire if mounted before the app starts.
    if config.ui.enabled:
        try:
            import gradio as gr

            from plsautomate_runtime.ui import create_demo, get_gradio_auth

            demo = create_demo(config, pipeline=None)
            demo.api_open = False
            demo.queue(default_concurrency_limit=40)

            auth_fn = get_gradio_auth(config)
            mount_kwargs: dict[str, Any] = {}
            if auth_fn:
                mount_kwargs["auth"] = auth_fn
                mount_kwargs["auth_message"] = (
                    "Enter any username and your API key as password"
                )
            gr.mount_gradio_app(
                app, demo, path=config.ui.path,
                show_error=True,
                **mount_kwargs,
            )
            logger.info("Demo UI mounted at %s", config.ui.path)
        except ImportError:
            logger.info(
                "Gradio not installed — demo UI disabled. "
                "Install with: pip install plsautomate-runtime[ui]"
            )

    # --- Health endpoint (no auth) ---

    @app.get("/health")
    async def health() -> dict[str, Any]:
        uptime = int(time.monotonic() - _start_time) if _start_time else 0

        processes_status: dict[str, Any] = {}
        try:
            session_factory = get_session_factory()
            async with session_factory() as session:
                for pname in config.processes:
                    last = await get_last_execution_time(session, pname)
                    processes_status[pname] = {
                        "status": "active",
                        "last_execution": last.isoformat() if last else None,
                    }
        except Exception:
            for pname in config.processes:
                processes_status[pname] = {"status": "active", "last_execution": None}

        return {
            "status": "healthy",
            "project_id": config.project.id,
            "config_version": config.project.version,
            "runtime_version": __version__,
            "uptime_seconds": uptime,
            "processes": processes_status,
        }

    # --- Process endpoints (authenticated) ---

    for process_name, process_config in config.processes.items():
        _register_process_route(
            app, process_name, process_config, executor, storage, auth_dep, config,
            lambda: app.state.pipeline,
            lambda: getattr(app.state, "execution_semaphore", None),
            webhook_backend=webhook_backend,
        )

    # --- Execution endpoints (authenticated) ---

    @app.get("/executions")
    async def list_executions_endpoint(
        process: str | None = None,
        status: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 50,
        offset: int = 0,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        status_list = [s.strip() for s in status.split(",")] if status else None
        from_dt = datetime.fromisoformat(from_date) if from_date else None
        to_dt = datetime.fromisoformat(to_date) if to_date else None

        session_factory = get_session_factory()
        async with session_factory() as session:
            executions = await list_executions(
                session,
                process_name=process,
                status=status_list,
                from_date=from_dt,
                to_date=to_dt,
                limit=limit,
                offset=offset,
            )
            return {
                "executions": [e.to_dict() for e in executions],
                "limit": limit,
                "offset": offset,
            }

    @app.get("/executions/stats")
    async def execution_stats_endpoint(
        process: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        from_dt = datetime.fromisoformat(from_date) if from_date else None
        to_dt = datetime.fromisoformat(to_date) if to_date else None

        session_factory = get_session_factory()
        async with session_factory() as session:
            return await get_execution_stats(
                session,
                process_name=process,
                from_date=from_dt,
                to_date=to_dt,
            )

    @app.get("/executions/{execution_id}")
    async def get_execution_endpoint(
        execution_id: str,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        session_factory = get_session_factory()
        async with session_factory() as session:
            execution = await get_execution(session, execution_id)
            if execution is None:
                raise HTTPException(status_code=404, detail="Execution not found")
            return execution.to_dict()

    # --- Review endpoints (authenticated) ---

    @app.get("/reviews")
    async def list_reviews_endpoint(
        process: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        review_status = status or "pending_review"
        status_list = [s.strip() for s in review_status.split(",")]

        session_factory = get_session_factory()
        async with session_factory() as session:
            executions = await list_executions(
                session,
                process_name=process,
                status=status_list,
                limit=limit,
                offset=offset,
            )
            return {
                "reviews": [e.to_dict() for e in executions],
                "limit": limit,
                "offset": offset,
            }

    @app.get("/reviews/{execution_id}")
    async def get_review_endpoint(
        execution_id: str,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        session_factory = get_session_factory()
        async with session_factory() as session:
            execution = await get_execution(session, execution_id)
            if execution is None:
                raise HTTPException(status_code=404, detail="Execution not found")

            proc_config = config.processes.get(execution.process_name)
            result = execution.to_dict()
            result["timeout"] = proc_config.review.timeout if proc_config else "24h"
            return result

    @app.post("/reviews/{execution_id}/approve")
    async def approve_review_endpoint(
        execution_id: str,
        request: Request,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        pl = app.state.pipeline
        if not pl:
            raise HTTPException(status_code=503, detail="Pipeline not initialized")

        body = {}
        try:
            body = await request.json()
        except Exception:
            pass

        modified_output = body.get("output")
        reviewed_by = body.get("reviewed_by")

        try:
            output = await pl.approve_review(
                execution_id, modified_output, reviewed_by
            )
            return {"status": "approved", "output": output}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/reviews/{execution_id}/reject")
    async def reject_review_endpoint(
        execution_id: str,
        request: Request,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        pl = app.state.pipeline
        if not pl:
            raise HTTPException(status_code=503, detail="Pipeline not initialized")

        body = {}
        try:
            body = await request.json()
        except Exception:
            pass

        reason = body.get("reason")
        reviewed_by = body.get("reviewed_by")

        try:
            await pl.reject_review(execution_id, reason, reviewed_by)
            return {"status": "rejected"}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/reviews/{execution_id}/edit")
    async def edit_review_endpoint(
        execution_id: str,
        request: Request,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        pl = app.state.pipeline
        if not pl:
            raise HTTPException(status_code=503, detail="Pipeline not initialized")

        body = await request.json()
        modified_output = body.get("output")
        reviewed_by = body.get("reviewed_by")

        if not modified_output:
            raise HTTPException(status_code=400, detail="output is required for edit")

        try:
            output = await pl.approve_review(execution_id, modified_output, reviewed_by)
            return {"status": "edited", "output": output}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/decisions")
    async def list_decisions_endpoint(
        execution_id: str | None = None,
        request_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        from plsautomate_runtime.db import list_decisions

        session_factory = get_session_factory()
        async with session_factory() as session:
            decisions = await list_decisions(
                session,
                execution_id=execution_id,
                request_id=request_id,
                limit=limit,
                offset=offset,
            )
            return {
                "decisions": [d.to_dict() for d in decisions],
                "limit": limit,
                "offset": offset,
            }

    return app


def _make_process_handler(
    process_name: str,
    process_config: ProcessConfig,
    executor: Executor,
    storage: StorageBackend,
    auth_dep: APIKeyAuth,
    config: AppConfig,
    get_pipeline,
    get_semaphore=None,
    input_model: type | None = None,
    webhook_backend=None,
):
    """Create a process endpoint handler with properly captured closure variables.

    When input_model (a Pydantic BaseModel) is provided, FastAPI uses it for:
    - Request body validation
    - Auto-generated OpenAPI docs with typed fields
    - The /docs Swagger UI shows the exact input schema
    """

    async def process_endpoint(
        request: Request,
        _auth: str = Depends(auth_dep),
    ) -> dict[str, Any]:
        # Check if project or process is deactivated
        if not config.project.active:
            raise HTTPException(
                status_code=503,
                detail=f"Project '{config.project.id}' is deactivated.",
            )
        if not process_config.active:
            raise HTTPException(
                status_code=503,
                detail=f"Process '{process_name}' is deactivated.",
            )

        execution_id = str(uuid4())
        content_type = request.headers.get("content-type", "")

        # Parse input based on content type
        if "multipart/form-data" in content_type:
            input_data = await _parse_multipart(request, execution_id, storage)
        else:
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid JSON body")

            # Validate against InputSchema if available
            if input_model and isinstance(body, dict):
                try:
                    validated = input_model.model_validate(body)
                    input_data = validated.model_dump()
                except Exception as e:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Input validation failed: {e}",
                    )
            else:
                input_data = body

        if not isinstance(input_data, dict):
            raise HTTPException(status_code=400, detail="Input must be a JSON object")

        # Extract request_id from header or body
        request_id = request.headers.get("x-request-id")
        if isinstance(input_data, dict):
            request_id = input_data.pop("_request_id", None) or request_id

        # Extract propagated connector params from process.call chains
        # (e.g. mailbox for email actions in downstream webhook-triggered processes)
        connector_params_header = request.headers.get("x-connector-params")
        propagated_connector_params: dict | None = None
        if connector_params_header:
            try:
                import json as _json
                propagated_connector_params = _json.loads(connector_params_header)
            except Exception:
                pass

        # Resolve FileRefs: download from URL or decode base64 data
        input_data = await resolve_file_refs(input_data, execution_id, storage)

        # Use pipeline if available
        pl = get_pipeline()
        if pl:
            from plsautomate_runtime.types import TriggerContext

            # Use propagated trigger ref if present (e.g. original Gmail message ID
            # forwarded from an upstream process via process.call). Falls back to the
            # execution UUID so standalone webhook calls are unaffected.
            trigger_ref = request.headers.get("x-trigger-ref") or execution_id
            trigger = TriggerContext(type="webhook", ref=trigger_ref)
            semaphore = get_semaphore() if get_semaphore else None
            try:
                if semaphore:
                    async with semaphore:
                        output = await pl.execute_process(
                            process_name, input_data, trigger, execution_id,
                            request_id=request_id,
                            initiator=propagated_connector_params,
                        )
                else:
                    output = await pl.execute_process(
                        process_name, input_data, trigger, execution_id,
                        request_id=request_id,
                        initiator=propagated_connector_params,
                    )

                # Fire webhook if configured
                if webhook_backend:
                    try:
                        await webhook_backend.log_execution({
                            "execution_id": execution_id,
                            "process_name": process_name,
                            "status": "success",
                            "output": output,
                        })
                    except Exception as wh_err:
                        logger.warning("Webhook logging failed: %s", wh_err)

                # Cleanup temporary storage
                from plsautomate_runtime.storage import NoneStorage

                if isinstance(storage, NoneStorage):
                    storage.cleanup(execution_id)

                return output or {}
            except Exception as exc:
                from plsautomate_runtime.storage import NoneStorage

                if isinstance(storage, NoneStorage):
                    storage.cleanup(execution_id)
                raise HTTPException(status_code=500, detail=str(exc))

        # Fallback: direct LLM execution (Phase 1 mode)
        # Load prompts from prompts/ dir, fall back to legacy instructions.md, then config
        from plsautomate_runtime.pipeline import _load_prompts, _load_output_schema

        module_base = process_name.replace("-", "_")
        prompts = _load_prompts(module_base)
        instructions = prompts.get("system") or process_config.instructions
        if not instructions:
            raise HTTPException(
                status_code=501,
                detail="No system prompt configured for this process. "
                "Add prompts/system.md or configure instructions in the config.",
            )

        output_schema_model = _load_output_schema(module_base)

        session_factory = get_session_factory()
        async with session_factory() as session:
            await create_execution(
                session,
                execution_id=execution_id,
                process_name=process_name,
                process_id=process_config.process_id,
                trigger_type="webhook",
                input_data=input_data,
                runtime_version=__version__,
                config_version=config.project.version,
            )

        start = time.monotonic()
        try:
            result = await executor.execute(
                instructions=instructions,
                input_data=input_data,
                process_name=process_name,
                model_override=process_config.llm_model,
                output_schema=output_schema_model or process_config.output_schema,
            )
            duration_ms = int((time.monotonic() - start) * 1000)

            async with session_factory() as session:
                await update_execution(
                    session,
                    execution_id,
                    output=result.output,
                    status="success",
                    finished_at=datetime.utcnow(),
                    duration_ms=duration_ms,
                    llm_model=result.llm_model,
                    llm_tokens_in=result.llm_tokens_in,
                    llm_tokens_out=result.llm_tokens_out,
                    llm_cost_usd=result.llm_cost_usd,
                    llm_latency_ms=result.llm_latency_ms,
                    instructions_version=result.instructions_version,
                )

            # Fire webhook if configured
            if webhook_backend:
                try:
                    await webhook_backend.log_execution({
                        "execution_id": execution_id,
                        "process_name": process_name,
                        "status": "success",
                        "duration_ms": duration_ms,
                        "output": result.output,
                    })
                except Exception as wh_err:
                    logger.warning("Webhook logging failed: %s", wh_err)

            # Cleanup temporary storage
            from plsautomate_runtime.storage import NoneStorage

            if isinstance(storage, NoneStorage):
                storage.cleanup(execution_id)

            return result.output

        except Exception as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            async with session_factory() as session:
                await update_execution(
                    session,
                    execution_id,
                    status="error",
                    error=str(exc),
                    finished_at=datetime.utcnow(),
                    duration_ms=duration_ms,
                )

            # Cleanup temporary storage on error too
            from plsautomate_runtime.storage import NoneStorage

            if isinstance(storage, NoneStorage):
                storage.cleanup(execution_id)

            raise HTTPException(status_code=500, detail=str(exc))

    return process_endpoint


def _load_schema_model(process_name: str, kind: str) -> type | None:
    """Eagerly load InputSchema or OutputSchema for typed API docs."""
    module_base = process_name.replace("-", "_")
    class_name = f"{kind.capitalize()}Schema"
    module_path = f"processes.{module_base}.{kind}_schema"
    try:
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name, None)
    except (ImportError, Exception):
        return None


def _register_process_route(
    app: FastAPI,
    process_name: str,
    process_config: ProcessConfig,
    executor: Executor,
    storage: StorageBackend,
    auth_dep: APIKeyAuth,
    config: AppConfig,
    get_pipeline=None,
    get_semaphore=None,
    webhook_backend=None,
) -> None:
    """Register a POST /process/{name} endpoint for a process.

    Loads InputSchema and OutputSchema from the process directory at registration
    time. These are self-contained Pydantic models baked into the generated app —
    no PlsAutomate API calls needed.

    When available, FastAPI uses them for:
    - Request body validation (InputSchema)
    - Response model in OpenAPI docs (OutputSchema)
    - Typed /docs Swagger UI
    """
    input_model = _load_schema_model(process_name, "input")
    output_model = _load_schema_model(process_name, "output")
    handler = _make_process_handler(
        process_name, process_config, executor, storage, auth_dep, config,
        get_pipeline or (lambda: None),
        get_semaphore,
        input_model=input_model,
        webhook_backend=webhook_backend,
    )
    route_kwargs: dict[str, Any] = {
        "methods": ["POST"],
        "name": f"process_{process_name}",
        "description": f"Execute the {process_name} process.",
    }
    if output_model:
        route_kwargs["response_model"] = output_model
    app.add_api_route(
        f"/process/{process_name}",
        handler,
        **route_kwargs,
    )


async def _parse_multipart(
    request: Request,
    execution_id: str,
    storage: StorageBackend,
) -> dict[str, Any]:
    """Parse a multipart/form-data request into an input dict.

    Supports two modes:

    **Structured mode** (sent by the PlsAutomate test executor):
    - ``metadata`` field: JSON input with FileRefs replaced by ``{ "__mref": "fN", ... }``
      placeholders.  Each placeholder may carry a ``url`` fallback.
    - ``fN`` fields: binary file parts, one per FileRef.
    The placeholders are resolved to full FileRef dicts after all parts are collected.
    Already-uploaded files get a ``path`` field so ``resolve_file_refs`` skips them.

    **Legacy mode** (direct multipart uploads from external callers):
    No ``metadata`` field — uploaded files are collected in ``input["file"]`` (single)
    or ``input["files"]`` (multiple), preserving previous behaviour.
    """
    from plsautomate_runtime.storage import LocalStorage

    form = await request.form()
    metadata_str: str | None = None
    uploaded_files: dict[str, dict[str, Any]] = {}   # fieldName → resolved FileRef dict
    legacy_file_refs: list[dict[str, Any]] = []
    extra_fields: dict[str, Any] = {}

    for field_name, field_value in form.multi_items():
        if hasattr(field_value, "read") and hasattr(field_value, "filename"):
            # File part — resolve and store
            file_ref = await process_uploaded_file(field_value, execution_id, storage)
            ref_dict = file_ref.model_dump(by_alias=True)
            # Attach local filesystem path so resolve_file_refs can skip re-downloading
            if isinstance(storage, LocalStorage):
                ref_dict["path"] = str(storage.base_path / file_ref.key)
            uploaded_files[field_name] = ref_dict
            legacy_file_refs.append(ref_dict)
        elif field_name == "metadata":
            metadata_str = field_value if isinstance(field_value, str) else None
        else:
            extra_fields[field_name] = field_value

    await form.close()

    if metadata_str is not None:
        # Structured mode: resolve __mref placeholders with uploaded file dicts
        try:
            input_data = json.loads(metadata_str)
        except (json.JSONDecodeError, TypeError):
            input_data = {}

        input_data = _resolve_mref_placeholders(input_data, uploaded_files)
        input_data.update(extra_fields)
        return input_data

    # Legacy mode: expose files via "file" / "files" keys
    input_data = dict(extra_fields)
    if legacy_file_refs:
        if len(legacy_file_refs) == 1:
            input_data["file"] = legacy_file_refs[0]
        else:
            input_data["files"] = legacy_file_refs
    return input_data


def _resolve_mref_placeholders(
    obj: Any,
    uploaded_files: dict[str, dict[str, Any]],
) -> Any:
    """Recursively replace ``{ "__mref": "fN", ... }`` placeholders with resolved FileRef dicts.

    If the file part was uploaded, the full resolved dict (with ``path``) is used.
    If the part is missing but the placeholder carries a ``url``, a minimal FileRef with
    ``url`` is returned so ``resolve_file_refs`` can fall back to a download.
    """
    if isinstance(obj, list):
        return [_resolve_mref_placeholders(item, uploaded_files) for item in obj]
    if isinstance(obj, dict):
        mref = obj.get("__mref")
        if isinstance(mref, str):
            if mref in uploaded_files:
                return uploaded_files[mref]
            # File part missing — build a minimal ref from placeholder metadata so
            # resolve_file_refs can still attempt a URL download
            fallback: dict[str, Any] = {
                "type": "url",
                "key": mref,
                "filename": obj.get("filename", "unnamed"),
                "mimeType": obj.get("mimeType", "application/octet-stream"),
                "extension": obj.get("extension", ""),
                "size": obj.get("size", 0),
            }
            if obj.get("url"):
                fallback["url"] = obj["url"]
            return fallback
        return {k: _resolve_mref_placeholders(v, uploaded_files) for k, v in obj.items()}
    return obj


def _init_connectors(
    config: AppConfig, secrets: dict[str, str], storage: StorageBackend
) -> dict[str, Any]:
    """Initialize connectors for processes that use them."""
    from plsautomate_runtime.connectors.exchange import ExchangeConnector
    from plsautomate_runtime.connectors.gmail import GmailConnector
    from plsautomate_runtime.connectors.webhook import WebhookConnector

    connector_map = {
        "exchange": ExchangeConnector,
        "gmail": GmailConnector,
        "webhook": WebhookConnector,
    }

    connectors: dict[str, Any] = {}
    for pname, pconfig in config.processes.items():
        if pconfig.connector and pconfig.connector in connector_map:
            cls = connector_map[pconfig.connector]
            connectors[pname] = cls(
                params=pconfig.connector_params,
                secrets=secrets,
                storage=storage,
            )
    return connectors


def _register_scheduled_jobs(
    config: AppConfig,
    scheduler: Scheduler,
    pipeline: Pipeline,
    connectors: dict[str, Any],
    semaphore: asyncio.Semaphore,
    storage: StorageBackend | None = None,
) -> None:
    """Register cron-triggered jobs for scheduled processes."""
    for pname, pconfig in config.processes.items():
        if pconfig.trigger.type == "schedule" and pconfig.trigger.cron:
            connector = connectors.get(pname)

            # Capture filter config at registration time
            _filter = (
                pconfig.trigger_filter.model_dump(exclude_none=True)
                if pconfig.trigger_filter
                else None
            )

            async def _scheduled_run(
                _pname: str = pname,
                _connector: Any = connector,
                _tf: dict[str, Any] | None = _filter,
            ) -> None:
                from plsautomate_runtime.condition import evaluate_condition
                from plsautomate_runtime.types import TriggerContext

                if _connector:
                    # Fetch items from connector
                    try:
                        items = await _connector.fetch()
                    except Exception as e:
                        logger.error(f"Connector fetch failed for '{_pname}': {e}")
                        return

                    for item in items:
                        # Deduplication: check if trigger_ref already processed
                        if item.ref:
                            session_factory = get_session_factory()
                            async with session_factory() as session:
                                if await trigger_ref_exists(session, _pname, item.ref):
                                    logger.debug(
                                        "Skipping already-processed item '%s' for '%s'",
                                        item.ref, _pname,
                                    )
                                    continue

                        # Apply trigger filter (condition evaluator)
                        if _tf and _tf.get("mode") != "always":
                            if not evaluate_condition(_tf, item.data, fn_name="should_process"):
                                logger.info(
                                    "Filtered out item ref='%s' for '%s' (filter mode=%s)",
                                    item.ref, _pname, _tf.get("mode"),
                                )
                                continue

                        execution_id = str(uuid4())
                        trigger = TriggerContext(
                            type="schedule", ref=item.ref
                        )
                        input_data = item.data

                        # Resolve FileRefs into executions/{id}/ path (same as API webhook)
                        input_data = await resolve_file_refs(
                            input_data, execution_id, storage
                        )

                        async with semaphore:
                            try:
                                await pipeline.execute_process(
                                    _pname, input_data, trigger, execution_id
                                )
                            except Exception as e:
                                logger.error(
                                    f"Scheduled execution failed for '{_pname}' "
                                    f"(ref: {item.ref}): {e}"
                                )
                else:
                    # No connector — just trigger with empty input
                    trigger = TriggerContext(type="schedule")
                    async with semaphore:
                        try:
                            await pipeline.execute_process(_pname, {}, trigger)
                        except Exception as e:
                            logger.error(f"Scheduled execution failed for '{_pname}': {e}")

            scheduler.add_cron_job(
                job_id=f"process_{pname}",
                cron_expression=pconfig.trigger.cron,
                func=_scheduled_run,
            )


def _create_app_from_env() -> FastAPI | None:
    """Factory for uvicorn: reads PLSAUTOMATE_CONFIG env var to create the app.

    Usage: PLSAUTOMATE_CONFIG=path.yaml uvicorn plsautomate_runtime.server:app --reload
    """
    config_path = os.environ.get("PLSAUTOMATE_CONFIG")
    if not config_path:
        return None

    from plsautomate_runtime.config import load_config

    config = load_config(config_path)
    return create_app(config)


# Module-level app for uvicorn import (only created when PLSAUTOMATE_CONFIG is set)
app = _create_app_from_env()
