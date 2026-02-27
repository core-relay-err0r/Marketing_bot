import asyncio
import logging
import os
import sys
import traceback
from datetime import datetime

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()

print("[STARTUP] Loading web_app.py ...", flush=True)

from src.utils import load_config, setup_logging

print("[STARTUP] Core utils loaded", flush=True)

app = FastAPI(title="Web Scraper")

templates = Jinja2Templates(directory="web/templates")
app.mount("/static", StaticFiles(directory="web/static"), name="static")

active_tasks: dict[str, asyncio.Task] = {}


def _lazy_import_orchestrator():
    from src.orchestrator import run_pipeline, run_email_only, _worksheet_title_for_today
    return run_pipeline, run_email_only, _worksheet_title_for_today


def _lazy_import_sheets():
    from src.sheets import SheetsManager
    return SheetsManager


class WebSocketLogHandler(logging.Handler):
    """Captures log records and pushes them into an asyncio queue."""

    def __init__(self, queue: asyncio.Queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        try:
            self.queue.put_nowait({
                "type": "log",
                "level": record.levelname,
                "message": self.format(record),
                "ts": datetime.now().strftime("%H:%M:%S"),
            })
        except asyncio.QueueFull:
            pass


_LOGGER_NAMES = [
    "leadgen", "leadgen.scraper", "leadgen.qualifier",
    "leadgen.emailer", "leadgen.sheets", "leadgen.orchestrator",
    "leadgen.dedup", "leadgen.ai_scorer",
]


def _attach_handler(handler: logging.Handler):
    handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    for name in _LOGGER_NAMES:
        lg = logging.getLogger(name)
        lg.addHandler(handler)
        lg.setLevel(logging.INFO)


def _detach_handler(handler: logging.Handler):
    for name in _LOGGER_NAMES:
        logging.getLogger(name).removeHandler(handler)


# ── REST endpoints ─────────────────────────────────────────────

@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/config")
async def get_config():
    try:
        config = load_config()
        return {
            "countries": config["countries"],
            "niches": config["niches"],
            "niche_priority": config.get("niche_priority", config["niches"]),
            "email": config.get("email", {}),
        }
    except Exception as e:
        print(f"[ERROR] /api/config: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/sheets")
async def get_sheets():
    try:
        SheetsManager = _lazy_import_sheets()
        sheets = SheetsManager()
        sheets._connect()
        tabs = [ws.title for ws in sheets._spreadsheet.worksheets()]
        return {"sheets": tabs}
    except Exception as e:
        print(f"[ERROR] /api/sheets: {e}", flush=True)
        return {"sheets": [], "error": str(e)}


@app.get("/api/stats/all")
async def get_all_stats():
    """Return per-sheet stats for the dashboard."""
    try:
        SheetsManager = _lazy_import_sheets()
        sheets = SheetsManager()
        sheets._connect()
        result = []
        for ws in sheets._spreadsheet.worksheets():
            try:
                records = sheets._safe_get_records(ws)
                total = len(records)
                emailed = sum(1 for r in records if r.get("Email Sent") == "Yes")
                result.append({
                    "tab": ws.title,
                    "total": total,
                    "emailed": emailed,
                    "pending": total - emailed,
                })
            except Exception:
                result.append({"tab": ws.title, "total": 0, "emailed": 0, "pending": 0})
        return {"stats": result}
    except Exception as e:
        print(f"[ERROR] /api/stats/all: {e}", flush=True)
        return {"stats": [], "error": str(e)}


@app.get("/api/status/{sheet_tab}")
async def get_tab_status(sheet_tab: str):
    try:
        SheetsManager = _lazy_import_sheets()
        sheets = SheetsManager()
        return sheets.get_daily_stats(sheet_tab)
    except Exception as e:
        print(f"[ERROR] /api/status: {e}", flush=True)
        return {"error": str(e)}


# ── WebSocket: pipeline ────────────────────────────────────────

async def _stream_queue(ws: WebSocket, queue: asyncio.Queue, task: asyncio.Task):
    """Drain the log queue and forward messages until the task finishes."""
    while not task.done():
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=0.5)
            await ws.send_json(msg)
        except asyncio.TimeoutError:
            continue
        except (WebSocketDisconnect, Exception):
            task.cancel()
            return
    while not queue.empty():
        await ws.send_json(queue.get_nowait())


@app.websocket("/ws/pipeline")
async def pipeline_ws(ws: WebSocket):
    await ws.accept()
    queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
    handler = WebSocketLogHandler(queue)
    _attach_handler(handler)

    try:
        data = await ws.receive_json()
        await ws.send_json({"type": "status", "status": "running",
                            "message": "Pipeline starting…"})

        run_pipeline, _, _ = _lazy_import_orchestrator()
        task = asyncio.create_task(run_pipeline(
            city=data.get("city") or None,
            country=data.get("country") or None,
            niche=data.get("niche") or None,
            headless=True,
            send_emails=data.get("send_emails", False),
            use_ai=True,
        ))
        active_tasks["pipeline"] = task

        await _stream_queue(ws, queue, task)

        try:
            result = task.result()
            await ws.send_json({"type": "result", "status": "completed", "data": result})
        except asyncio.CancelledError:
            await ws.send_json({"type": "result", "status": "cancelled", "data": {}})
        except Exception as e:
            await ws.send_json({"type": "result", "status": "error", "message": str(e)})
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await ws.send_json({"type": "result", "status": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        _detach_handler(handler)
        active_tasks.pop("pipeline", None)


# ── WebSocket: email ──────────────────────────────────────────

@app.websocket("/ws/email")
async def email_ws(ws: WebSocket):
    await ws.accept()
    queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
    handler = WebSocketLogHandler(queue)
    _attach_handler(handler)

    try:
        data = await ws.receive_json()
        _, run_email_only, _worksheet_title_for_today = _lazy_import_orchestrator()
        sheet_tab = data.get("sheet_tab") or _worksheet_title_for_today()

        await ws.send_json({"type": "status", "status": "running",
                            "message": f"Sending emails for '{sheet_tab}'…"})

        task = asyncio.create_task(run_email_only(worksheet_title=sheet_tab))
        active_tasks["email"] = task

        await _stream_queue(ws, queue, task)

        try:
            result = task.result()
            await ws.send_json({"type": "result", "status": "completed", "data": result})
        except asyncio.CancelledError:
            await ws.send_json({"type": "result", "status": "cancelled", "data": {}})
        except Exception as e:
            await ws.send_json({"type": "result", "status": "error", "message": str(e)})
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await ws.send_json({"type": "result", "status": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        _detach_handler(handler)
        active_tasks.pop("email", None)


# ── WebSocket: stop ───────────────────────────────────────────

@app.post("/api/stop/{job}")
async def stop_job(job: str):
    task = active_tasks.get(job)
    if task and not task.done():
        task.cancel()
        return {"stopped": True}
    return {"stopped": False}


if __name__ == "__main__":
    import uvicorn
    setup_logging(verbose=False)
    port = int(os.getenv("PORT", "8000"))
    print(f"\n  Web Scraper — Web UI", flush=True)
    print(f"  http://localhost:{port}\n", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
