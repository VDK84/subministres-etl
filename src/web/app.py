# pyright: reportMissingImports=false
from collections import deque
from datetime import date, datetime
import logging
from pathlib import Path
from typing import Annotated, Any

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from src.config import settings
from src.connectors import (
    BonpreuConnector,
    CatgasConnector,
    DigiConnector,
    VeoliaConnector,
)
from src.email_client import imap_client
from src.models import FacturaMetriques, FacturaResult

# Logger i buffer circular per als logs de la interfície
logger = logging.getLogger("web_app")
log_buffer: deque[str] = deque(maxlen=200)


class WebLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            log_buffer.append(msg)
        except (TypeError, ValueError, RecursionError):
            return


web_handler = WebLogHandler()
web_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] (%(name)s) %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(web_handler)

app = FastAPI(title="Subministres ETL", version="0.2.0")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Estat d'execució global
execution_state: dict[str, Any] = {
    "is_running": False,
    "last_run": None,
    "last_status": "Idle",
    "last_results": [],
    "active_connector": None,
}


def get_all_connectors_map():
    return {
        "veolia": VeoliaConnector(),
        "catgas": CatgasConnector(),
        "bonpreu": BonpreuConnector(),
        "digi": DigiConnector(),
    }


def list_nas_invoices() -> list[dict[str, Any]]:
    """Explora el directori del NAS Samba per llistar totes les factures existents."""
    root = Path(settings.NAS_MOUNT_PATH)
    if not root.exists():
        return []

    invoices: list[dict[str, Any]] = []
    for pdf_path in root.glob("**/*.pdf"):
        try:
            stat = pdf_path.stat()
            rel_path = pdf_path.relative_to(root)
            parts = rel_path.parts  # [tipus, proveidor, any, filename]
            tipus = parts[0] if len(parts) > 0 else "general"
            proveidor = parts[1] if len(parts) > 1 else "desconegut"
            any_str = parts[2] if len(parts) > 2 else ""

            invoices.append(
                {
                    "filename": pdf_path.name,
                    "tipus": tipus,
                    "proveidor": proveidor,
                    "any": any_str,
                    "size_kb": round(stat.st_size / 1024, 1),
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                    "rel_path": str(rel_path).replace("\\", "/"),
                }
            )
        except OSError:
            continue

    # Ordenar per data de modificació descendent
    invoices.sort(key=lambda x: x["modified"], reverse=True)
    return invoices


def run_sync_task(connector_name: str | None = None) -> None:
    """Tasca d'execució d'ETL llançada en segon pla."""
    execution_state["is_running"] = True
    execution_state["active_connector"] = connector_name or "Tots"
    execution_state["last_status"] = "Sincronitzant..."
    logger.info(f"Iniciant sincronització sota demanda: {execution_state['active_connector']}")

    connectors_map = get_all_connectors_map()
    if connector_name and connector_name.lower() in connectors_map:
        to_run = [connectors_map[connector_name.lower()]]
    else:
        to_run = [c for c in connectors_map.values() if c.is_enabled()]

    results: list[FacturaResult] = []
    has_errors = False

    for conn in to_run:
        logger.info(f"Executant connector: {conn.name.upper()} ({conn.tipus.value})")
        try:
            res = conn.run(imap_client)
            results.extend(res)
            for r in res:
                if r.errors:
                    has_errors = True
        except (OSError, RuntimeError, ValueError) as err:
            logger.error(f"Error executant connector {conn.name}: {err}")
            has_errors = True

    execution_state["is_running"] = False
    execution_state["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    execution_state["last_status"] = "Error" if has_errors else "Completat"
    execution_state["last_results"] = [r.model_dump() for r in results]
    execution_state["active_connector"] = None
    logger.info(f"Sincronització finalitzada amb estat: {execution_state['last_status']}")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Pàgina principal amb dashboard operatiu i controls."""
    invoices = list_nas_invoices()

    # Calcular darrera factura per proveïdor
    last_by_provider: dict[str, str] = {}
    for inv in invoices:
        prov = inv["proveidor"].lower()
        if prov not in last_by_provider:
            last_by_provider[prov] = inv["filename"]

    status_cards = [
        {
            "id": "veolia",
            "name": "Veolia",
            "tipus": "Aigua",
            "enabled": settings.VEOLIA_ENABLED,
            "icon": "droplet",
            "color": "blue",
            "strategy": "Email (PDF adjunt)",
            "last_invoice": last_by_provider.get("veolia", "Cap factura trobada"),
        },
        {
            "id": "catgas",
            "name": "Catgas",
            "tipus": "Gas Natural",
            "enabled": settings.CATGAS_ENABLED,
            "icon": "flame",
            "color": "amber",
            "strategy": "Email (PDF adjunt)",
            "last_invoice": last_by_provider.get("catgas", "Cap factura trobada"),
        },
        {
            "id": "bonpreu",
            "name": "Bonpreu Energia",
            "tipus": "Electricitat",
            "enabled": settings.BONPREU_ENABLED,
            "icon": "zap",
            "color": "yellow",
            "strategy": "Trigger Email + Scraper Web",
            "last_invoice": last_by_provider.get("bonpreu", "Cap factura trobada"),
        },
        {
            "id": "digi",
            "name": "Digi Mobil",
            "tipus": "Telefonia / Fibra",
            "enabled": settings.DIGI_ENABLED,
            "icon": "smartphone",
            "color": "indigo",
            "strategy": "Trigger Email + Scraper Mi Digi",
            "last_invoice": last_by_provider.get("digi", "Cap factura trobada"),
        },
    ]

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "state": execution_state,
            "cards": status_cards,
            "invoices": invoices,
            "logs": list(log_buffer),
            "settings": settings,
        },
    )


@app.post("/api/sync")
async def trigger_sync(background_tasks: BackgroundTasks, connector: str = Form(default="")):
    """Dispara la sincronització en segon pla."""
    if execution_state["is_running"]:
        return HTMLResponse(
            '<div class="p-4 mb-4 text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300" role="alert">'
            "Ja hi ha una sincronització en curs. Si us plau, espera que finalitzi.</div>"
        )

    target = connector.strip() if connector else None
    background_tasks.add_task(run_sync_task, target)

    target_desc = f"del proveïdor {target.upper()}" if target else "de tots els proveïdors"
    return HTMLResponse(
        f'<div class="p-4 mb-4 text-sm text-green-800 rounded-lg bg-green-50 dark:bg-gray-800 dark:text-green-400" role="alert">'
        f"🚀 Sincronització iniciada {target_desc} en segon pla. Els logs s'estan actualitzant a continuació.</div>"
    )


@app.get("/api/logs")
async def get_logs_fragment():
    """Retorna els logs recents en format HTML per a polling HTMX."""
    lines = list(log_buffer)[-40:]
    if not lines:
        return HTMLResponse('<div class="text-gray-400 italic">No hi ha logs recents...</div>')

    html_lines = []
    for line in lines:
        color = "text-gray-300"
        if "ERROR" in line:
            color = "text-red-400 font-semibold"
        elif "WARNING" in line:
            color = "text-yellow-300"
        elif "OK" in line or "correctament" in line or "desada" in line:
            color = "text-emerald-400"
        html_lines.append(f'<div class="{color} py-0.5">{line}</div>')

    is_running_badge = ""
    if execution_state["is_running"]:
        is_running_badge = (
            f'<div class="mb-2 inline-flex items-center gap-2 px-2.5 py-1 text-xs font-medium text-blue-400 bg-blue-950/60 rounded border border-blue-800 animate-pulse">'
            f"<span>Executant {execution_state['active_connector']}...</span></div>"
        )

    return HTMLResponse(is_running_badge + "".join(html_lines))


@app.post("/api/upload")
async def upload_invoice_pdf(
    file: Annotated[UploadFile, File()],
    proveidor: Annotated[str, Form()],
):
    """Pujada manual de factura PDF (Dropzone / Drag & Drop)."""
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="El fitxer ha de ser un PDF vàlid.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="El fitxer PDF és buit.")

    connectors = get_all_connectors_map()
    connector = connectors.get(proveidor.lower())
    if not connector:
        raise HTTPException(status_code=400, detail=f"Proveïdor '{proveidor}' desconegut.")

    logger.info(f"Processant pujada manual de factura {file.filename} per al proveïdor {proveidor.upper()}")

    metriques = connector.extract_metrics_from_pdf(content)
    if not metriques:
        metriques = FacturaMetriques(
            proveidor=connector.name,
            tipus=connector.tipus,
            numero_factura=f"MANUAL-{date.today().strftime('%Y%m%d')}",
            data_emissio=date.today(),
            import_total=0.0,
        )

    _ = connector.save_and_ingest(content, metriques, origen="manual_web_upload")

    msg = f"Factura '{file.filename}' processada i desada correctament al NAS Samba per a {proveidor.upper()}."
    return HTMLResponse(
        f'<div class="p-4 text-sm text-green-800 rounded-lg bg-green-50 dark:bg-gray-800 dark:text-green-400">'
        f"✅ {msg} (Total: {metriques.import_total:.2f} €)</div>"
    )


@app.get("/api/pdf/view/{rel_path:path}")
async def view_pdf(rel_path: str):
    """Serveix el PDF inline per obrir-lo al visor integrat del navegador."""
    full_path = (Path(settings.NAS_MOUNT_PATH) / rel_path).resolve()
    # Validació de seguretat path traversal
    if not str(full_path).startswith(str(Path(settings.NAS_MOUNT_PATH).resolve())) or not full_path.exists():
        raise HTTPException(status_code=404, detail="Factura no trobada.")

    return FileResponse(
        path=full_path,
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename={full_path.name}"},
    )


@app.get("/api/pdf/download/{rel_path:path}")
async def download_pdf(rel_path: str):
    """Descarrega el PDF directament."""
    full_path = (Path(settings.NAS_MOUNT_PATH) / rel_path).resolve()
    if not str(full_path).startswith(str(Path(settings.NAS_MOUNT_PATH).resolve())) or not full_path.exists():
        raise HTTPException(status_code=404, detail="Factura no trobada.")

    return FileResponse(
        path=full_path,
        media_type="application/pdf",
        filename=full_path.name,
    )


def start() -> None:
    """Punt d'entrada per executar el servidor web FastAPI amb Uvicorn."""
    import os

    bind_host = os.getenv("APP_HOST", "127.0.0.1")
    uvicorn.run("src.web.app:app", host=bind_host, port=8000, reload=False)


if __name__ == "__main__":
    start()
