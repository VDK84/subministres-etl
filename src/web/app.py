# pyright: reportMissingImports=false
import logging
import os
from collections import deque
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any

import requests
import uvicorn
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

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


# Silenciar el polling continu de /api/logs dels access logs d'Uvicorn
class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find("/api/logs") == -1


logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

app = FastAPI(title="Subministres ETL", version=settings.APP_VERSION)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def auto_annotate_k8s_pod() -> None:
    """Anota automàticament el Pod actual a Kubernetes amb la versió SemVer de l'aplicació."""
    pod_name = os.getenv("HOSTNAME")
    if not pod_name:
        return

    token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    if not token_path.exists():
        logger.debug("Execució fora de Kubernetes. Ometent auto-anotació del Pod.")
        return

    token = token_path.read_text().strip()
    ns_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    namespace = ns_path.read_text().strip() if ns_path.exists() else "subministres"
    ca_cert = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    url = f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/pods/{pod_name}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/strategic-merge-patch+json",
    }
    payload = {"metadata": {"annotations": {"app.kubernetes.io/version": f"v{settings.APP_VERSION}"}}}

    try:
        resp = requests.patch(
            url,
            json=payload,
            headers=headers,
            verify=ca_cert if Path(ca_cert).exists() else False,
            timeout=5,
        )
        if resp.status_code in (200, 201):
            logger.info(f"✅ Pod {pod_name} auto-anotat a Kubernetes amb versió: v{settings.APP_VERSION}")
        else:
            logger.warning(f"No s'ha pogut auto-anotar el Pod (HTTP {resp.status_code}): {resp.text}")
    except (OSError, RuntimeError, requests.exceptions.RequestException) as err:
        logger.warning(f"Error intentant auto-anotar el Pod a Kubernetes: {err}")


@app.on_event("startup")
async def startup_event() -> None:
    auto_annotate_k8s_pod()


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


def run_sync_task(
    connector_name: str | None = None,
    mode: str = "normal",
    limit: int | None = None,
) -> None:
    """Tasca d'execució d'ETL llançada en segon pla."""
    execution_state["is_running"] = True
    execution_state["active_connector"] = f"{connector_name or 'Tots'} ({mode})"
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
        logger.info(f"Executant connector: {conn.name.upper()} (mode={mode})")
        try:
            res = conn.run(imap_client, mode=mode, limit=limit)
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


@app.post("/api/sync/latest")
async def trigger_sync_latest(
    background_tasks: BackgroundTasks,
    connector: str = Form(default=""),
):
    """Cerca exclusivament la darrera factura disponible per a un proveïdor."""
    if execution_state["is_running"]:
        return HTMLResponse(
            '<div class="p-4 mb-4 text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300">'
            "Ja hi ha una sincronització en curs. Si us plau, espera que finalitzi.</div>"
        )

    target = connector.strip() if connector else None
    background_tasks.add_task(run_sync_task, target, mode="latest", limit=1)

    desc = f"de l'última factura de {target.upper()}" if target else "de les últimes factures de tots els proveïdors"
    return HTMLResponse(
        f'<div class="p-4 mb-4 text-sm text-emerald-800 rounded-lg bg-emerald-50 dark:bg-gray-800 dark:text-emerald-400">'
        f"⚡ Cerca ràpida iniciada {desc}. Si ja és al NAS, no es duplicarà.</div>"
    )


@app.get("/api/history/{connector}")
async def get_connector_history(connector: str):
    """Consulta les factures històriques disponibles i en retorna el fragment per al modal."""
    connectors = get_all_connectors_map()
    conn = connectors.get(connector.lower())
    if not conn:
        raise HTTPException(status_code=404, detail=f"Proveïdor '{connector}' desconegut.")

    invoices = conn.fetch_available_invoices(imap_client)
    pendents = [inv for inv in invoices if not inv.get("exists_in_nas", False)]

    # Renderitzem el fragment HTML per al modal
    html_items = []
    for inv in invoices:
        exists = inv.get("exists_in_nas", False)
        badge = (
            '<span class="px-2 py-0.5 rounded text-[11px] font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">✅ Al NAS</span>'
            if exists
            else '<span class="px-2 py-0.5 rounded text-[11px] font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20">⏳ Pendent</span>'
        )
        checked = "" if exists else "checked"

        html_items.append(
            f'<tr class="border-b border-slate-800/60 hover:bg-slate-800/30">'
            f'<td class="py-2.5 px-3">'
            f'<input type="checkbox" name="selected_invoices" value="{inv.get("id")}" {checked} class="bulk-cb rounded bg-slate-900 border-slate-700 text-emerald-600 focus:ring-emerald-500">'
            f"</td>"
            f'<td class="py-2.5 px-3 font-mono text-xs text-white">{inv.get("data", "-")}</td>'
            f'<td class="py-2.5 px-3 text-xs text-slate-300 font-mono truncate max-w-[200px]" title="{inv.get("numero", "")}">{inv.get("numero", "-")}</td>'
            f'<td class="py-2.5 px-3 text-xs font-semibold text-emerald-400 text-right">{inv.get("import", 0.0):.2f} €</td>'
            f'<td class="py-2.5 px-3 text-center">{badge}</td>'
            f"</tr>"
        )

    rows_html = (
        "".join(html_items)
        if html_items
        else (
            '<tr><td colspan="5" class="py-8 text-center text-slate-500 text-xs">No s\'ha trobat cap factura disponible o no s\'han pogut consultar metadades.</td></tr>'
        )
    )

    modal_html = f"""
    <div class="p-6 space-y-5">
      <div class="flex items-center justify-between border-b border-slate-800 pb-4">
        <div>
          <h3 class="text-base font-bold text-white flex items-center gap-2">
            <span>📚 Històric de Factures — {conn.name.upper()}</span>
          </h3>
          <p class="text-xs text-slate-400">Total trobades: {len(invoices)} | Pendents de descarregar: <strong class="text-amber-400">{len(pendents)}</strong></p>
        </div>
        <button onclick="closeHistoryModal()" class="text-slate-400 hover:text-white p-1 rounded-lg hover:bg-slate-800">
          ✕
        </button>
      </div>

      <!-- Accions ràpides KISS -->
      <div class="flex flex-wrap items-center justify-between gap-3 bg-slate-950/60 p-3 rounded-xl border border-slate-800">
        <div class="flex items-center gap-2 text-xs">
          <button type="button" onclick="toggleAllCheckboxes(true)" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg border border-slate-700">Seleccionar Tot</button>
          <button type="button" onclick="toggleAllCheckboxes(false)" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg border border-slate-700">Deseleccionar</button>
          <button type="button" onclick="selectOnlyPending()" class="px-2.5 py-1 bg-amber-500/10 hover:bg-amber-500/20 text-amber-300 rounded-lg border border-amber-500/30">Només Pendents</button>
        </div>

        <form hx-post="/api/sync" hx-target="#notification-area" onclick="closeHistoryModal()" class="flex items-center gap-2">
          <input type="hidden" name="connector" value="{conn.name}">
          <button type="submit" class="px-4 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold rounded-lg shadow transition flex items-center gap-1.5">
            <span>📥 Sincronitzar Tot l'Històric</span>
          </button>
        </form>
      </div>

      <!-- Taula de factures -->
      <div class="max-h-72 overflow-y-auto rounded-xl border border-slate-800 bg-slate-950/40">
        <table class="w-full text-left border-collapse">
          <thead class="sticky top-0 bg-slate-900 border-b border-slate-800 text-[11px] text-slate-400 uppercase tracking-wider">
            <tr>
              <th class="py-2.5 px-3 w-8"></th>
              <th class="py-2.5 px-3">Data</th>
              <th class="py-2.5 px-3">Factura</th>
              <th class="py-2.5 px-3 text-right">Import</th>
              <th class="py-2.5 px-3 text-center">Estat al NAS</th>
            </tr>
          </thead>
          <tbody>
            {rows_html}
          </tbody>
        </table>
      </div>
    </div>
    """
    return HTMLResponse(modal_html)


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
