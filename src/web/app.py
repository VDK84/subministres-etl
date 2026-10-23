# pyright: reportMissingImports=false
import asyncio
import email
import html
import io
import logging
import os
import re
import zipfile
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any

import requests
import uvicorn
from fastapi import (
    BackgroundTasks,
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from src.config import settings
from src.connectors import (
    BonpreuConnector,
    CatgasConnector,
    DigiConnector,
    VeoliaConnector,
)
from src.email_client import decode_str, imap_client
from src.influx_writer import influx_writer
from src.models import FacturaMetriques, FacturaResult
from src.pdf_parser import extract_text_from_pdf_bytes, parse_euro_amount
from src.storage import storage

# Logger i buffer circular per als logs de la interfície
logger = logging.getLogger("web_app")
log_buffer: deque[str] = deque(maxlen=200)

main_loop: asyncio.AbstractEventLoop | None = None


class ConnectionManager:
    """Gestiona les connexions WebSocket actives per emetre logs i notificacions de versió."""

    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast_log(self, formatted_line: str) -> None:
        for ws in list(self.active_connections):
            try:
                await ws.send_json({"type": "log", "line": formatted_line})
            except (WebSocketDisconnect, ConnectionResetError, OSError):
                self.disconnect(ws)

    async def broadcast_version(self, version: str) -> None:
        for ws in list(self.active_connections):
            try:
                await ws.send_json({"type": "version", "version": version})
            except (WebSocketDisconnect, ConnectionResetError, OSError):
                self.disconnect(ws)


ws_manager = ConnectionManager()


class WebLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            log_buffer.append(msg)
            # Emetre immediatament per WebSocket si hi ha un event loop i clients connectats
            if main_loop and main_loop.is_running() and ws_manager.active_connections:
                asyncio.run_coroutine_threadsafe(ws_manager.broadcast_log(msg), main_loop)
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


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()
    await asyncio.to_thread(auto_annotate_k8s_pod)
    yield


app = FastAPI(title="Subministres ETL", version=settings.APP_VERSION, lifespan=lifespan)

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


# Estat d'execució global
execution_state: dict[str, Any] = {
    "is_running": False,
    "last_run": None,
    "last_status": "Idle",
    "last_results": [],
    "active_connector": None,
}


CONNECTORS_MAP: dict[str, Any] = {
    "veolia": VeoliaConnector(),
    "catgas": CatgasConnector(),
    "bonpreu": BonpreuConnector(),
    "digi": DigiConnector(),
}


def get_all_connectors_map() -> dict[str, Any]:
    return CONNECTORS_MAP


MESOS_CATALANS = {
    "01": "Gen",
    "02": "Feb",
    "03": "Mar",
    "04": "Abr",
    "05": "Mai",
    "06": "Jun",
    "07": "Jul",
    "08": "Ago",
    "09": "Set",
    "10": "Oct",
    "11": "Nov",
    "12": "Des",
}


# Memòria cau per no re-parsejar els PDFs del NAS a cada recàrrega de pàgina
_pdf_amount_cache: dict[str, tuple[float, float]] = {}  # rel_path -> (mtime, amount)


def list_nas_invoices() -> list[dict[str, Any]]:
    """Explora el directori del NAS Samba per llistar totes les factures existents amb mes i import."""
    root = Path(settings.NAS_MOUNT_PATH)
    if not root.exists():
        return []

    amounts_map = influx_writer.get_factures_amounts()
    invoices: list[dict[str, Any]] = []

    for pdf_path in root.glob("**/*.pdf"):
        try:
            stat = pdf_path.stat()
            rel_path = pdf_path.relative_to(root)
            parts = rel_path.parts  # [tipus, proveidor, any, filename]
            tipus = parts[0] if len(parts) > 0 else "general"
            proveidor = parts[1] if len(parts) > 1 else "desconegut"
            any_str = parts[2] if len(parts) > 2 else ""

            # 1. Deducció del Mes (ex: 2026_10_catgas_FG26086862 -> 10)
            fn_parts = pdf_path.stem.split("_")
            mes_code = "01"
            if len(fn_parts) >= 2 and fn_parts[1].isdigit():
                mes_code = fn_parts[1].zfill(2)
            elif len(fn_parts) >= 2 and "-" in fn_parts[1]:
                # Si és bimensual/trimestral (ex: 09-10), agafem el primer mes del rang
                sub_p = fn_parts[1].split("-")
                if sub_p[0].isdigit():
                    mes_code = sub_p[0].zfill(2)
            else:
                mes_code = datetime.fromtimestamp(stat.st_mtime).strftime("%m")

            mes_label = f"{MESOS_CATALANS.get(mes_code, mes_code)} ({mes_code})"

            # 2. Deducció de l'Import
            num_clean = pdf_path.stem.lower()
            for p in ["catgas", "veolia", "bonpreu", "digi"]:
                if f"_{p}_" in num_clean:
                    num_clean = num_clean.split(f"_{p}_", 1)[1]
                    break

            import_val = amounts_map.get(num_clean) or amounts_map.get(pdf_path.stem.lower())
            if import_val is None:
                cached = _pdf_amount_cache.get(str(rel_path))
                if cached and cached[0] == stat.st_mtime:
                    import_val = cached[1]
                else:
                    try:
                        pdf_data = pdf_path.read_bytes()
                        txt = extract_text_from_pdf_bytes(pdf_data)
                        import_val = parse_euro_amount(txt)
                        if import_val is not None:
                            _pdf_amount_cache[str(rel_path)] = (stat.st_mtime, import_val)
                    except (OSError, RuntimeError, ValueError):
                        pass

            if import_val is not None:
                import_str = f"{import_val:.2f} €"
            else:
                import_str = "- €"
                import_val = 0.0

            invoices.append(
                {
                    "filename": pdf_path.name,
                    "tipus": tipus,
                    "proveidor": proveidor,
                    "any": any_str,
                    "mes": mes_code,
                    "mes_label": mes_label,
                    "import_val": import_val,
                    "import_str": import_str,
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
    invoices = await asyncio.to_thread(list_nas_invoices)

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
async def trigger_sync(
    background_tasks: BackgroundTasks,
    connector: str = Form(default=""),
    mode: str = Form(default="normal"),
):
    """Dispara la sincronització en segon pla."""
    if execution_state["is_running"]:
        return HTMLResponse(
            '<div class="p-4 mb-4 text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300" role="alert">'
            "Ja hi ha una sincronització en curs. Si us plau, espera que finalitzi.</div>"
        )

    target = connector.strip() if connector else None
    sync_mode = mode.strip() if mode else "normal"
    background_tasks.add_task(run_sync_task, target, mode=sync_mode)

    target_escaped = html.escape(target.upper()) if target else None
    target_desc = f"del proveïdor {target_escaped}" if target_escaped else "de tots els proveïdors"
    mode_desc = "històrica completa (730 dies)" if sync_mode == "all" else "ordinària"
    return HTMLResponse(
        f'<div class="p-4 mb-4 text-sm text-green-800 rounded-lg bg-green-50 dark:bg-gray-800 dark:text-green-400" role="alert">'
        f"🚀 Sincronització {mode_desc} iniciada {target_desc} en segon pla. Els logs s'estan actualitzant a continuació.</div>"
    )


@app.websocket("/ws/logs")
async def websocket_logs_endpoint(websocket: WebSocket):
    """Canal WebSocket bidireccional per transmetre logs en temps real i sincronització de versió."""
    await ws_manager.connect(websocket)
    try:
        # 1. Enviar la versió actual del servidor per al version-check del frontend
        await websocket.send_json({"type": "version", "version": f"v{settings.APP_VERSION}"})
        # 2. Enviar els darrers logs acumulats per a la càrrega inicial
        for line in list(log_buffer)[-40:]:
            await websocket.send_json({"type": "log", "line": line})
        while True:
            # Manté la connexió oberta
            await websocket.receive_text()
    except (WebSocketDisconnect, ConnectionResetError, OSError):
        ws_manager.disconnect(websocket)


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
        html_lines.append(f'<div class="{color} py-0.5">{html.escape(line)}</div>')

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

    target_escaped = html.escape(target.upper()) if target else None
    desc = (
        f"de l'última factura de {target_escaped}"
        if target_escaped
        else "de les últimes factures de tots els proveïdors"
    )
    return HTMLResponse(
        f'<div class="p-4 mb-4 text-sm text-emerald-800 rounded-lg bg-emerald-50 dark:bg-gray-800 dark:text-emerald-400">'
        f"⚡ Cerca ràpida iniciada {desc}. Si ja és al NAS, no es duplicarà.</div>"
    )


@app.get("/api/history/{connector}")
async def get_connector_history(connector: str):
    """Consulta les factures històriques disponibles sense bloquejar l'event loop."""
    connectors = get_all_connectors_map()
    conn = connectors.get(connector.lower())
    if not conn:
        raise HTTPException(status_code=404, detail=f"Proveïdor '{connector}' desconegut.")

    invoices = await asyncio.to_thread(conn.fetch_available_invoices, imap_client)
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

        inv_id = html.escape(str(inv.get("id", "")))
        inv_data = html.escape(str(inv.get("data", "-")))
        inv_num = html.escape(str(inv.get("numero", "-")))

        html_items.append(
            f'<tr class="border-b border-slate-800/60 hover:bg-slate-800/30">'
            f'<td class="py-2.5 px-3">'
            f'<input type="checkbox" name="selected_invoices" value="{inv_id}" {checked} class="bulk-cb rounded bg-slate-900 border-slate-700 text-emerald-600 focus:ring-emerald-500">'
            f"</td>"
            f'<td class="py-2.5 px-3 font-mono text-xs text-white">{inv_data}</td>'
            f'<td class="py-2.5 px-3 text-xs text-slate-300 font-mono truncate max-w-[200px]" title="{inv_num}">{inv_num}</td>'
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

      <form id="history-sync-form" hx-post="/api/sync" hx-target="#notification-area" onsubmit="closeHistoryModal()" class="space-y-4">
        <input type="hidden" name="connector" value="{conn.name}">
        <input type="hidden" name="mode" value="all">

        <!-- Accions ràpides KISS -->
        <div class="flex flex-wrap items-center justify-between gap-3 bg-slate-950/60 p-3 rounded-xl border border-slate-800">
          <div class="flex items-center gap-2 text-xs">
            <button type="button" onclick="toggleAllCheckboxes(true)" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg border border-slate-700">Seleccionar Tot</button>
            <button type="button" onclick="toggleAllCheckboxes(false)" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg border border-slate-700">Deseleccionar</button>
            <button type="button" onclick="selectOnlyPending()" class="px-2.5 py-1 bg-amber-500/10 hover:bg-amber-500/20 text-amber-300 rounded-lg border border-amber-500/30">Només Pendents</button>
          </div>

          <button type="submit" class="px-4 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold rounded-lg shadow transition flex items-center gap-1.5">
            <span>📥 Sincronitzar Tot l'Històric</span>
          </button>
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
      </form>
    </div>
    """
    return HTMLResponse(modal_html)


def detect_provider_from_content(pdf_bytes: bytes, filename: str) -> str:
    """Detecta el proveïdor analitzant el nom de fitxer o el text del PDF."""
    text = extract_text_from_pdf_bytes(pdf_bytes) or ""
    combined = (filename + " " + text).lower()

    if "bonpreu" in combined or "esclat" in combined:
        return "bonpreu"
    if "digi" in combined or "digimobil" in combined:
        return "digi"
    if "veolia" in combined or "agbar" in combined or "aigües" in combined or "aigues" in combined:
        return "veolia"
    if "catgas" in combined:
        return "catgas"
    return "bonpreu"


def extract_pdfs_from_eml(eml_bytes: bytes) -> list[tuple[bytes, str, str | None]]:
    """Extreu fitxers PDF d'un correu .eml, retornant (pdf_bytes, filename, provider_hint)."""
    results: list[tuple[bytes, str, str | None]] = []
    try:
        msg = email.message_from_bytes(eml_bytes)
        sender = decode_str(msg.get("From", ""))
        subject = decode_str(msg.get("Subject", ""))
        combined_meta = f"{sender} {subject}".lower()

        provider_hint: str | None = None
        if "bonpreu" in combined_meta:
            provider_hint = "bonpreu"
        elif "digi" in combined_meta:
            provider_hint = "digi"
        elif "veolia" in combined_meta or "agbar" in combined_meta:
            provider_hint = "veolia"
        elif "catgas" in combined_meta:
            provider_hint = "catgas"

        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", "")).lower()
            fname = part.get_filename()
            if fname:
                fname = decode_str(fname)

            is_pdf_name = bool(fname and fname.lower().endswith(".pdf"))
            is_image = bool(
                fname and fname.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".webp"))
            )
            if is_image:
                continue

            if is_pdf_name or "attachment" in cd:
                payload = part.get_payload(decode=True)
                if isinstance(payload, bytes) and payload.startswith(b"%PDF"):
                    results.append((payload, fname or "adjunt.pdf", provider_hint))
            elif ct == "message/rfc822" or (fname and fname.lower().endswith(".eml")):
                sub_payload = part.get_payload(decode=True)
                if isinstance(sub_payload, bytes):
                    results.extend(extract_pdfs_from_eml(sub_payload))
    except (TypeError, ValueError, OSError) as e:
        logger.warning(f"Error parsejant fitxer EML: {e}")
    return results


MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # Límit màxim de 50 MB per seguretat


@app.post("/api/upload")
async def upload_invoice_files(
    file: Annotated[UploadFile, File()],
    proveidor: Annotated[str, Form()] = "auto",
):
    """Pujada manual de factures (Dropzone): admet PDF, EML o ZIP (amb múltiples PDFs/EMLs)."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Nom de fitxer no proporcionat.")

    fname_lower = file.filename.lower()
    if not fname_lower.endswith((".pdf", ".eml", ".zip")):
        raise HTTPException(
            status_code=400,
            detail="Format no admès. Només es permeten fitxers .pdf, .eml o .zip.",
        )

    content = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="El fitxer és buit.")
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="El fitxer supera el límit màxim de 50 MB.")

    connectors = get_all_connectors_map()

    # Recollir tots els PDFs a processar: llista de (pdf_bytes, filename, provider_hint)
    pdf_queue: list[tuple[bytes, str, str | None]] = []

    if fname_lower.endswith(".zip"):
        logger.info(f"Desempaquetant fitxer ZIP: {file.filename}")
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for zinfo in z.infolist():
                    if zinfo.is_dir() or zinfo.file_size > MAX_UPLOAD_BYTES:
                        continue
                    inner_name = zinfo.filename.lower()
                    if inner_name.endswith(".pdf"):
                        pdf_data = z.read(zinfo)
                        if pdf_data.startswith(b"%PDF"):
                            pdf_queue.append((pdf_data, Path(zinfo.filename).name, None))
                    elif inner_name.endswith(".eml"):
                        eml_data = z.read(zinfo)
                        pdf_queue.extend(extract_pdfs_from_eml(eml_data))
        except zipfile.BadZipFile:
            raise HTTPException(status_code=400, detail="El fitxer ZIP no és vàlid o està malmès.")
    elif fname_lower.endswith(".eml"):
        logger.info(f"Processant fitxer EML: {file.filename}")
        pdf_queue.extend(extract_pdfs_from_eml(content))
    else:
        # PDF individual
        pdf_queue.append((content, file.filename, None))

    if not pdf_queue:
        return HTMLResponse(
            '<div class="p-4 text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300">'
            "⚠️ No s'ha trobat cap factura en PDF vàlida dins del fitxer pujat.</div>"
        )

    # Processar cada PDF de la cua
    total_processats = 0
    nous_descarregats = 0
    ja_existents = 0
    total_import = 0.0

    for pdf_bytes, filename, hint in pdf_queue:
        # Determinar el proveïdor (hint, manual o autodetectat)
        target_prov = proveidor.lower()
        if target_prov == "auto" or target_prov not in connectors:
            target_prov = hint or detect_provider_from_content(pdf_bytes, filename)

        conn = connectors.get(target_prov, connectors["bonpreu"])
        metriques = conn.extract_metrics_from_pdf(pdf_bytes)

        if not metriques:
            metriques = FacturaMetriques(
                proveidor=conn.name,
                tipus=conn.tipus,
                numero_factura=f"MANUAL-{filename.replace('.pdf', '')}",
                data_emissio=date.today(),
                import_total=0.0,
            )

        res = conn.save_and_ingest(pdf_bytes, metriques, origen="manual_web_upload")
        total_processats += 1
        if res.nou_descarregat:
            nous_descarregats += 1
        else:
            ja_existents += 1

        total_import += metriques.import_total

    summary = (
        f"🎉 S'han processat amb èxit <strong>{total_processats}</strong> factures "
        f"(<strong>{nous_descarregats}</strong> noves desades al NAS, {ja_existents} ja existents omeses). "
        f"Import total registrat a InfluxDB: <strong>{total_import:.2f} €</strong>."
    )
    return HTMLResponse(
        f'<div class="p-4 text-sm text-emerald-800 rounded-lg bg-emerald-50 dark:bg-gray-800 dark:text-emerald-400">'
        f"✅ {summary}</div>"
    )


@app.get("/api/pdf/view/{rel_path:path}")
async def view_pdf(rel_path: str):
    """Serveix el PDF inline per obrir-lo al visor integrat del navegador."""
    base_resolved = Path(settings.NAS_MOUNT_PATH).resolve()
    full_path = (base_resolved / rel_path).resolve()
    if not full_path.is_relative_to(base_resolved) or not full_path.is_file():
        raise HTTPException(status_code=404, detail="Factura no trobada.")

    return FileResponse(
        path=full_path,
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename={full_path.name}"},
    )


@app.get("/api/pdf/download/{rel_path:path}")
async def download_pdf(rel_path: str):
    """Descarrega el PDF directament."""
    base_resolved = Path(settings.NAS_MOUNT_PATH).resolve()
    full_path = (base_resolved / rel_path).resolve()
    if not full_path.is_relative_to(base_resolved) or not full_path.is_file():
        raise HTTPException(status_code=404, detail="Factura no trobada.")

    return FileResponse(
        path=full_path,
        media_type="application/pdf",
        filename=full_path.name,
    )


@app.delete("/api/invoice/{rel_path:path}")
async def delete_invoice(rel_path: str):
    """Elimina una factura del NAS Samba i suprimeix les mètriques d'InfluxDB per evitar dades errònies a Grafana."""
    parts = Path(rel_path).parts
    proveidor = parts[1] if len(parts) > 1 else ""

    # Deduïm el número de factura preservant majúscules/minúscules
    stem = Path(rel_path).stem
    numero_factura = stem
    for p in ["bonpreu", "digi", "veolia", "catgas"]:
        match = re.split(rf"_{p}_", stem, maxsplit=1, flags=re.IGNORECASE)
        if len(match) == 2:
            numero_factura = match[1]
            break

    # 1. Eliminar del NAS
    deleted_nas = storage.delete_pdf(rel_path)

    # 2. Eliminar d'InfluxDB
    deleted_influx = False
    if proveidor:
        deleted_influx = influx_writer.delete_factura_metrics(proveidor, numero_factura)

    logger.info(
        f"Factura '{rel_path}' eliminada: NAS={deleted_nas}, InfluxDB={deleted_influx} (Proveïdor: {proveidor}, Factura: {numero_factura})"
    )
    return HTMLResponse(content="", status_code=200)


def start() -> None:
    """Punt d'entrada per executar el servidor web FastAPI amb Uvicorn."""
    import os

    bind_host = os.getenv("APP_HOST", "127.0.0.1")
    uvicorn.run("src.web.app:app", host=bind_host, port=8000, reload=False)


if __name__ == "__main__":
    start()
