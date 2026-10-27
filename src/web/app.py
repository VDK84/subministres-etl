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
from src.health import health_manager
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

    async def broadcast_status(self, payload: dict[str, Any]) -> None:
        for ws in list(self.active_connections):
            try:
                await ws.send_json({"type": "status", **payload})
            except (WebSocketDisconnect, ConnectionResetError, OSError):
                self.disconnect(ws)

    async def broadcast_progress(self, payload: dict[str, Any]) -> None:
        for ws in list(self.active_connections):
            try:
                await ws.send_json({"type": "progress", **payload})
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


root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
web_handler = WebLogHandler()
web_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] (%(name)s) %(message)s", "%H:%M:%S"))
root_logger.addHandler(web_handler)


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
    "T1": "1r Trimestre",
    "T2": "2n Trimestre",
    "T3": "3r Trimestre",
    "T4": "4t Trimestre",
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

            # 1. Deducció del Mes o Trimestre (ex: 2026_10_catgas... -> 10, 2026_T3_veolia... -> T3)
            fn_parts = pdf_path.stem.split("_")
            mes_code = "01"
            if len(fn_parts) >= 2 and fn_parts[1].isdigit():
                mes_code = fn_parts[1].zfill(2)
            elif len(fn_parts) >= 2 and fn_parts[1].upper() in ("T1", "T2", "T3", "T4", "Q1", "Q2", "Q3", "Q4"):
                mes_code = fn_parts[1].upper().replace("Q", "T")
            elif len(fn_parts) >= 2 and "-" in fn_parts[1]:
                # Si és bimensual/trimestral (ex: 09-10), agafem el primer mes del rang
                sub_p = fn_parts[1].split("-")
                if sub_p[0].isdigit():
                    mes_code = sub_p[0].zfill(2)
            else:
                mes_code = datetime.fromtimestamp(stat.st_mtime).strftime("%m")

            # Mapeig especial per a factures d'aigua trimestrals (Veolia / Agbar)
            if proveidor.lower() == "veolia":
                if mes_code in ("01", "02", "03", "T1"):
                    mes_label = "1r Trim (T1)"
                elif mes_code in ("04", "05", "06", "T2"):
                    mes_label = "2n Trim (T2)"
                elif mes_code in ("07", "08", "09", "10", "T3"):
                    mes_label = "3r Trim (T3)"
                elif mes_code in ("11", "12", "T4"):
                    mes_label = "4t Trim (T4)"
                else:
                    mes_label = f"{MESOS_CATALANS.get(mes_code, mes_code)} ({mes_code})"
            else:
                mes_label = f"{MESOS_CATALANS.get(mes_code, mes_code)} ({mes_code})"

            # Deducció de data ISO (YYYY-MM-DD) per al filtratge per rang a la web
            clean_year = (
                any_str
                if any_str.isdigit() and len(any_str) == 4
                else datetime.fromtimestamp(stat.st_mtime).strftime("%Y")
            )
            date_month = "01"
            if mes_code.isdigit():
                date_month = mes_code.zfill(2)
            elif mes_code == "T1":
                date_month = "03"
            elif mes_code == "T2":
                date_month = "06"
            elif mes_code == "T3":
                date_month = "09"
            elif mes_code == "T4":
                date_month = "12"
            date_iso = f"{clean_year}-{date_month}-01"

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
                        conn = CONNECTORS_MAP.get(proveidor.lower())
                        metriques = conn.extract_metrics_from_pdf(pdf_data) if conn else None
                        if metriques and metriques.import_total is not None:
                            import_val = metriques.import_total
                        else:
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
                    "date_iso": date_iso,
                    "import_val": import_val,
                    "import_str": import_str,
                    "size_kb": round(stat.st_size / 1024, 1),
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                    "rel_path": str(rel_path).replace("\\", "/"),
                }
            )
        except OSError:
            continue

    # Ordenar per data real de la factura descendent (i per data de modificació/nom en cas d'empat)
    invoices.sort(key=lambda x: (x["date_iso"], x["modified"], x["filename"]), reverse=True)
    return invoices


def reparse_and_fix_nas_invoices() -> tuple[int, int]:
    """Re-analitza tots els PDFs del NAS, reanomena/mou els que tenien data errònia (ex: 2022) i actualitza InfluxDB."""
    root = Path(settings.NAS_MOUNT_PATH)
    if not root.exists():
        return 0, 0

    _pdf_amount_cache.clear()
    processed = 0
    renamed = 0

    # Netejar prèviament totes les mètriques d'InfluxDB per eliminar qualsevol punt fantasma/residu antic
    for prov in ["bonpreu", "catgas", "veolia", "digi"]:
        influx_writer.delete_factura_metrics(prov)
    logger.info("🗑️ Netejats tots els punts anteriors a InfluxDB abans de la re-ingesta.")

    all_pdfs = list(root.glob("**/*.pdf"))
    total = len(all_pdfs)

    for idx, pdf_path in enumerate(all_pdfs, start=1):
        try:
            rel_path = pdf_path.relative_to(root)
            parts = rel_path.parts
            proveidor = parts[1].lower() if len(parts) > 1 else ""
            conn = CONNECTORS_MAP.get(proveidor)
            if not conn:
                continue

            if main_loop and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    ws_manager.broadcast_progress(
                        {
                            "connector": proveidor.upper(),
                            "step": f"🔄 Re-analitzant PDF {pdf_path.name}...",
                            "current": idx,
                            "total": total,
                            "percent": int((idx / total) * 100),
                            "status": "running",
                        }
                    ),
                    main_loop,
                )

            pdf_bytes = pdf_path.read_bytes()
            metriques = conn.extract_metrics_from_pdf(pdf_bytes)
            if not metriques:
                continue

            # Verificar si el fitxer tenia un any o mes erroni al nom (ex: 2022_10_digi_... quan és del 2026)
            any_correcte = str(metriques.data_emissio.year)
            mes_correcte = f"{metriques.data_emissio.month:02d}"
            clean_num = re.sub(r"[^A-Za-z0-9_-]", "", metriques.numero_factura)
            expected_filename = f"{any_correcte}_{mes_correcte}_{conn.name}_{clean_num}.pdf"
            expected_dir = storage.get_destination_dir(conn.tipus.value, conn.name, any_correcte)
            expected_path = expected_dir / expected_filename

            if pdf_path.resolve() != expected_path.resolve():
                expected_path.write_bytes(pdf_bytes)
                pdf_path.unlink(missing_ok=True)
                renamed += 1
                logger.info(f"🔧 Corregida ubicació/data: {pdf_path.name} -> {expected_path.name}")

            # Escriure la nova mètrica neta a InfluxDB
            influx_writer.write_factura_metrics(metriques)
            processed += 1
        except (OSError, RuntimeError, ValueError) as err:
            logger.warning(f"Error re-analitzant {pdf_path.name}: {err}")

    return processed, renamed


@app.post("/api/reparse")
async def trigger_reparse(background_tasks: BackgroundTasks):
    """Re-analitza tots els PDFs del NAS per corregir dates (ex: 2022) i imports a InfluxDB."""
    if execution_state["is_running"]:
        return HTMLResponse(
            '<div class="p-4 mb-4 text-sm text-yellow-300 rounded-xl bg-yellow-950/60 border border-yellow-700/50">'
            "⚠️ Ja hi ha una tasca en curs. Espera que finalitzi.</div>"
        )

    def _task():
        execution_state["is_running"] = True
        execution_state["active_connector"] = "REPARACIÓ NAS & INFLUXDB"
        try:
            proc, ren = reparse_and_fix_nas_invoices()
            msg = (
                f"✅ Reparació completada: {proc} factures re-analitzades i actualitzades a InfluxDB "
                f"({ren} fitxers moguts a la seva data correcta)."
            )
            logger.info(msg)
            if main_loop and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    ws_manager.broadcast_status({"status": "ok", "message": msg, "new_count": proc}),
                    main_loop,
                )
        finally:
            execution_state["is_running"] = False
            execution_state["active_connector"] = None

    background_tasks.add_task(_task)
    return HTMLResponse(
        '<div class="p-4 mb-4 text-sm text-blue-300 rounded-xl bg-blue-950/60 border border-blue-700/50">'
        "🔄 Re-analitzant tots els PDFs del NAS per corregir imports i dates errònies... La pàgina es recarregarà en acabar.</div>"
    )


def run_sync_task(
    connector_name: str | None = None,
    mode: str = "normal",
    limit: int | None = None,
    source: str = "auto",
) -> None:
    """Tasca d'execució d'ETL amb emissió de progrés (X/N) i diagnòstic en temps real."""
    source_label = {"email": "📧 Correu IMAP", "portal": "🌐 Portal Web", "auto": "⚡ Híbrid"}.get(source, source)
    execution_state["is_running"] = True
    execution_state["active_connector"] = f"{(connector_name or 'Tots').upper()} ({source_label})"
    execution_state["last_status"] = "Sincronitzant..."

    def _emit_progress(data: dict[str, Any]) -> None:
        if main_loop and main_loop.is_running():
            asyncio.run_coroutine_threadsafe(ws_manager.broadcast_progress(data), main_loop)

    connectors_map = get_all_connectors_map()
    if connector_name and connector_name.lower() in connectors_map:
        to_run = [connectors_map[connector_name.lower()]]
    else:
        to_run = [c for c in connectors_map.values() if c.is_enabled()]

    results: list[FacturaResult] = []
    error_messages: list[str] = []

    for conn in to_run:
        conn.progress_callback = _emit_progress
        try:
            _emit_progress(
                {
                    "connector": conn.name.upper(),
                    "step": f"Iniciant extracció ({source_label})...",
                    "current": 0,
                    "total": 0,
                    "percent": 0,
                    "status": "running",
                }
            )
            res = conn.run(imap_client, mode=mode, limit=limit, source=source)
            results.extend(res)
            for r in res:
                if r.errors:
                    error_messages.extend(r.errors)
        except (OSError, RuntimeError, ValueError) as err:
            err_str = f"{conn.name.upper()}: {err}"
            logger.error(f"❌ Error executant {err_str}")
            error_messages.append(err_str)
        finally:
            conn.progress_callback = None

    noves = sum(1 for r in results if r.nou_descarregat)
    existents = len(results) - noves

    execution_state["is_running"] = False
    execution_state["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    execution_state["last_status"] = "Error" if error_messages else "Completat"
    execution_state["last_results"] = [r.model_dump() for r in results]
    execution_state["active_connector"] = None

    if error_messages:
        summary_msg = f"❌ Finalitzat amb errors ({source_label}): {'; '.join(error_messages)}"
        status_code = "error"
    elif len(results) == 0:
        summary_msg = (
            f"⚠️ Procés finalitzat ({source_label}): 0 factures obtingudes. "
            "Revisa la consola de logs inferior per veure el detall."
        )
        status_code = "warning"
    else:
        summary_msg = (
            f"✅ Completat ({source_label}): {len(results)} factures processades "
            f"({noves} noves desades al NAS, {existents} ja existents)."
        )
        status_code = "ok"

    logger.info(summary_msg)
    if main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(
            ws_manager.broadcast_status({"status": status_code, "message": summary_msg, "new_count": noves}),
            main_loop,
        )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Pàgina principal amb dashboard operatiu i controls."""
    invoices = await asyncio.to_thread(list_nas_invoices)

    # Calcular darrera factura i recompte total de factures per proveïdor
    count_by_provider: dict[str, int] = {}
    last_by_provider: dict[str, str] = {}
    last_mod_by_provider: dict[str, str] = {}
    for inv in invoices:
        prov = inv["proveidor"].lower()
        count_by_provider[prov] = count_by_provider.get(prov, 0) + 1
        if prov not in last_by_provider:
            last_by_provider[prov] = inv["filename"]
            last_mod_by_provider[prov] = inv["modified"]

    veolia_health = health_manager.get_provider_health("veolia", last_mod_by_provider.get("veolia"))
    catgas_health = health_manager.get_provider_health("catgas", last_mod_by_provider.get("catgas"))
    bonpreu_health = health_manager.get_provider_health("bonpreu", last_mod_by_provider.get("bonpreu"))
    digi_health = health_manager.get_provider_health("digi", last_mod_by_provider.get("digi"))
    global_health = health_manager.get_global_summary()

    status_cards = [
        {
            "id": "veolia",
            "name": "Veolia",
            "tipus": "Aigua",
            "enabled": settings.VEOLIA_ENABLED,
            "icon": "droplet",
            "color": "blue",
            "strategy": "Trigger Email + Portal Agbar",
            "last_invoice": last_by_provider.get("veolia", "Cap factura trobada"),
            "invoice_count": count_by_provider.get("veolia", 0),
            "last_success_at": veolia_health["last_success_at"],
            "incident_count": veolia_health["incident_count"],
            "has_errors": veolia_health["has_errors"],
        },
        {
            "id": "catgas",
            "name": "Catgas",
            "tipus": "Gas Natural",
            "enabled": settings.CATGAS_ENABLED,
            "icon": "flame",
            "color": "amber",
            "strategy": "Trigger Email + Oficina Virtual",
            "last_invoice": last_by_provider.get("catgas", "Cap factura trobada"),
            "invoice_count": count_by_provider.get("catgas", 0),
            "last_success_at": catgas_health["last_success_at"],
            "incident_count": catgas_health["incident_count"],
            "has_errors": catgas_health["has_errors"],
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
            "invoice_count": count_by_provider.get("bonpreu", 0),
            "last_success_at": bonpreu_health["last_success_at"],
            "incident_count": bonpreu_health["incident_count"],
            "has_errors": bonpreu_health["has_errors"],
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
            "invoice_count": count_by_provider.get("digi", 0),
            "last_success_at": digi_health["last_success_at"],
            "incident_count": digi_health["incident_count"],
            "has_errors": digi_health["has_errors"],
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
            "global_health": global_health,
        },
    )


@app.post("/api/sync")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    connector: str = Form(default=""),
    mode: str = Form(default="normal"),
    source: str = Form(default="auto"),
):
    """Dispara la sincronització en segon pla podent triar source ('email', 'portal' o 'auto')."""
    if execution_state["is_running"]:
        return HTMLResponse(
            '<div class="p-4 mb-4 text-sm text-yellow-300 rounded-xl bg-yellow-950/60 border border-yellow-700/50">'
            "⚠️ Ja hi ha una sincronització en curs. Mira el panell de progrés superior.</div>"
        )

    target = connector.strip() if connector else None
    sync_mode = mode.strip() if mode else "normal"
    sync_source = source.strip() if source else "auto"
    limit = 1 if sync_mode == "latest" else None

    background_tasks.add_task(run_sync_task, target, mode=sync_mode, limit=limit, source=sync_source)

    target_escaped = html.escape(target.upper()) if target else "TOTS ELS PROVEÏDORS"
    source_desc = {"email": "📧 Correu IMAP", "portal": "🌐 Portal Web", "auto": "⚡ Automàtic"}.get(
        sync_source, sync_source
    )
    return HTMLResponse(
        f'<div class="p-4 mb-4 rounded-xl bg-slate-900 border border-blue-700/60 shadow-lg space-y-2">'
        f'  <div class="flex items-center justify-between text-xs font-semibold text-blue-300">'
        f'    <span id="live-progress-title">⏳ {target_escaped} ({source_desc}) — Iniciant...</span>'
        f'    <span id="live-progress-counter" class="font-mono text-emerald-400">Connectant...</span>'
        f"  </div>"
        f'  <div class="w-full bg-slate-800 rounded-full h-2 overflow-hidden">'
        f'    <div id="live-progress-bar" class="bg-emerald-500 h-2 rounded-full transition-all duration-300 animate-pulse" style="width: 10%"></div>'
        f"  </div>"
        f'  <p id="live-progress-step" class="text-xs text-slate-300 font-mono truncate">Esperant resposta del connector...</p>'
        f"</div>"
    )


@app.websocket("/ws/logs")
async def websocket_logs_endpoint(websocket: WebSocket):
    """Canal WebSocket amb heartbeat automàtic per transmetre logs, progrés X/N i estat."""
    await ws_manager.connect(websocket)
    try:
        # 1. Enviar la versió actual del servidor per al version-check del frontend
        await websocket.send_json({"type": "version", "version": f"v{settings.APP_VERSION}"})
        # 2. Enviar els darrers logs acumulats per a la càrrega inicial
        for line in list(log_buffer)[-60:]:
            await websocket.send_json({"type": "log", "line": line})
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=20.0)
            except TimeoutError:
                await websocket.send_json({"type": "ping"})
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


@app.get("/api/incidents", response_class=HTMLResponse)
async def get_incidents_modal(proveidor: str = "all"):
    """Retorna el contingut HTML del pop-up d'incidències (global o filtrat per proveïdor)."""
    prov_clean = proveidor.strip().lower()
    summary = health_manager.get_global_summary()
    all_inc = summary["incidents"]

    if prov_clean and prov_clean != "all":
        incidents = [i for i in all_inc if i.get("proveidor") == prov_clean]
        title_scope = f"Incidències — {html.escape(prov_clean.upper())}"
    else:
        incidents = all_inc
        title_scope = "Centre Global d'Incidències i Avisos"

    items_html = []
    for inc in incidents:
        sev = inc.get("severity", "error")
        is_err = sev == "error"
        border_cls = "border-red-500/30 bg-red-950/20" if is_err else "border-amber-500/30 bg-amber-950/20"
        badge_cls = (
            "bg-red-500/20 text-red-300 border-red-500/40"
            if is_err
            else "bg-amber-500/20 text-amber-300 border-amber-500/40"
        )
        prov_name = html.escape(str(inc.get("proveidor", "")).upper())
        title_txt = html.escape(str(inc.get("title", "Incidència")))
        msg_txt = html.escape(str(inc.get("message", "")))
        hint_txt = html.escape(str(inc.get("hint", "")))
        ts_txt = html.escape(str(inc.get("timestamp", "")))
        try:
            count_val = int(inc.get("count", 1))
        except (ValueError, TypeError):
            count_val = 1
        count_badge = (
            f'<span class="px-1.5 py-0.5 text-[10px] rounded bg-slate-800 text-slate-300 font-mono">x{count_val}</span>'
            if count_val > 1
            else ""
        )

        items_html.append(
            f'<div class="p-4 rounded-xl border {border_cls} space-y-2">'
            f'  <div class="flex flex-wrap items-center justify-between gap-2">'
            f'    <div class="flex items-center gap-2">'
            f'      <span class="px-2 py-0.5 rounded text-[11px] font-bold border {badge_cls}">{prov_name}</span>'
            f'      <span class="text-sm font-semibold text-white">{title_txt}</span>'
            f"      {count_badge}"
            f"    </div>"
            f'    <span class="text-xs font-mono text-slate-400">{ts_txt}</span>'
            f"  </div>"
            f'  <p class="text-xs font-mono text-slate-200 bg-slate-950/70 p-2.5 rounded-lg border border-slate-800 break-words">{msg_txt}</p>'
            f'  <p class="text-[11px] text-slate-400 flex items-center gap-1.5">💡 <span>{hint_txt}</span></p>'
            f"</div>"
        )

    if not items_html:
        body_html = (
            '<div class="py-12 text-center space-y-3">'
            '  <div class="w-12 h-12 mx-auto rounded-full bg-emerald-500/10 border border-emerald-500/30 flex items-center justify-center text-emerald-400 text-xl">✓</div>'
            '  <h4 class="text-sm font-bold text-white">Tot funciona correctament!</h4>'
            '  <p class="text-xs text-slate-400">No hi ha cap incidència ni error pendent de revisió.</p>'
            "</div>"
        )
    else:
        body_html = f'<div class="space-y-3 max-h-96 overflow-y-auto pr-1">{"".join(items_html)}</div>'

    clear_btn = ""
    if incidents:
        clear_btn = (
            f'<button type="button" hx-post="/api/incidents/clear" hx-vals=\'{{"proveidor": "{html.escape(prov_clean)}"}}\' '
            f'hx-target="#incidents-modal-content" '
            f'class="px-3 py-1.5 bg-slate-800 hover:bg-red-950/60 text-slate-200 hover:text-red-300 text-xs font-medium rounded-lg border border-slate-700 hover:border-red-700/50 transition cursor-pointer">'
            f"🧹 Netejar incidències</button>"
        )

    return HTMLResponse(
        f'<div class="p-6 space-y-5">'
        f'  <div class="flex items-center justify-between border-b border-slate-800 pb-4">'
        f"    <div>"
        f'      <h3 class="text-base font-bold text-white flex items-center gap-2">🔔 {title_scope}</h3>'
        f'      <p class="text-xs text-slate-400">Resum d\'errors de 2FA, canvis de parser o timeouts recents ({len(incidents)} registres)</p>'
        f"    </div>"
        f'    <div class="flex items-center gap-2">'
        f"      {clear_btn}"
        f'      <button type="button" onclick="closeIncidentsModal()" class="text-slate-400 hover:text-white p-1.5 rounded-lg hover:bg-slate-800 cursor-pointer">✕</button>'
        f"    </div>"
        f"  </div>"
        f"  {body_html}"
        f"</div>"
    )


@app.post("/api/incidents/clear", response_class=HTMLResponse)
async def clear_incidents_endpoint(proveidor: str = Form(default="all")):
    """Neteja les incidències i refresca el modal i la pàgina."""
    prov_clean = proveidor.strip().lower()
    health_manager.clear_incidents(None if prov_clean == "all" else prov_clean)
    resp = await get_incidents_modal(prov_clean)
    resp.headers["HX-Trigger"] = "incidentsCleared"
    return resp


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

    # 1. Eliminar del NAS i de la memòria cau
    deleted_nas = storage.delete_pdf(rel_path)
    _pdf_amount_cache.pop(rel_path, None)
    _pdf_amount_cache.pop(rel_path.replace("/", "\\"), None)

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
