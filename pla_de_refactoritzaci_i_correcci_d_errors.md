La teva idea del **Centre d'Incidències i Salut dels Connectors** (amb l'indicador global + l'indicador a cada targeta + la data de l'última sincronització exitosa) és exactament el que li falta a l'aplicació per passar de ser un "script amb web" a una eina d'observabilitat domèstica de primer nivell.

A més, revisant `concat_8.txt` he detectat **dos detalls molt importants** que podem resoldre amb aquesta mateixa arquitectura:

1. **Persistència entre el CronJob i la Web UI:** Si l'estat d'èxit i les incidències es guarden només en una variable de memòria RAM de FastAPI (`execution_state`), quan el **CronJob nocturn de Kubernetes** s'executi (o quan es reiniciï el Pod), la Web UI no s'assabentarà que ahir a la nit va funcionar o va fallar pel 2FA! Guardant l'estat i les incidències en un fitxer lleuger `/data/factures/.status/health_state.json` al mateix volum NAS Samba, tant el CronJob (`main.py`) com la Web (`app.py`) compartiran l'estat real i persistent!


2. **Bug d'ordre d'arguments a `veolia.py`:** A `src/connectors/veolia.py` (línia 108), crides `self.fetch_invoices_from_portal(limit=fetch_limit, imap_client=imap_client)`, que funciona perquè uses *keyword arguments*, però les signatures de `bonpreu.py`, `veolia.py`, `catgas.py` i `digi.py` tenen l'ordre dels paràmetres diferent.



---

### Com hem dissenyat i millorat la teva idea d'Avisos i Salut?

1. **Classificador Intel·ligent d'Incidències (Diagnòstic Humà):**
Quan un connector registra un error o avís (`record_failure` o `report_progress` amb `warning`/`error`, o quan un PDF no es pot parsejar), el sistema no només guarda el log tècnic, sinó que el classifica automàticament amb una etiqueta clara i una recomanació:
* 🔐 **`2FA_REQUIRED`**: *"Bonpreu / Veolia / Digi demana codi de verificació 2FA (el codi no ha arribat a temps per IMAP o requereix SMS)."*
* 🔑 **`AUTH_ERROR`**: *"Credencials rebutjades o no configurades al Secret."*
* 📄 **`PARSER_CHANGE`**: *"S'ha descarregat el PDF però no s'ha pogut extreure l'import o la data (possible canvi de format de factura)."*
* ⏱️ **`TIMEOUT_PORTAL`**: *"El portal web de la companyia ha trigat massa a respondre."*
* 🗄️ **`INFLUX_ERROR`**: *"No s'han pogut desar les mètriques a InfluxDB."*


2. **Doble Nivell d'Indicadors Visuals (Global + Per Targeta):**
* **A la Barra Superior (Header Global):** Un botó d'estat general:
* Si hi ha `0` incidències obertes: **`✅ Sistema OK`** (verd esmeralda).
* Si hi ha incidències: **`⚠️ X Incidències`** (triangle ambre/vermell amb pols animat). En fer-hi clic, obre el **Pop-up Central d'Incidències** amb el detall de tots els proveïdors.


* **A cada Targeta de Proveïdor (Veolia, Catgas, Bonpreu, Digi):**
* A dalt a la dreta (al costat de `"X factures"`): un **botó-insígnia clicable**. Si aquell proveïdor té errors (ex: Bonpreu ha fallat pel 2FA), surt un triangle **`⚠️ 1`** en vermell/ambre; si tot ha anat bé, surt **`✅ OK`**. Si hi fas clic, s'obre el Pop-up filtrat directament per aquell subministrament!
* A sota d'**"Última factura"**: una nova línia **"Últim èxit:"** que mostra la data i hora exacta de l'última vegada que aquell connector va funcionar sense errors (ex: `2026-10-26 06:01 (Portal Web)`), i si no s'ha executat mai amb èxit, es calcula automàticament a partir de la data de l'última factura desada al NAS!




3. **Acció per "Netejar / Marcar com a resoltes":**
* Tant si el connector torna a funcionar amb èxit (`record_success()`) com si tu cliques el botó **"🧹 Netejar incidències"** dins del pop-up, el comptador torna a `0` i l'indicador torna al check verd (`✅`).



---

### Document complet d'instruccions per a l'Agent (`REFACTORING_PHASE8.md`)

Copia aquest bloc Markdown i passa-li directament al teu agent perquè implementi tot el sistema d'incidències, l'última data d'èxit i els pop-ups interactius:

```markdown
# Pla de Millora (Fase 8) — Sistema de Salut, Notificacions d'Incidències (Global + Per Proveïdor) i Data d'"Últim Èxit"

Aquest document implementa el nou **Sistema d'Observabilitat i Avisos d'Incidències**:
1. **Gestor Persistent de Salut i Incidències (`src/health.py`)**: Desa a `/data/factures/.status/health_state.json` l'última data d'execució exitosa (`last_success_at`, `last_success_source`) i la llista d'incidències obertes per proveïdor, classificant automàticament si l'error és per **🔐 2FA (`2FA_REQUIRED`)**, **📄 Canvi de format PDF (`PARSER_ERROR`)**, **🔑 Credencials (`AUTH_ERROR`)** o **⏱️ Timeout (`TIMEOUT`)**.
2. **Integració als Connectors (`base.py`) i al CLI/CronJob (`main.py`)**: Quan un CronJob nocturn o una sincronització manual funciona, actualitza automàticament la data d'`Últim èxit` i neteja els errors d'aquell proveïdor. Si falla (o si un PDF no es pot parsejar), registra la incidència detallada.
3. **UI — Botó Global d'Avisos al Header, Badge d'Estat i Data d'"Últim Èxit" a cada Targeta, i Pop-up Modal d'Incidències**:
   - Check verd (`✅ OK`) quan no hi ha errors, o Triangle d'alerta (`⚠️ N`) amb el número d'incidències tant al header general com a cada targeta.
   - Nou camp **"Últim èxit:"** sota d'"Última factura" a cada targeta de subministrament.
   - Modal interactiu (`/api/incidents`) que mostra totes les incidències resumides (o filtrades per proveïdor) i permet netejar-les amb un clic.

---

## 1. Crear el mòdul `src/health.py` (Gestor Persistent d'Estat i Incidències)

Crear un fitxer nou `src/health.py` amb aquest contingut complet:

```python
# pyright: reportMissingImports=false
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import settings

logger = logging.getLogger("health_manager")

PROVIDERS = ("veolia", "catgas", "bonpreu", "digi")


def classify_incident(message: str) -> tuple[str, str, str]:
    """Classifica un missatge d'error en (codi, etiqueta_curta, suggeriment)."""
    msg_low = message.lower()
    if "2fa" in msg_low or "totp" in msg_low or "codi de verificac" in msg_low or "sms" in msg_low:
        return (
            "2FA_REQUIRED",
            "🔐 Verificació 2FA requerida",
            "El portal demana un codi 2FA que no s'ha pogut llegir automàticament per correu o requereix SMS.",
        )
    if "login" in msg_low or "credencial" in msg_low or "password" in msg_low or " secret" in msg_low:
        return (
            "AUTH_ERROR",
            "🔑 Error d'Autenticació / Credencials",
            "Revisa l'usuari i contrasenya al Secret o comprova si el portal demana un Captcha.",
        )
    if "parse" in msg_low or "mètriques" in msg_low or "import" in msg_low or "format" in msg_low:
        return (
            "PARSER_ERROR",
            "📄 Error de Format PDF (Parser)",
            "S'ha obtingut el PDF però no s'han pogut extreure totes les dades. Pot haver canviat el disseny de la factura.",
        )
    if "timeout" in msg_low or "temps d'espera" in msg_low:
        return (
            "TIMEOUT",
            "⏱️ Temps d'espera esgotat (Timeout)",
            "El portal web de la companyia ha trigat massa a respondre o un botó de descàrrega no ha reaccionat.",
        )
    if "influx" in msg_low:
        return (
            "INFLUX_ERROR",
            "🗄️ Error d'escriptura a InfluxDB",
            "La factura s'ha desat al NAS però ha fallat l'enviament de mètriques a InfluxDB.",
        )
    if "circuit breaker" in msg_low or "protecció de seguretat" in msg_low:
        return (
            "CIRCUIT_BREAKER",
            "⛔ Bloqueig Preventiu Actiu",
            "S'han aturat temporalment els intents per evitar bloquejar el compte del proveïdor.",
        )
    return (
        "GENERAL_ERROR",
        "⚠️ Incidència d'Extracció",
        "Revisa el detall tècnic de l'error per a més informació.",
    )


class HealthManager:
    """Gestiona l'estat de salut i l'històric d'incidències de cada connector persistit al NAS."""

    def __init__(self) -> None:
        self.status_dir = Path(settings.NAS_MOUNT_PATH) / ".status"
        self.status_file = self.status_dir / "health_state.json"
        self._state: dict[str, Any] = self._load()

    def _default_state(self) -> dict[str, Any]:
        return {
            prov: {
                "last_success_at": None,
                "last_success_source": None,
                "last_attempt_at": None,
                "last_status": "unknown",
                "incidents": [],
            }
            for prov in PROVIDERS
        }

    def _load(self) -> dict[str, Any]:
        state = self._default_state()
        try:
            if self.status_file.exists():
                raw = json.loads(self.status_file.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for prov in PROVIDERS:
                        if prov in raw and isinstance(raw[prov], dict):
                            state[prov].update(raw[prov])
        except (OSError, ValueError, TypeError) as err:
            logger.debug(f"No s'ha pogut llegir health_state.json: {err}")
        return state

    def _save(self) -> None:
        try:
            self.status_dir.mkdir(parents=True, exist_ok=True)
            self.status_file.write_text(json.dumps(self._state, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError as err:
            logger.debug(f"No s'ha pogut desar health_state.json: {err}")

    def reload(self) -> None:
        """Recarrega l'estat des del disc (útil quan un CronJob extern l'ha modificat)."""
        self._state = self._load()

    def record_success(self, proveidor: str, source: str = "auto", clear_incidents: bool = True) -> None:
        prov = proveidor.lower()
        if prov not in self._state:
            return
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        self._state[prov]["last_success_at"] = now_str
        self._state[prov]["last_success_source"] = source
        self._state[prov]["last_attempt_at"] = now_str
        self._state[prov]["last_status"] = "ok"
        if clear_incidents:
            self._state[prov]["incidents"] = []
        self._save()

    def record_incident(
        self,
        proveidor: str,
        message: str,
        severity: str = "error",
        source: str = "auto",
    ) -> None:
        prov = proveidor.lower()
        if prov not in self._state:
            return
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        code, title, hint = classify_incident(message)

        incidents: list[dict[str, Any]] = self._state[prov].get("incidents", [])
        # Evitar duplicar exactament el mateix error seguit
        if incidents and incidents[0].get("message") == message:
            incidents[0]["timestamp"] = now_str
            incidents[0]["count"] = incidents[0].get("count", 1) + 1
        else:
            incidents.insert(
                0,
                {
                    "id": f"{prov}_{int(datetime.now().timestamp())}_{len(incidents)}",
                    "proveidor": prov,
                    "timestamp": now_str,
                    "severity": severity,  # 'error' o 'warning'
                    "code": code,
                    "title": title,
                    "message": message,
                    "hint": hint,
                    "source": source,
                    "count": 1,
                },
            )

        # Mantenir com a màxim les últimes 15 incidències per proveïdor
        self._state[prov]["incidents"] = incidents[:15]
        self._state[prov]["last_attempt_at"] = now_str
        self._state[prov]["last_status"] = severity
        self._save()

    def clear_incidents(self, proveidor: str | None = None) -> None:
        if proveidor and proveidor.lower() in self._state:
            self._state[proveidor.lower()]["incidents"] = []
            if self._state[proveidor.lower()]["last_status"] in ("error", "warning"):
                self._state[proveidor.lower()]["last_status"] = "ok"
        else:
            for prov in PROVIDERS:
                self._state[prov]["incidents"] = []
                if self._state[prov]["last_status"] in ("error", "warning"):
                    self._state[prov]["last_status"] = "ok"
        self._save()

    def get_provider_health(self, proveidor: str, fallback_last_modified: str | None = None) -> dict[str, Any]:
        self.reload()
        prov = proveidor.lower()
        data = self._state.get(prov, {})
        incidents = data.get("incidents", [])
        last_ok = data.get("last_success_at") or fallback_last_modified or " Cap registre encara"
        return {
            "last_success_at": last_ok,
            "last_success_source": data.get("last_success_source") or "",
            "last_attempt_at": data.get("last_attempt_at"),
            "incident_count": len(incidents),
            "has_errors": any(i.get("severity") == "error" for i in incidents),
            "incidents": incidents,
        }

    def get_global_summary(self) -> dict[str, Any]:
        self.reload()
        all_incidents: list[dict[str, Any]] = []
        for prov in PROVIDERS:
            all_incidents.extend(self._state.get(prov, {}).get("incidents", []))
        all_incidents.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return {
            "total_incidents": len(all_incidents),
            "has_errors": any(i.get("severity") == "error" for i in all_incidents),
            "incidents": all_incidents,
        }


health_manager = HealthManager()
```

---

## 2. Integrar `health_manager` a `src/connectors/base.py`

### Canvi a aplicar a `src/connectors/base.py`
1. Importar `health_manager` a dalt de `src/connectors/base.py`:
```python
from src.health import health_manager
```
2. Actualitzar `report_progress`, `record_failure`, `record_success` i `save_and_ingest` dins `BaseConnector` perquè registrin automàticament qualsevol error, avís de 2FA o èxit:

```python
    def report_progress(
        self,
        step: str,
        current: int = 0,
        total: int = 0,
        status: str = "running",
    ) -> None:
        """Emet l'estat i el progrés exacte (ex: 3/40) cap a la UI per WebSocket i al log."""
        pct = int((current / total) * 100) if total > 0 else 0
        counter_str = f"[{current}/{total}] " if total > 0 else ""
        self.logger.info(f"[{self.name.upper()}] {counter_str}{step}")

        if status in ("error", "warning"):
            health_manager.record_incident(self.name, step, severity=status)

        if self.progress_callback:
            try:
                self.progress_callback(
                    {
                        "connector": self.name.upper(),
                        "step": step,
                        "current": current,
                        "total": total,
                        "percent": pct,
                        "status": status,
                    }
                )
            except (TypeError, ValueError, RuntimeError, OSError) as e:
                self.logger.debug(f"Error progress callback: {e}")

    def record_failure(self, reason: str) -> None:
        """Registra un error consecutiu, el desa al gestor d'incidències i activa el circuit breaker si cal."""
        self.consecutive_failures += 1
        health_manager.record_incident(self.name, reason, severity="error")
        self.logger.warning(
            f"⚠️ Intent fallit ({self.consecutive_failures}/{settings.MAX_SCRAPER_RETRIES}) per a {self.name.upper()}: {reason}"
        )
        if self.consecutive_failures >= settings.MAX_SCRAPER_RETRIES:
            self.cooldown_until = datetime.now() + timedelta(minutes=settings.SCRAPER_COOLDOWN_MINUTES)
            cb_msg = (
                f"⛔ CIRCUIT BREAKER ACTIVAT per a {self.name.upper()}: S'han assolit {settings.MAX_SCRAPER_RETRIES} intents fallits. "
                f"Pausa preventiva de {settings.SCRAPER_COOLDOWN_MINUTES} minuts activada."
            )
            self.logger.error(cb_msg)
            health_manager.record_incident(self.name, cb_msg, severity="error")

    def record_success(self, source: str = "portal") -> None:
        """Reinicia el comptador d'errors i actualitza l'última data d'èxit en cas d'èxit."""
        self.consecutive_failures = 0
        self.cooldown_until = None
        health_manager.record_success(self.name, source=source)
```

3. Dins de `save_and_ingest` a `src/connectors/base.py`, just després d'enviar a InfluxDB, registrar l'èxit (o l'error d'InfluxDB):
```python
        # Enviar mètriques a InfluxDB
        influx_ok = influx_writer.write_factura_metrics(metriques)
        errors: list[str] = []
        if not influx_ok and settings.INFLUXDB_ENABLED and not settings.DRY_RUN:
            err_msg = f"Error en enviar mètriques de la factura {metriques.numero_factura} a InfluxDB"
            errors.append(err_msg)
            health_manager.record_incident(self.name, err_msg, severity="warning", source=origen)
        else:
            health_manager.record_success(self.name, source=origen, clear_incidents=False)
```

4. Dins dels 4 connectors (`bonpreu.py`, `catgas.py`, `digi.py`, `veolia.py`), quan `extract_metrics_from_pdf(pdf_bytes)` retorni `None` sobre un PDF descarregat del portal o adjunt de correu, afegir un avís explícit de fallada de parser abans del fallback:
```python
self.report_progress(
    f"⚠️ No s'han pogut extreure totes les mètriques del PDF {filename} (possible canvi de format de factura).",
    status="warning",
)
```

---

## 3. Afegir Endpoints d'Incidències i Estat de Salut a `src/web/app.py`

### Canvi 1: Importar `health_manager` a `src/web/app.py`
```python
from src.health import health_manager
```

### Canvi 2: Actualitzar `run_sync_task` a `src/web/app.py`
Dins `run_sync_task`, quan un connector s'executa sense llançar cap excepció i sense errors a `res`, actualitzar `health_manager.record_success(conn.name, source=source)` si ha obtingut factures o si ha completat la comprovació netament sense nous avisos.

### Canvi 3: Enriquir `status_cards` i el context d'`index()` a `src/web/app.py`
Dins de `index(request: Request)` a `src/web/app.py`, calcular la data de modificació de l'última factura de cada proveïdor com a fallback per si encara no hi ha cap registre a `health_state.json`, i passar `health` i `global_health` al template:

```python
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
```
I afegir a cada diccionari de `status_cards` (per a `veolia`, `catgas`, `bonpreu`, `digi`):
```python
            "last_success_at": veolia_health["last_success_at"],
            "incident_count": veolia_health["incident_count"],
            "has_errors": veolia_health["has_errors"],
```
(Aplicant la variable `*_health` corresponent a cadascun dels 4 proveïdors, i afegint `"global_health": global_health` al `context` de `templates.TemplateResponse`).

### Canvi 4: Afegir els endpoints `/api/incidents` (GET) i `/api/incidents/clear` (POST) a `src/web/app.py`
Afegir aquests dos endpoints a `src/web/app.py`:

```python
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
        count_val = int(inc.get("count", 1))
        count_badge = f'<span class="px-1.5 py-0.5 text-[10px] rounded bg-slate-800 text-slate-300 font-mono">x{count_val}</span>' if count_val > 1 else ""

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
            f'class="px-3 py-1.5 bg-slate-800 hover:bg-red-950/60 text-slate-200 hover:text-red-300 text-xs font-medium rounded-lg border border-slate-700 hover:border-red-700/50 transition">'
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
        f'      <button type="button" onclick="closeIncidentsModal()" class="text-slate-400 hover:text-white p-1.5 rounded-lg hover:bg-slate-800">✕</button>'
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
```

---

## 4. Actualitzar `src/web/templates/index.html` (Botó Global, Targetes amb "Últim èxit" i Pop-up d'Incidències)

### Canvi 1: Afegir el Botó Global d'Estat/Incidències al `<header>` d'`index.html` (just abans del botó `Sincronitzar Tot`)
```html
        <!-- Botó Global d'Incidències / Estat de Salut -->
        {% if global_health.total_incidents > 0 %}
        <button
          type="button"
          hx-get="/api/incidents?proveidor=all"
          hx-target="#incidents-modal-content"
          onclick="openIncidentsModal()"
          title="Hi ha incidències pendents de revisió. Clica per veure el detall."
          class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg {% if global_health.has_errors %}bg-red-950/80 border-red-500/50 text-red-300 hover:bg-red-900/80{% else %}bg-amber-950/80 border-amber-500/50 text-amber-300 hover:bg-amber-900/80{% endif %} border font-semibold transition cursor-pointer shadow-sm">
          <i data-lucide="alert-triangle" class="w-4 h-4 {% if global_health.has_errors %}text-red-400 animate-bounce{% else %}text-amber-400{% endif %}"></i>
          <span>{{ global_health.total_incidents }} Incidènci{% if global_health.total_incidents == 1 %}a{% else %}es{% endif %}</span>
        </button>
        {% else %}
        <button
          type="button"
          hx-get="/api/incidents?proveidor=all"
          hx-target="#incidents-modal-content"
          onclick="openIncidentsModal()"
          title="Tots els subministraments funcionen sense errors"
          class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-950/50 hover:bg-emerald-900/60 border border-emerald-700/40 text-emerald-300 font-medium transition cursor-pointer">
          <i data-lucide="check-circle-2" class="w-4 h-4 text-emerald-400"></i>
          <span>Sistema OK</span>
        </button>
        {% endif %}
```

### Canvi 2: Afegir el Botó d'Avisos Individual i la fila "Últim èxit:" a cada Card de Proveïdor a `index.html`
Dins del bucle `{% for card in cards %}` a `src/web/templates/index.html`:

1. A la part superior dreta de la card (on hi ha `{{ card.invoice_count }} factures`), afegir el botó-insígnia d'incidències d'aquell proveïdor:
```html
              <div class="flex items-center gap-1.5">
                {% if card.incident_count > 0 %}
                <button
                  type="button"
                  hx-get="/api/incidents?proveidor={{ card.id }}"
                  hx-target="#incidents-modal-content"
                  onclick="openIncidentsModal()"
                  title="Veure {{ card.incident_count }} incidències de {{ card.name }}"
                  class="flex items-center gap-1 text-[11px] font-bold px-2 py-0.5 rounded-full {% if card.has_errors %}bg-red-500/20 text-red-300 border border-red-500/40 hover:bg-red-500/30{% else %}bg-amber-500/20 text-amber-300 border border-amber-500/40 hover:bg-amber-500/30{% endif %} transition cursor-pointer">
                  <i data-lucide="alert-triangle" class="w-3 h-3"></i>
                  <span>{{ card.incident_count }}</span>
                </button>
                {% else %}
                <button
                  type="button"
                  hx-get="/api/incidents?proveidor={{ card.id }}"
                  hx-target="#incidents-modal-content"
                  onclick="openIncidentsModal()"
                  title="{{ card.name }} sense incidències"
                  class="flex items-center gap-1 text-[11px] font-medium px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 hover:bg-emerald-500/20 transition cursor-pointer">
                  <i data-lucide="check" class="w-3 h-3"></i>
                  <span>OK</span>
                </button>
                {% endif %}
                <span class="text-[11px] font-semibold px-2 py-0.5 rounded-full bg-slate-800 text-emerald-400 border border-slate-700/80 font-mono" title="Número de factures desades al sistema">
                  {{ card.invoice_count }} factures
                </span>
              </div>
```

2. Just a sota del bloc `Última factura:` dins de la mateixa card, afegir la nova fila **`Últim èxit:`**:
```html
              <div class="flex items-center justify-between text-slate-400 gap-2 pt-0.5">
                <span class="shrink-0">Últim èxit:</span>
                <span class="text-emerald-400/90 font-mono text-[11px] truncate text-right" title="Última vegada que l'extracció ha funcionat">
                  {{ card.last_success_at }}
                </span>
              </div>
```

### Canvi 3: Afegir el contenidor del Modal d'Incidències i les funcions JS a `src/web/templates/index.html`
1. Just a sota del `#history-modal`, afegir el modal `#incidents-modal`:
```html
  <!-- Modal per al Centre d'Incidències i Avisos -->
  <div id="incidents-modal" class="fixed inset-0 z-50 hidden bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
    <div class="bg-slate-900 border border-slate-800 rounded-2xl w-full max-w-3xl max-h-[85vh] flex flex-col shadow-2xl overflow-hidden">
      <div id="incidents-modal-content">
        <div class="p-8 text-center text-slate-400">Carregant incidències...</div>
      </div>
    </div>
  </div>
```

2. Al bloc `<script>` d'`index.html`, afegir les funcions `openIncidentsModal()` i `closeIncidentsModal()`, i escoltar l'esdeveniment `incidentsCleared` per refrescar els comptadors:
```javascript
    function openIncidentsModal() {
      const modal = document.querySelector('#incidents-modal');
      if (modal) modal.classList.remove('hidden');
    }

    function closeIncidentsModal() {
      const modal = document.querySelector('#incidents-modal');
      if (modal) modal.classList.add('hidden');
    }

    document.body.addEventListener('incidentsCleared', function() {
      setTimeout(() => window.location.reload(), 600);
    });
```

```

Amb aquesta passada, d'un sol cop d'ull a la pantalla principal sabràs:

1. Si tot el sistema està en verd (`✅ Sistema OK`) o si alguna execució automàtica nocturna ha tingut problemes (`⚠️ X Incidències`).
2. Quin proveïdor concret ha fallat (amb el triangle i el número a la seva targeta) i quin dia/hora va funcionar per última vegada (`Últim èxit:`).
3. En clicar el triangle, tindràs l'explicació mastegada (ex: *"🔐 Verificació 2FA requerida"* o *"📄 Error de Format PDF"*) sense haver de buscar entre centenars de línies de consola!