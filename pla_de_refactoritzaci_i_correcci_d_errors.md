Analitzant `concat_5.txt` a fons, he trobat **exactament per què s'ha mort la consola de logs per WebSocket**, **per què Veolia falla en silenci** i per què l'usuari es queda a cegues sense saber si el procés va per la factura `3/40` o si està clavat fent un *timeout*.

La teva idea de separar l'acció en **dos botons clars ("📧 Correu" vs "🌐 Portal Web")** combinada amb una **barra de progrés i traçabilitat en temps real (`3/40`, estat de connexió, avís de timeout)** és la solució definitiva.

---

### Diagnòstic dels 3 problemes greus actuals

1. **Per què ha deixat de funcionar el WebSocket dels logs?**
* **El Root Logger d'Uvicorn està en nivell `WARNING` per defecte:** Quan arrenques el contenidor amb `CMD ["uvicorn", "src.web.app:app", ...]` al `Dockerfile`, **no s'executa `configure_logging()**` (que només és a `src/main.py`). Per tant, el *Root Logger* (`logging.getLogger()`) es queda amb el nivell per defecte de Python (`WARNING`). Tots els `self.logger.info(...)` dels connectors es descarten abans d'arribar al `WebLogHandler`!


* **Tall per inactivitat (Idle Timeout):** A `src/web/app.py`, el bucle del WebSocket es queda bloquejat a `await websocket.receive_text()`. Com que el navegador mai envia text al servidor, la connexió cau per timeout als 60s o es queda penjada sense fer *ping/pong*.




2. **Per què no hi ha cap traçabilitat del progrés (`3/40` vs *Timeout*)?**
* Quan cliques un botó, surt un missatge estàtic *"🚀 Sincronització iniciada en segon pla..."* que no s'actualitza mai més.


* Els connectors (`veolia.py`, `digi.py`, `bonpreu.py`, `catgas.py`) iteren sobre els correus o sobre els botons de descàrrega (`for i, btn in enumerate(...)`), però **no emeten cap esdeveniment d'estat ni percentatge** cap a la web ni indiquen quants segons porten esperant una resposta del navegador o quina acció exacta estan executant.




3. **Per què Veolia no funciona i falla sense dir res?**
* **`wait_for_load_state("networkidle")` provoca un Timeout garantit:** A `veolia.py`, cridar `page.wait_for_load_state("networkidle", timeout=30000)` fa que Playwright es quedi congelat 30 segons sencers perquè la web d'Agbar/Veolia té scripts d'analítica en segon pla que mai deixen la xarxa en repòs. L'usuari espera 30 segons sense veure res i el procés mor per timeout.


* **Errors silenciosos:** Si `VEOLIA_USER` o `VEOLIA_PASSWORD` no estan configurats al Secret, `veolia.py` surt amb `return results` sense escriure ni un sol error al log. I si falla el login o surt el banner de cookies, busca factures a la mateixa pantalla de login (`Trobats 0 enllaços`) i acaba com a `"Completat"`.





---

### Document complet d'instruccions per a l'Agent (`REFACTORING_PHASE5.md`)

Copia aquest bloc Markdown i passa-li al teu agent perquè implementi els **botons separats (Correu vs Portal Web)**, la **barra de progrés en temps real (`3/40`, fase actual i detecció de timeouts)**, la **reparació del WebSocket** i el **fix de Veolia**:

```markdown
# Pla de Correcció (Fase 5) — Botons Separats (Correu vs Web), Barra de Progrés en Viu (X/N + Timeouts), Fix WebSocket i Fix Veolia

Aquest document resol 4 problemes crítics:
1. **Els logs no es veien al WebSocket**: `uvicorn` no configurava el nivell `INFO` al Root Logger (quedava en `WARNING` per defecte) i el WebSocket no tenia heartbeat.
2. **Falta de traçabilitat en segon pla (`3/40`, fase actual, timeouts)**: S'afegeix un sistema d'emissió de progrés en temps real (`report_progress`) des de `BaseConnector` cap al WebSocket amb barra de progrés visual, comptador `X/N` factures, pas actual ("Autenticant...", "Descarregant 3/40...", "⚠️ Timeout en factura 4/40...") i resum final.
3. **Separació d'accions (Correu vs Portal Web)**: Cada targeta té ara dos botons independents (`📧 Correu` i `🌐 Portal Web`) per evitar que l'usuari no sàpiga on s'està buscant.
4. **Veolia fallava en silenci per `networkidle`**: S'elimina el bloqueig `wait_for_load_state("networkidle")`, s'accepten les cookies automàticament, es valida el login i es reporten els timeouts de descàrrega de forma clara.

---

## 1. Afegir traçabilitat de progrés (`progress_callback`) i `source` a `src/connectors/base.py`

### Canvi a aplicar a `src/connectors/base.py`
Afegir un atribut `progress_callback` i el mètode `report_progress` a `BaseConnector`, i actualitzar la signatura de `run()`:

```python
from collections.abc import Callable

class BaseConnector(ABC):
    """Classe base abstracta per als connectors de subministraments."""

    name: str = "base"
    tipus: SubministreType = SubministreType.ELECTRICITAT

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"connector.{self.name}")
        self.consecutive_failures: int = 0
        self.cooldown_until: datetime | None = None
        self.progress_callback: Callable[[dict[str, Any]], None] | None = None

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
        if self.progress_callback:
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

    @abstractmethod
    def run(
        self,
        imap_client: ImapMailboxClient,
        mode: str = "normal",
        limit: int | None = None,
        source: str = "auto",
    ) -> list[FacturaResult]:
        """Executa l'ETL per a aquest connector.
        source: "auto" (híbrid), "email" (només bústia IMAP), "portal" (només web scraping).
        """

```

---

## 2. Reescriure `veolia.py` i actualitzar `digi.py`, `bonpreu.py` i `catgas.py` amb traçabilitat `X/N`

### 2.1. Substituir `run` i `fetch_invoices_from_portal` a `src/connectors/veolia.py`

Substituir aquests dos mètodes a `src/connectors/veolia.py` perquè reportin cada pas (`report_progress`), eliminin el bloqueig `networkidle` i informin de qualsevol timeout o error de login:

```python
    def run(
        self,
        imap_client: ImapMailboxClient,
        mode: str = "normal",
        limit: int | None = None,
        source: str = "auto",
    ) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            self.report_progress("Connector desactivat per configuració.", status="warning")
            return results

        days_back = 730 if mode == "all" else settings.DAYS_BACK
        fetch_limit = 1 if mode == "latest" else (limit if limit is not None else (None if mode == "all" else 3))

        # 1. Extracció via Email (IMAP)
        if source in ("auto", "email"):
            self.report_progress(f"📧 Connectant a bústia IMAP (últims {days_back} dies)...")
            emails = imap_client.fetch_recent_emails(
                sender_filters=list(self.EMAIL_SENDERS),
                days_back=days_back,
            )
            emails.sort(key=lambda x: x.date.timestamp() if x.date else 0.0, reverse=True)

            # Comptar adjunts PDF totals trobats als correus
            pdf_attachments = [
                (em, att)
                for em in emails
                for att in em.attachments
                if att.filename.lower().endswith(".pdf")
            ]
            if fetch_limit:
                pdf_attachments = pdf_attachments[:fetch_limit]

            total_pdfs = len(pdf_attachments)
            self.report_progress(
                f"📧 Trobats {len(emails)} correus i {total_pdfs} factures PDF adjuntes.",
                current=0,
                total=total_pdfs,
            )

            for idx, (em, att) in enumerate(pdf_attachments, start=1):
                self.report_progress(
                    f"📧 Processant PDF de correu ({att.filename})...",
                    current=idx,
                    total=total_pdfs,
                )
                metriques = self.extract_metrics_from_pdf(att.data) or self._fallback_metrics(
                    att.data, em.date.date() if em.date else date.today()
                )
                if metriques:
                    res = self.save_and_ingest(att.data, metriques, origen="email_attachment")
                    results.append(res)

            if source == "email":
                return results
            if results and mode != "all":
                return results

        # 2. Extracció via Portal Web (Playwright)
        if source in ("auto", "portal"):
            if not settings.VEOLIA_USER or not settings.VEOLIA_PASSWORD:
                msg = "❌ Faltan credencials VEOLIA_USER / VEOLIA_PASSWORD al Secret per entrar al portal web."
                self.report_progress(msg, status="error")
                if source == "portal":
                    raise RuntimeError(msg)
                return results

            pdf_list = self.fetch_invoices_from_portal(limit=fetch_limit)
            total_dl = len(pdf_list)
            for idx, (pdf_bytes, filename) in enumerate(pdf_list, start=1):
                self.report_progress(
                    f"💾 Analitzant i desant PDF {filename} al NAS i InfluxDB...",
                    current=idx,
                    total=total_dl,
                )
                metriques = self.extract_metrics_from_pdf(pdf_bytes) or self._fallback_metrics(pdf_bytes, date.today())
                if metriques:
                    res = self.save_and_ingest(pdf_bytes, metriques, origen="web_portal_scraping")
                    results.append(res)

        return results

    def fetch_invoices_from_portal(self, limit: int | None = 3) -> list[tuple[bytes, str]]:
        """Autenticació al portal d'Agbar/Veolia via Playwright amb traçabilitat pas a pas."""
        downloaded: list[tuple[bytes, str]] = []
        if self.is_in_cooldown():
            return downloaded

        if not HAVE_PLAYWRIGHT or sync_playwright is None:
            self.report_progress("❌ Playwright no està instal·lat al contenidor.", status="error")
            return downloaded

        session_dir = Path(settings.NAS_MOUNT_PATH) / ".sessions"
        session_file = session_dir / "veolia_session.json"
        try:
            session_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        try:
            self.report_progress("🌐 Iniciant navegador headless Chromium per a Veolia...")
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=settings.HEADLESS_BROWSER)
                try:
                    context_kwargs: dict[str, Any] = {"accept_downloads": True}
                    if session_file.exists():
                        self.report_progress("🌐 Carregant sessió prèvia de Veolia des del NAS...")
                        context_kwargs["storage_state"] = str(session_file)

                    context = browser.new_context(**context_kwargs)
                    page = context.new_page()

                    # 1. Obrir portal (domcontentloaded per evitar bloqueig per scripts d'analítica)
                    self.report_progress(f"🌐 Obrint portal Veolia ({self.PORTAL_INVOICES_URL})...")
                    page.goto(
                        self.PORTAL_INVOICES_URL,
                        wait_until="domcontentloaded",
                        timeout=settings.SCRAPER_TIMEOUT_MS,
                    )
                    page.wait_for_timeout(2500)

                    # Acceptar cookies si apareix OneTrust
                    try:
                        cookie_btn = page.query_selector(
                            "#onetrust-accept-btn-handler, button:has-text('Acceptar'), button:has-text('Aceptar')"
                        )
                        if cookie_btn and cookie_btn.is_visible():
                            self.report_progress("🍪 Acceptant banner de cookies...")
                            cookie_btn.click()
                            page.wait_for_timeout(1000)
                    except PlaywrightError:
                        pass

                    # 2. Comprovar si cal fer login
                    login_input = page.query_selector(
                        "input#login, input[name*='login'], input[type='email'], input[name='username']"
                    )
                    pass_input = page.query_selector(
                        "input#password, input[name*='password'], input[type='password']"
                    )

                    if "login" in page.url.lower() or (login_input and pass_input):
                        self.report_progress(f"🔑 Sessió no activa. Autenticant usuari '{settings.VEOLIA_USER}'...")
                        if not login_input or not pass_input:
                            page.goto(
                                self.PORTAL_LOGIN_URL,
                                wait_until="domcontentloaded",
                                timeout=settings.SCRAPER_TIMEOUT_MS,
                            )
                            page.wait_for_timeout(2000)
                            login_input = page.query_selector(
                                "input#login, input[name*='login'], input[type='email'], input[name='username']"
                            )
                            pass_input = page.query_selector(
                                "input#password, input[name*='password'], input[type='password']"
                            )

                        if not login_input or not pass_input:
                            msg = f"No s'ha trobat el formulari de login a {page.url}"
                            self.record_failure(msg)
                            self.report_progress(f"❌ {msg}", status="error")
                            return downloaded

                        login_input.fill(settings.VEOLIA_USER or "")
                        pass_input.fill(settings.VEOLIA_PASSWORD or "")
                        submit_btn = page.query_selector(
                            "button#btn-submit, button[type='submit'], input[type='submit']"
                        )
                        if submit_btn:
                            submit_btn.click()
                        else:
                            pass_input.press("Enter")

                        page.wait_for_load_state("domcontentloaded", timeout=settings.SCRAPER_TIMEOUT_MS)
                        page.wait_for_timeout(3500)

                        if "login" in page.url.lower() and page.query_selector("input[type='password']"):
                            msg = "Login rebutjat a Veolia (revisa usuari/contrasenya o bloqueig captcha)."
                            self.record_failure(msg)
                            self.report_progress(f"❌ {msg}", status="error")
                            return downloaded

                    # 3. Desar sessió vàlida
                    try:
                        context.storage_state(path=str(session_file))
                    except (OSError, RuntimeError):
                        pass

                    # 4. Anar a l'apartat de factures si cal
                    if "mis-facturas" not in page.url.lower() and "factur" not in page.url.lower():
                        self.report_progress("🌐 Accedint al llistat de factures de Veolia...")
                        page.goto(
                            self.PORTAL_INVOICES_URL,
                            wait_until="domcontentloaded",
                            timeout=settings.SCRAPER_TIMEOUT_MS,
                        )
                        page.wait_for_timeout(3000)

                    # 5. Cercar enllaços de descàrrega de PDF
                    download_buttons = page.query_selector_all(
                        "#descargar-factura-digital a, a:has-text('Descarrega'), a:has-text('Descargar'), "
                        "a[href*='descargarFacturaPdf'], a[href*='.pdf'], button:has-text('PDF')"
                    )
                    total_found = len(download_buttons)
                    max_dl = min(limit, total_found) if limit is not None else total_found

                    if total_found == 0:
                        self.report_progress(
                            f"⚠️ Connectat a {page.url}, però s'han trobat 0 botons de factura descarregables.",
                            status="warning",
                        )
                        return downloaded

                    self.report_progress(
                        f"🌐 Trobades {total_found} factures al portal. Iniciant descàrrega de {max_dl}...",
                        current=0,
                        total=max_dl,
                    )

                    for i, btn in enumerate(download_buttons[:max_dl], start=1):
                        self.report_progress(
                            f"⬇️ Descarregant factura {i} de {max_dl} des del portal Veolia...",
                            current=i,
                            total=max_dl,
                        )
                        try:
                            with page.expect_download(timeout=20000) as download_info:
                                btn.click()
                            download = download_info.value
                            path = download.path()
                            if path:
                                with open(path, "rb") as f:
                                    downloaded.append(
                                        (f.read(), download.suggested_filename or f"veolia_factura_{i}.pdf")
                                    )
                        except PlaywrightTimeoutError:
                            self.report_progress(
                                f"⏱️ Timeout (20s) esperant la descàrrega de la factura {i}/{max_dl}. Saltant a la següent...",
                                current=i,
                                total=max_dl,
                                status="warning",
                            )
                        except (PlaywrightError, TimeoutError, OSError) as err:
                            self.logger.warning(f"Error en factura {i}/{max_dl}: {err}")

                except PlaywrightTimeoutError as t_err:
                    msg = f"⏱️ Timeout carregant el portal de Veolia ({settings.SCRAPER_TIMEOUT_MS}ms): {t_err}"
                    self.record_failure(msg)
                    self.report_progress(msg, status="error")
                except (PlaywrightError, TimeoutError, OSError, RuntimeError) as page_err:
                    self.record_failure(f"Error de navegació a Veolia: {page_err}")
                    self.report_progress(f"❌ Error navegant a Veolia: {page_err}", status="error")
                finally:
                    browser.close()

        except (PlaywrightError, TimeoutError, OSError, RuntimeError) as e:
            self.record_failure(f"Error de Playwright a Veolia: {e}")
            self.report_progress(f"❌ Error crític de Playwright: {e}", status="error")

        if downloaded:
            self.record_success()

        return downloaded

```

### 2.2. Aplicar `source` i `self.report_progress(..., current=i, total=max_dl)` a `digi.py`, `bonpreu.py` i `catgas.py`

1. A **`src/connectors/digi.py`** i **`src/connectors/bonpreu.py`**:
* Afegir `source: str = "auto"` a `run()`.
* Si `source == "email"`, executar només la cerca IMAP i retornar `results`.
* Si `source == "portal"`, saltar el bloc d'IMAP i anar directament a `fetch_invoices_from_portal`.
* Dins `fetch_invoices_from_portal`, corregir `max_dl = min(limit, len(download_buttons)) if limit is not None else len(download_buttons)` i dins del bucle `for i, btn in enumerate(download_buttons[:max_dl], start=1):` cridar:
`self.report_progress(f"⬇️ Descarregant factura {i} de {max_dl} del portal...", current=i, total=max_dl)`
I al bloc `except PlaywrightTimeoutError:` cridar:
`self.report_progress(f"⏱️ Timeout descarregant factura {i}/{max_dl}...", current=i, total=max_dl, status="warning")`


2. A **`src/connectors/catgas.py`**:
* Afegir `source: str = "auto"` a `run()`.
* Dins del bucle de PDFs de correu, cridar `self.report_progress(f"📧 Processant factura Catgas {idx}/{total_pdfs}: {att.filename}", current=idx, total=total_pdfs)`.
* Si `source == "portal"`, fer `self.report_progress("⚠️ Catgas només rep factures per Correu IMAP (no té scraper web configurat). Cercant al correu...", status="warning")`.



---

## 3. Reparar el WebSocket de Logs i emetre el Progrés en Viu a `src/web/app.py`

### Canvi 1: Configurar el nivell `INFO` al Root Logger i afegir `broadcast_progress` i `broadcast_status` a `src/web/app.py`

Substituir la configuració de `WebLogHandler` i `ConnectionManager` a `src/web/app.py`:

```python
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

```

I just a sota de `WebLogHandler` (línia 83), forçar el nivell del Root Logger perquè Uvicorn no descarti els logs `INFO`:

```python
root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
web_handler = WebLogHandler()
web_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] (%(name)s) %(message)s", "%H:%M:%S"))
root_logger.addHandler(web_handler)

```

### Canvi 2: Afegir Heartbeat (`ping`) a `websocket_logs_endpoint` a `src/web/app.py`

```python
@app.websocket("/ws/logs")
async def websocket_logs_endpoint(websocket: WebSocket):
    """Canal WebSocket amb heartbeat automàtic per transmetre logs, progrés X/N i estat."""
    await ws_manager.connect(websocket)
    try:
        await websocket.send_json({"type": "version", "version": f"v{settings.APP_VERSION}"})
        for line in list(log_buffer)[-60:]:
            await websocket.send_json({"type": "log", "line": line})
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=20.0)
            except TimeoutError:
                await websocket.send_json({"type": "ping"})
    except (WebSocketDisconnect, ConnectionResetError, OSError):
        ws_manager.disconnect(websocket)

```

### Canvi 3: Connectar `progress_callback` a `run_sync_task` i `/api/sync` a `src/web/app.py`

Substituir `run_sync_task` i `trigger_sync` a `src/web/app.py`:

```python
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
        except Exception as err:
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

```

---

## 4. Actualitzar els Botons i la Barra de Progrés en Viu a `src/web/templates/index.html`

### Canvi 1: Posar 2 botons separats ("📧 Correu" i "🌐 Portal Web") + "📚 Històric" a cada targeta (línies 140-163)

Substituir el bloc de botons de cada card a `src/web/templates/index.html`:

```html
          <div class="pt-4 border-t border-slate-800/80 mt-2 space-y-2">
            <div class="grid grid-cols-2 gap-2">
              <!-- Botó 1: Cercar per Correu (IMAP) -->
              <button 
                hx-post="/api/sync" 
                hx-vals='{"connector": "{{ card.id }}", "mode": "normal", "source": "email"}'
                hx-target="#notification-area"
                title="Cerca factures als correus electrònics rebuts (IMAP)"
                class="flex items-center justify-center gap-1.5 py-2 px-2 bg-blue-950/60 hover:bg-blue-900/80 text-blue-300 text-xs font-semibold rounded-xl border border-blue-800/60 transition shadow-sm">
                <i data-lucide="mail" class="w-3.5 h-3.5 text-blue-400"></i>
                <span>Correu</span>
              </button>

              <!-- Botó 2: Fer Scraping al Portal Web -->
              <button 
                hx-post="/api/sync" 
                hx-vals='{"connector": "{{ card.id }}", "mode": "normal", "source": "portal"}'
                hx-target="#notification-area"
                title="Entra al portal web de la companyia amb Playwright i descarrega els PDFs"
                class="flex items-center justify-center gap-1.5 py-2 px-2 bg-emerald-950/60 hover:bg-emerald-900/80 text-emerald-300 text-xs font-semibold rounded-xl border border-emerald-800/60 transition shadow-sm">
                <i data-lucide="globe" class="w-3.5 h-3.5 text-emerald-400"></i>
                <span>Portal Web</span>
              </button>
            </div>

            <!-- Botó 3: Explorador Històric & Bulk -->
            <button 
              hx-get="/api/history/{{ card.id }}"
              hx-target="#history-modal-content"
              onclick="openHistoryModal()"
              title="Explora tot l'històric disponible i tria quines descarregar"
              class="w-full flex items-center justify-center gap-1.5 py-1.5 px-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-medium rounded-xl border border-slate-700 transition">
              <i data-lucide="history" class="w-3.5 h-3.5 text-amber-400"></i>
              <span>Explorar Històric (730 dies)</span>
            </button>
          </div>

```

### Canvi 2: Actualitzar el receptor WebSocket (`logsSocket.onmessage`) a `src/web/templates/index.html`

Dins de `logsSocket.onmessage = function(event)` a `src/web/templates/index.html`, afegir la gestió dels missatges `type === 'progress'` (per actualitzar el comptador `3/40` i la barra) i `type === 'status'` (per mostrar el resultat final o error):

```javascript
            if (data.type === 'ping') return;

            // A) Progrés pas a pas en temps real (ex: 3/40 o avís de timeout)
            if (data.type === 'progress') {
              const notif = document.querySelector('#notification-area');
              if (notif && !document.querySelector('#live-progress-bar')) {
                notif.innerHTML = `
                  <div class="p-4 mb-4 rounded-xl bg-slate-900 border border-blue-700/60 shadow-lg space-y-2">
                    <div class="flex items-center justify-between text-xs font-semibold text-blue-300">
                      <span id="live-progress-title">⏳ Executant ${data.connector || 'ETL'}...</span>
                      <span id="live-progress-counter" class="font-mono text-emerald-400"></span>
                    </div>
                    <div class="w-full bg-slate-800 rounded-full h-2 overflow-hidden">
                      <div id="live-progress-bar" class="bg-emerald-500 h-2 rounded-full transition-all duration-300" style="width: 10%"></div>
                    </div>
                    <p id="live-progress-step" class="text-xs text-slate-300 font-mono truncate"></p>
                  </div>`;
              }
              const titleEl = document.querySelector('#live-progress-title');
              const counterEl = document.querySelector('#live-progress-counter');
              const barEl = document.querySelector('#live-progress-bar');
              const stepEl = document.querySelector('#live-progress-step');

              if (titleEl && data.connector) titleEl.textContent = `⏳ Processant ${data.connector}...`;
              if (stepEl && data.step) stepEl.textContent = data.step;
              if (counterEl) {
                counterEl.textContent = data.total > 0 ? `${data.current} / ${data.total} (${data.percent}%)` : 'En curs...';
              }
              if (barEl) {
                barEl.style.width = (data.total > 0 ? Math.max(8, data.percent) : 25) + '%';
                if (data.status === 'warning') barEl.className = 'bg-amber-500 h-2 rounded-full transition-all duration-300';
                else if (data.status === 'error') barEl.className = 'bg-red-500 h-2 rounded-full transition-all duration-300';
                else barEl.className = 'bg-emerald-500 h-2 rounded-full transition-all duration-300';
              }
            }

            // B) Resultat Final d'Execució (OK / Warning / Error)
            if (data.type === 'status' && data.message) {
              const notif = document.querySelector('#notification-area');
              if (notif) {
                let styles = 'text-blue-300 bg-blue-950/60 border-blue-700/50';
                if (data.status === 'error') styles = 'text-red-300 bg-red-950/70 border-red-700/60';
                else if (data.status === 'warning') styles = 'text-amber-300 bg-amber-950/70 border-amber-700/60';
                else if (data.status === 'ok') styles = 'text-emerald-300 bg-emerald-950/70 border-emerald-700/60';

                notif.innerHTML = `<div class="p-4 mb-4 text-sm rounded-xl border ${styles} font-medium shadow">${data.message}</div>`;
                if (data.new_count && data.new_count > 0) {
                  setTimeout(() => window.location.reload(), 2000);
                }
              }
            }

```

```

Amb això, tant a la barra superior com a la consola inferior sabràs al segon si està obrint el navegador, si està fent login, si ha trobat 40 factures i va per la `3 / 40 (7%)`, o si s'ha encallat en un timeout en una factura concreta (la barra es posarà taronja/vermella a l'instant)!

```