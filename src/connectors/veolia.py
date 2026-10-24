# pyright: reportMissingImports=false
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    HAVE_PLAYWRIGHT = True
except ImportError:
    HAVE_PLAYWRIGHT = False
    PlaywrightError = Exception  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore
    sync_playwright = None  # type: ignore

from src.config import settings
from src.connectors.base import BaseConnector
from src.email_client import ImapMailboxClient
from src.models import FacturaMetriques, FacturaResult, SubministreType
from src.pdf_parser import (
    extract_date_range,
    extract_float_value,
    extract_regex_first,
    extract_text_from_pdf_bytes,
    parse_date_catalan_spanish,
    parse_euro_amount,
)

logger = logging.getLogger("connector.veolia")


class VeoliaConnector(BaseConnector):
    """Connector per a Veolia / Aigües de Barcelona (Aigua).

    Extreu factures PDF rebudes per correu com a fitxer adjunt o descarrega del portal.
    """

    name = "veolia"
    tipus = SubministreType.AIGUA

    EMAIL_SENDERS: tuple[str, ...] = ("veolia", "aiguesdebarcelona", "agbar", "suac@veolia.com")
    PORTAL_LOGIN_URL = "https://agbar.veolia.cat/login"
    PORTAL_INVOICES_URL = "https://agbar.veolia.cat/ca/group/sgab/mis-facturas"

    def is_enabled(self) -> bool:
        return settings.VEOLIA_ENABLED

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

            pdf_attachments = [
                (em, att) for em in emails for att in em.attachments if att.filename.lower().endswith(".pdf")
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
                    pass_input = page.query_selector("input#password, input[name*='password'], input[type='password']")

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

                    # 5. Cercar enllaços de descàrrega de PDF (segons DOM real)
                    download_buttons = page.query_selector_all(
                        "#descargar-factura-digital a, a:has-text('Descarrega'), a:has-text('Descargar'), "
                        "a[href*='descargarFacturaPdf'], a[onclick*='descargarFacturaPdf'], a[href*='.pdf'], button:has-text('PDF')"
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

    def fetch_available_invoices(self, imap_client: ImapMailboxClient) -> list[dict]:
        """Llista les factures de Veolia trobades a la bústia i el seu estat al NAS."""
        invoices: list[dict] = []
        if not self.is_enabled():
            return invoices

        emails = imap_client.fetch_recent_emails(sender_filters=list(self.EMAIL_SENDERS), days_back=730)
        emails.sort(key=lambda x: x.date.timestamp() if x.date else 0.0, reverse=True)

        from src.storage import storage

        for em in emails:
            for att in em.attachments:
                if not att.filename.lower().endswith(".pdf"):
                    continue

                metriques = self.extract_metrics_from_pdf(att.data)
                if metriques:
                    any_str = str(metriques.data_emissio.year)
                    mes_str = f"{metriques.data_emissio.month:02d}"
                    clean_num = metriques.numero_factura.replace("/", "_").replace("\\", "_").replace(" ", "")
                    expected_filename = f"{any_str}_{mes_str}_{self.name}_{clean_num}.pdf"
                    d_str = str(metriques.data_emissio)
                    parsed_amt = metriques.import_total
                    num_display = metriques.numero_factura
                else:
                    d_str = em.date.strftime("%Y-%m-%d") if em.date else "Data desconeguda"
                    any_str = str(em.date.year) if em.date else str(date.today().year)
                    mes_str = f"{em.date.month:02d}" if em.date else "01"
                    clean_name = re.sub(r"[^A-Za-z0-9_-]", "_", att.filename.replace(".pdf", "").replace(".PDF", ""))
                    expected_filename = f"{any_str}_{mes_str}_{self.name}_{clean_name}.pdf"
                    parsed_amt = parse_euro_amount(em.subject) or parse_euro_amount(em.body_text) or 0.0
                    num_display = att.filename.replace(".pdf", "")

                ja_al_nas = storage.exists(self.tipus.value, self.name, any_str, expected_filename)
                invoices.append(
                    {
                        "id": f"{em.msg_id}_{att.filename}",
                        "proveidor": self.name,
                        "tipus": self.tipus.value,
                        "data": d_str,
                        "numero": num_display,
                        "import": parsed_amt,
                        "subject": em.subject,
                        "exists_in_nas": ja_al_nas,
                    }
                )
        return invoices

    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Parseja el PDF de Veolia per obtenir dades estructurades de consum i facturació."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # 1. Número de factura
        # Exemples: "Número factura: 2026/123456" o "Factura núm. B12345678"
        num_factura = extract_regex_first(text, r"(?:Factura\s*n[úu]m\.?|N[úu]mero\s*factura:?)\s*([A-Za-z0-9/_-]+)")
        if not num_factura:
            num_factura = extract_regex_first(text, r"Factura\s+([A-Za-z0-9/_-]{6,20})")

        # 2. Data d'emissió
        data_str = extract_regex_first(
            text, r"(?:Data\s*d'emissi[óo]|Fecha\s*emisi[óo]n):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"
        )
        data_emissio = parse_date_catalan_spanish(data_str) if data_str else None

        # 3. Import total
        total_eur = extract_float_value(
            text, r"(?:Total\s*factura|Import\s*total|Total\s*a\s*pagar):?\s*(\d+(?:[.,]\d{1,2})?)\s*€?"
        )
        if total_eur is None:
            total_eur = parse_euro_amount(text)

        # Si no tenim com a mínim import total, no és una factura vàlida
        if total_eur is None:
            return None

        # 4. Consum d'aigua en m3
        # Exemples: "Consum facturat: 12 m3" o "Consum del període: 14 m³"
        consum_m3 = extract_float_value(
            text, r"(?:Consum\s*facturat|Consumo\s*facturado|Consum\s*per[íi]ode):?\s*(\d+(?:[.,]\d{1,2})?)\s*m[3³]"
        )
        if consum_m3 is None:
            consum_m3 = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*m[3³]")

        # 5. Dates del període
        dt_inici, dt_fi = extract_date_range(
            text,
            r"(?:Per[íi]ode\s*facturat|Per[íi]odo):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:a|fins\s*a|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        )

        # 6. Referència o contracte
        contracte = extract_regex_first(text, r"(?:Contracte|Contrato|P[òo]lissa):?\s*([0-9A-Za-z/_-]+)")

        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=num_factura or f"VEOLIA-{date.today().isoformat()}",
            data_emissio=data_emissio or date.today(),
            periode_inici=dt_inici,
            periode_fi=dt_fi,
            dies_facturats=(dt_fi - dt_inici).days if (dt_inici and dt_fi) else None,
            import_total=float(total_eur),
            consum_m3=consum_m3,
            cups_o_referencia=contracte,
        )

    def _fallback_metrics(self, pdf_bytes: bytes, fallback_date: date) -> FacturaMetriques | None:
        amount = parse_euro_amount(extract_text_from_pdf_bytes(pdf_bytes))
        if amount is None:
            return None
        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=f"VEOLIA-{fallback_date.strftime('%Y%m')}",
            data_emissio=fallback_date,
            import_total=amount,
        )
