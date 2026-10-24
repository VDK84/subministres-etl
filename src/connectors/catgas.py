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

logger = logging.getLogger("connector.catgas")


class CatgasConnector(BaseConnector):
    """Connector per a Catgas Energia (Gas natural).

    Extreu factures PDF tant des de la bústia de correu (IMAP) com des de l'Oficina Virtual web (Playwright).
    """

    name = "catgas"
    tipus = SubministreType.GAS

    EMAIL_SENDERS: tuple[str, ...] = ("catgas", "catgas.cat", "catgas.es", "facturacio@catgas.cat")
    PORTAL_LOGIN_URL = "https://oficinavirtual.catgas.cat/acceso"
    PORTAL_INVOICES_URL = "https://oficinavirtual.catgas.cat/factures"

    def is_enabled(self) -> bool:
        return settings.CATGAS_ENABLED

    def run(
        self,
        imap_client: ImapMailboxClient,
        mode: str = "normal",
        limit: int | None = None,
        source: str = "auto",
    ) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            self.report_progress("Connector Catgas desactivat per configuració.", status="warning")
            return results

        days_back = 730 if mode == "all" else settings.DAYS_BACK
        fetch_limit = 1 if mode == "latest" else (limit if limit is not None else (None if mode == "all" else 3))

        # 1. Extracció via Email (IMAP)
        if source in ("auto", "email"):
            self.report_progress(f"📧 Connectant a bústia IMAP per Catgas (últims {days_back} dies)...")
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
                    f"📧 Processant factura Catgas {idx}/{total_pdfs}: {att.filename}",
                    current=idx,
                    total=total_pdfs,
                )
                metriques = self.extract_metrics_from_pdf(att.data)
                if not metriques:
                    metriques = self._fallback_metrics(att.data, em.date.date() if em.date else date.today())

                if metriques:
                    res = self.save_and_ingest(att.data, metriques, origen="email_attachment")
                    results.append(res)

                    if mode == "latest":
                        return results
                    if limit and len(results) >= limit:
                        return results

            if source == "email":
                return results
            if results and mode != "all":
                return results

        # 2. Extracció via Oficina Virtual web (Playwright)
        if source in ("auto", "portal"):
            if not settings.CATGAS_USER or not settings.CATGAS_PASSWORD:
                msg = "❌ Faltan credencials CATGAS_USER / CATGAS_PASSWORD al Secret per entrar a l'Oficina Virtual de Catgas."
                self.report_progress(msg, status="error")
                if source == "portal":
                    raise RuntimeError(msg)
                return results

            self.report_progress("🌐 Iniciant descàrrega de factures des de l'Oficina Virtual de Catgas...")
            pdf_list = self.fetch_invoices_from_portal(limit=fetch_limit)
            total_dl = len(pdf_list)
            for idx, (pdf_bytes, filename) in enumerate(pdf_list, start=1):
                self.report_progress(
                    f"💾 Analitzant i desant factura Catgas {filename} al NAS i InfluxDB...",
                    current=idx,
                    total=total_dl,
                )
                metriques = self.extract_metrics_from_pdf(pdf_bytes)
                if not metriques:
                    metriques = self._fallback_metrics(pdf_bytes, date.today())
                if metriques:
                    res = self.save_and_ingest(pdf_bytes, metriques, origen="web_portal_scraping")
                    results.append(res)
                    if mode == "latest":
                        return results

        return results

    def fetch_invoices_from_portal(self, limit: int | None = 3) -> list[tuple[bytes, str]]:
        """Autenticació a l'Oficina Virtual de Catgas via Playwright i descàrrega de factures de Gas."""
        downloaded: list[tuple[bytes, str]] = []
        if self.is_in_cooldown():
            return downloaded

        if not HAVE_PLAYWRIGHT or sync_playwright is None:
            self.report_progress("❌ Playwright no està instal·lat al contenidor.", status="error")
            return downloaded

        session_dir = Path(settings.NAS_MOUNT_PATH) / ".sessions"
        session_file = session_dir / "catgas_session.json"
        try:
            session_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        try:
            self.report_progress("🌐 Iniciant navegador headless Chromium per a Catgas...")
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=settings.HEADLESS_BROWSER)
                try:
                    context_kwargs: dict[str, Any] = {"accept_downloads": True}
                    if session_file.exists():
                        self.report_progress("🌐 Carregant sessió prèvia de Catgas des del NAS...")
                        context_kwargs["storage_state"] = str(session_file)

                    context = browser.new_context(**context_kwargs)
                    page = context.new_page()

                    # 1. Obrir pàgina de factures
                    self.report_progress(f"🌐 Obrint Oficina Virtual Catgas ({self.PORTAL_INVOICES_URL})...")
                    page.goto(
                        self.PORTAL_INVOICES_URL,
                        wait_until="domcontentloaded",
                        timeout=settings.SCRAPER_TIMEOUT_MS,
                    )
                    page.wait_for_timeout(2000)

                    # Acceptar banner de cookies si apareix
                    try:
                        cookie_btn = page.query_selector("button:has-text('ACEPTAR TODAS')")
                        if cookie_btn and cookie_btn.is_visible():
                            self.report_progress("🍪 Acceptant cookies de Catgas...")
                            cookie_btn.click()
                            page.wait_for_timeout(500)
                    except PlaywrightError:
                        pass

                    # 2. Comprovar si cal fer login
                    is_authenticated = (
                        "factures" in page.url.lower()
                        and "acceso" not in page.url.lower()
                        and page.query_selector("button:has-text('GAS'), [role='tab']:has-text('GAS')") is not None
                    )

                    if not is_authenticated:
                        self.report_progress(f"🔑 Sessió no activa. Accedint a login ({self.PORTAL_LOGIN_URL})...")
                        page.goto(
                            self.PORTAL_LOGIN_URL,
                            wait_until="domcontentloaded",
                            timeout=settings.SCRAPER_TIMEOUT_MS,
                        )
                        page.wait_for_timeout(2000)

                        try:
                            cookie_btn = page.query_selector("button:has-text('ACEPTAR TODAS')")
                            if cookie_btn and cookie_btn.is_visible():
                                cookie_btn.click()
                                page.wait_for_timeout(500)
                        except PlaywrightError:
                            pass

                        user_input = page.wait_for_selector("input#username", timeout=15000)
                        pass_input = page.wait_for_selector("input#password-field", timeout=15000)

                        if not user_input or not pass_input:
                            msg = f"No s'ha trobat el formulari de login a {page.url}"
                            self.record_failure(msg)
                            self.report_progress(f"❌ {msg}", status="error")
                            return downloaded

                        self.report_progress(f"🔑 Introduint credencials de '{settings.CATGAS_USER}'...")
                        user_input.fill(settings.CATGAS_USER or "")
                        pass_input.fill(settings.CATGAS_PASSWORD or "")
                        page.click("button[type='submit']:has-text('ACCEDER')")
                        page.wait_for_load_state("domcontentloaded", timeout=settings.SCRAPER_TIMEOUT_MS)
                        page.wait_for_timeout(4000)

                        if "acceso" in page.url.lower() and page.query_selector("input#password-field"):
                            msg = "Login rebutjat a l'Oficina Virtual de Catgas (revisa NIF/contrasenya)."
                            self.record_failure(msg)
                            self.report_progress(f"❌ {msg}", status="error")
                            return downloaded

                    # 3. Desar sessió vàlida al NAS
                    try:
                        context.storage_state(path=str(session_file))
                        self.logger.info("✅ Sessió de Catgas desada al NAS!")
                    except (OSError, RuntimeError):
                        pass

                    # 4. Assegurar que som a la pestanya GAS
                    self.report_progress("🔥 Seleccionant pestanya de factures de GAS...")
                    gas_tab = page.query_selector("button:has-text('GAS'), [role='tab']:has-text('GAS')")
                    if gas_tab:
                        gas_tab.click()
                        page.wait_for_timeout(3000)

                    # 5. Cercar botons de descàrrega de PDF
                    dl_buttons = page.query_selector_all(
                        "button:has-text('DESCARREGAR PDF'), button:has-text('DESCARGAR PDF')"
                    )
                    total_found = len(dl_buttons)
                    max_dl = min(limit, total_found) if limit is not None else total_found

                    if total_found == 0:
                        self.report_progress(
                            "⚠️ Connectat a l'Oficina Virtual de Catgas, però s'han trobat 0 botons de descàrrega de gas.",
                            status="warning",
                        )
                        return downloaded

                    self.report_progress(
                        f"🌐 Trobades {total_found} factures de gas al portal. Iniciant descàrrega de {max_dl}...",
                        current=0,
                        total=max_dl,
                    )

                    for i, btn in enumerate(dl_buttons[:max_dl], start=1):
                        self.report_progress(
                            f"⬇️ Descarregant factura de gas {i} de {max_dl} des de Catgas...",
                            current=i,
                            total=max_dl,
                        )
                        try:
                            with page.expect_download(timeout=20000) as download_info:
                                page.evaluate("el => el.click()", btn)
                            download = download_info.value
                            path = download.path()
                            if path:
                                with open(path, "rb") as f:
                                    downloaded.append(
                                        (f.read(), download.suggested_filename or f"catgas_factura_{i}.pdf")
                                    )
                                self.report_progress(
                                    f"✅ Factura {i}/{max_dl} descarregada correctament!",
                                    current=i,
                                    total=max_dl,
                                )
                        except PlaywrightTimeoutError:
                            self.report_progress(
                                f"⏱️ Timeout (20s) esperant la descàrrega de la factura {i}/{max_dl}. Saltant a la següent...",
                                current=i,
                                total=max_dl,
                                status="warning",
                            )
                        except (PlaywrightError, TimeoutError, OSError) as err:
                            self.logger.warning(f"Error en factura Catgas {i}/{max_dl}: {err}")

                except PlaywrightTimeoutError as t_err:
                    msg = f"⏱️ Timeout carregant l'Oficina Virtual de Catgas: {t_err}"
                    self.record_failure(msg)
                    self.report_progress(msg, status="error")
                except (PlaywrightError, TimeoutError, OSError, RuntimeError) as page_err:
                    self.record_failure(f"Error de navegació a Catgas: {page_err}")
                    self.report_progress(f"❌ Error navegant a Catgas: {page_err}", status="error")
                finally:
                    browser.close()

        except (PlaywrightError, TimeoutError, OSError, RuntimeError) as e:
            self.record_failure(f"Error de Playwright a Catgas: {e}")
            self.report_progress(f"❌ Error crític de Playwright: {e}", status="error")

        if downloaded:
            self.record_success()

        return downloaded

    def fetch_available_invoices(self, imap_client: ImapMailboxClient) -> list[dict]:
        """Llista les factures de Catgas trobades a la bústia i el seu estat al NAS."""
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
        """Parseja el PDF de Catgas per obtenir dades de consum en kWh/m3 i import."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # 1. Número de factura (ex: Factura núm: FG2026/001234)
        num_factura = extract_regex_first(
            text, r"(?:Factura\s*n[úu]m\.?|N[úu]mero\s*de\s*factura:?)\s*([A-Za-z0-9/_-]+)"
        )
        if not num_factura:
            num_factura = extract_regex_first(text, r"(FG\d{4}[/-]?\d+)")

        # 2. Data d'emissió
        data_str = extract_regex_first(
            text, r"(?:Data\s*emissi[óo]|Fecha\s*emisi[óo]n):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"
        )
        data_emissio = parse_date_catalan_spanish(data_str) if data_str else None

        # 3. Import total
        total_eur = extract_float_value(
            text, r"(?:Total\s*factura|Import\s*total|Total\s*a\s*pagar):?\s*(\d+(?:[.,]\d{1,2})?)\s*€?"
        )
        if total_eur is None:
            total_eur = parse_euro_amount(text)

        if total_eur is None:
            return None

        # 4. Consum de gas en kWh i m3
        consum_kwh = extract_float_value(
            text, r"(?:Consum\s*facturat|Consumo|Energia\s*facturada):?\s*(\d+(?:[.,]\d{1,2})?)\s*kWh"
        )
        if consum_kwh is None:
            consum_kwh = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*kWh")

        consum_m3 = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*m[3³]")

        # 5. Dates del període
        dt_inici, dt_fi = extract_date_range(
            text,
            r"(?:Per[íi]ode\s*de\s*facturaci[óo]|Per[íi]ode):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:fins\s*a|a|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        )

        # 6. CUPS de gas (ES02...)
        cups = extract_regex_first(text, r"(ES\d{16,20}[A-Za-z]{0,2})")

        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=num_factura or f"CATGAS-{date.today().isoformat()}",
            data_emissio=data_emissio or date.today(),
            periode_inici=dt_inici,
            periode_fi=dt_fi,
            dies_facturats=(dt_fi - dt_inici).days if (dt_inici and dt_fi) else None,
            import_total=float(total_eur),
            consum_kwh=consum_kwh,
            consum_m3=consum_m3,
            cups_o_referencia=cups,
        )

    def _fallback_metrics(self, pdf_bytes: bytes, fallback_date: date) -> FacturaMetriques | None:
        amount = parse_euro_amount(extract_text_from_pdf_bytes(pdf_bytes))
        if amount is None:
            return None
        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=f"CATGAS-{fallback_date.strftime('%Y%m')}",
            data_emissio=fallback_date,
            import_total=amount,
        )
