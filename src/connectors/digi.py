# pyright: reportMissingImports=false
import logging
import re
from datetime import date

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
    extract_float_value,
    extract_regex_first,
    extract_text_from_pdf_bytes,
    parse_date_catalan_spanish,
    parse_euro_amount,
)

logger = logging.getLogger("connector.digi")


class DigiConnector(BaseConnector):
    """Connector per a Digi Mobil (Fibra i Telefonia Mòbil).

    Digi no adjunta la factura en PDF al correu electrònic, només notifica l'import.
    El connector detecta l'avís i connecta al portal 'Mi Digi' per descarregar el PDF oficial.
    """

    name = "digi"
    tipus = SubministreType.TELEFONIA

    EMAIL_SENDERS: tuple[str, ...] = (
        "digimobil.es",
        "factura@digimobil.es",
        "digi",
        "clientes@digimobil.es",
    )

    PORTAL_LOGIN_URL = "https://mi.digimobil.es/login"

    def is_enabled(self) -> bool:
        return settings.DIGI_ENABLED

    def run(
        self,
        imap_client: ImapMailboxClient,
        mode: str = "normal",
        limit: int | None = None,
    ) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            return results

        if self.is_in_cooldown():
            return results

        fetch_limit = 1 if mode == "latest" else (limit or (None if mode == "all" else 3))
        self.logger.info(f"Executant connector Digi Mobil (mode={mode}, limit={fetch_limit})...")

        # 1. Comprovar correus d'avís de Digi
        days_back = 730 if mode == "all" else settings.DAYS_BACK
        emails = imap_client.fetch_recent_emails(
            sender_filters=list(self.EMAIL_SENDERS),
            days_back=days_back,
        )
        self.logger.info(f"Trobats {len(emails)} correus d'avís de Digi Mobil")

        trobat_avis = False
        import_avis: float | None = None
        data_avis: date | None = None

        for em in emails:
            trobat_avis = True
            parsed_amt = parse_euro_amount(em.body_text) or parse_euro_amount(em.subject)
            if parsed_amt is not None and import_avis is None:
                import_avis = parsed_amt
            if em.date and data_avis is None:
                data_avis = em.date.date()

            # Comprovació si el correu inclou PDF adjunt
            for att in em.attachments:
                if att.filename.lower().endswith(".pdf"):
                    self.logger.info(f"Detectat PDF adjunt a correu Digi: {att.filename}")
                    metriques = self.extract_metrics_from_pdf(att.data)
                    if metriques:
                        res = self.save_and_ingest(att.data, metriques, origen="email_attachment")
                        results.append(res)
                        if mode == "latest":
                            return results

        if results:
            return results

        # 2. Descàrrega des de Mi Digi si no hi ha PDF adjunt
        if not settings.DIGI_USER or not settings.DIGI_PASSWORD:
            self.logger.warning(
                "Credencials de Mi Digi (DIGI_USER / DIGI_PASSWORD) no definides. "
                "No es pot fer descàrrega automàtica del PDF des del portal."
            )
            if trobat_avis and import_avis is not None:
                data_final = data_avis or date.today()
                metriques_avis = FacturaMetriques(
                    proveidor=self.name,
                    tipus=self.tipus,
                    numero_factura=f"DIGI-AVIS-{data_final.strftime('%Y%m')}",
                    data_emissio=data_final,
                    import_total=import_avis,
                )
                res = self.save_and_ingest(b"", metriques_avis, origen="email_notification")
                results.append(res)
            return results

        self.logger.info("Iniciant descàrrega de factures des del portal Mi Digi...")
        pdf_list = self.fetch_invoices_from_portal(limit=fetch_limit)
        for pdf_bytes, filename in pdf_list:
            metriques = self.extract_metrics_from_pdf(pdf_bytes)
            if metriques:
                res = self.save_and_ingest(pdf_bytes, metriques, origen="web_portal_scraping")
                results.append(res)
                if mode == "latest":
                    return results

        return results

    def fetch_invoices_from_portal(self, limit: int | None = 3) -> list[tuple[bytes, str]]:
        """Autenticació al portal Mi Digi amb Playwright per descarregar les últimes factures."""
        downloaded: list[tuple[bytes, str]] = []
        if self.is_in_cooldown():
            return downloaded

        if not HAVE_PLAYWRIGHT or sync_playwright is None:
            self.logger.warning("Playwright no està instal·lat en aquest entorn.")
            return downloaded

        portal_url = settings.DIGI_PORTAL_URL
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=settings.HEADLESS_BROWSER)
                try:
                    context = browser.new_context(accept_downloads=True)
                    page = context.new_page()

                    self.logger.info(f"Obrint pàgina d'inici de sessió de Mi Digi: {portal_url}")
                    page.goto(portal_url, timeout=settings.SCRAPER_TIMEOUT_MS)

                    # Formulari d'inici de sessió Mi Digi (telèfon o email + password)
                    page.fill(
                        "input[name='username'], input[type='text'], input[type='email']", settings.DIGI_USER or ""
                    )
                    page.fill("input[type='password'], input[name='password']", settings.DIGI_PASSWORD or "")
                    page.click("button[type='submit'], input[type='submit']")

                    page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)

                    # Navegar a factures
                    if "factura" not in page.url.lower():
                        factura_link = page.query_selector("a[href*='factura'], button:has-text('Factura')")
                        if factura_link:
                            factura_link.click()
                            page.wait_for_load_state("networkidle")

                    # Cerca botons de descàrrega
                    download_buttons = page.query_selector_all(
                        "a[href*='.pdf'], button:has-text('Descargar'), a:has-text('PDF')"
                    )
                    max_dl = limit or 3
                    self.logger.info(f"Trobats {len(download_buttons)} enllaços de factura a Mi Digi (límit: {max_dl})")

                    for i, btn in enumerate(download_buttons[:max_dl]):
                        try:
                            with page.expect_download(timeout=15000) as download_info:
                                btn.click()
                            download = download_info.value
                            path = download.path()
                            if path:
                                with open(path, "rb") as f:
                                    downloaded.append((f.read(), download.suggested_filename or f"digi_{i}.pdf"))
                        except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError) as err:
                            self.logger.warning(f"No s'ha pogut descarregar el botó {i}: {err}")

                except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as page_err:
                    self.record_failure(f"Error de navegació a Mi Digi: {page_err}")
                    self.logger.error(f"Error navegant al portal Mi Digi: {page_err}")
                finally:
                    browser.close()

        except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as e:
            self.record_failure(f"Error de Playwright a Mi Digi: {e}")
            self.logger.error(f"Error durant el web scraping de Mi Digi: {e}")

        if downloaded:
            self.record_success()

        return downloaded

    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Parseja el PDF de factura de Digi Mobil."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # 1. Número de factura (ex: Factura: 2026/DIGI/01234 o Número: FA26001234)
        num_factura = extract_regex_first(
            text, r"(?:Factura\s*n[úu]m\.?|N[úu]mero\s*de\s*factura:?)\s*([A-Za-z0-9/_-]+)"
        )
        if not num_factura:
            num_factura = extract_regex_first(text, r"Factura\s+([A-Za-z0-9/_-]{6,20})")

        # 2. Data d'emissió
        data_str = extract_regex_first(
            text, r"(?:Fecha\s*de\s*emisi[óo]n|Data\s*d'emissi[óo]):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"
        )
        data_emissio = parse_date_catalan_spanish(data_str) if data_str else None

        # 3. Import total
        total_eur = extract_float_value(
            text, r"(?:Total\s*a\s*pagar|Total\s*factura|Import\s*total):?\s*(\d+(?:[.,]\d{1,2})?)\s*€?"
        )
        if total_eur is None:
            total_eur = parse_euro_amount(text)

        if total_eur is None:
            return None

        # 4. Període facturat
        periode_raw = extract_regex_first(
            text,
            r"(?:Per[íi]odo\s*de\s*facturaci[óo]n|Per[íi]ode):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:al|a|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        )
        dt_inici: date | None = None
        dt_fi: date | None = None
        if periode_raw:
            parts = re.split(r"\s*(?:al|a|-)\s*", periode_raw)
            if len(parts) == 2:
                dt_inici = parse_date_catalan_spanish(parts[0])
                dt_fi = parse_date_catalan_spanish(parts[1])

        # 5. Dades addicionals de telefonia
        total_gb = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*GB")

        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=num_factura or f"DIGI-{date.today().isoformat()}",
            data_emissio=data_emissio or date.today(),
            periode_inici=dt_inici,
            periode_fi=dt_fi,
            dies_facturats=(dt_fi - dt_inici).days if (dt_inici and dt_fi) else None,
            import_total=float(total_eur),
            total_gb_consumits=total_gb,
        )
