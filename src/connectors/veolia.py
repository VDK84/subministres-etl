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
    ) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            return results

        days_back = 730 if mode == "all" else settings.DAYS_BACK
        self.logger.info(f"Executant connector Veolia (Aigua, mode={mode})...")

        # 1. Extracció via Email (IMAP)
        emails = imap_client.fetch_recent_emails(
            sender_filters=list(self.EMAIL_SENDERS),
            days_back=days_back,
        )
        self.logger.info(f"Trobats {len(emails)} correus potencials de Veolia")

        # Ordenar els correus del més recent al més antic
        emails.sort(key=lambda x: x.date.timestamp() if x.date else 0.0, reverse=True)

        for em in emails:
            for att in em.attachments:
                if not att.filename.lower().endswith(".pdf"):
                    continue

                self.logger.info(f"Processant fitxer adjunt de Veolia: {att.filename} de '{em.subject}'")
                metriques = self.extract_metrics_from_pdf(att.data)
                if not metriques:
                    # Fallback si no troba número o data específica: intentem extreure data del correu
                    metriques = self._fallback_metrics(att.data, em.date.date() if em.date else date.today())

                if metriques:
                    res = self.save_and_ingest(att.data, metriques, origen="email_attachment")
                    results.append(res)

                    if mode == "latest":
                        return results
                    if limit and len(results) >= limit:
                        return results

        if results:
            return results

        # 2. Descàrrega directa des del portal d'Agbar-Veolia si no hi ha PDF per correu
        if settings.VEOLIA_USER and settings.VEOLIA_PASSWORD:
            fetch_limit = 1 if mode == "latest" else (limit or (None if mode == "all" else 3))
            self.logger.info(f"Iniciant descàrrega des del portal web d'Agbar-Veolia (límit: {fetch_limit})...")
            pdf_list = self.fetch_invoices_from_portal(limit=fetch_limit)
            for pdf_bytes, filename in pdf_list:
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
        """Autenticació al portal de clients d'Agbar/Veolia via Playwright i descàrrega de factures."""
        downloaded: list[tuple[bytes, str]] = []
        if self.is_in_cooldown():
            return downloaded

        if not HAVE_PLAYWRIGHT or sync_playwright is None:
            self.logger.warning("Playwright no està instal·lat en aquest entorn.")
            return downloaded

        session_dir = Path(settings.NAS_MOUNT_PATH) / ".sessions"
        session_file = session_dir / "veolia_session.json"
        try:
            session_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=settings.HEADLESS_BROWSER)
                try:
                    context_kwargs: dict[str, Any] = {"accept_downloads": True}
                    if session_file.exists():
                        self.logger.info("Carregant sessió prèvia d'Agbar-Veolia des del NAS...")
                        context_kwargs["storage_state"] = str(session_file)

                    context = browser.new_context(**context_kwargs)
                    page = context.new_page()

                    # 1. Intentem anar directament a l'apartat de factures
                    self.logger.info(f"Obrint factures d'Agbar-Veolia: {self.PORTAL_INVOICES_URL}")
                    page.goto(self.PORTAL_INVOICES_URL, timeout=settings.SCRAPER_TIMEOUT_MS)
                    page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)

                    # 2. Si ens redirigeix al formulari de login, iniciem sessió
                    if "login" in page.url.lower() or page.query_selector("#loginForm"):
                        self.logger.info("Sessió no activa a Agbar-Veolia. Iniciant login...")
                        page.fill("input#login, input[name='_CustomLoginPortlet_login']", settings.VEOLIA_USER or "")
                        page.fill(
                            "input#password, input[name='_CustomLoginPortlet_password']", settings.VEOLIA_PASSWORD or ""
                        )
                        page.click("button#btn-submit, button[type='submit']")
                        page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)

                    # 3. Si hem accedit correctament, desem la sessió al NAS
                    if "login" not in page.url.lower():
                        try:
                            context.storage_state(path=str(session_file))
                            self.logger.info("✅ Sessió d'Agbar-Veolia desada al NAS!")
                        except (OSError, RuntimeError):
                            pass

                    # 4. Navegar a factures si cal
                    if "mis-facturas" not in page.url.lower():
                        page.goto(self.PORTAL_INVOICES_URL, timeout=settings.SCRAPER_TIMEOUT_MS)
                        page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)

                    # 5. Cerca botons de descàrrega de factura digital (segons DOM real d'Agbar)
                    download_buttons = page.query_selector_all(
                        "#descargar-factura-digital a, a:has-text('Descarrega factura digital'), a[href*='descargarFacturaPdf']"
                    )
                    max_dl = limit or 3
                    self.logger.info(
                        f"Trobats {len(download_buttons)} enllaços de descàrrega a Agbar-Veolia (límit: {max_dl})"
                    )

                    for i, btn in enumerate(download_buttons[:max_dl]):
                        try:
                            with page.expect_download(timeout=15000) as download_info:
                                btn.click()
                            download = download_info.value
                            path = download.path()
                            if path:
                                with open(path, "rb") as f:
                                    downloaded.append(
                                        (f.read(), download.suggested_filename or f"veolia_factura_{i}.pdf")
                                    )
                        except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError) as err:
                            self.logger.warning(f"No s'ha pogut descarregar la factura {i}: {err}")

                except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as page_err:
                    self.record_failure(f"Error de navegació a Agbar-Veolia: {page_err}")
                    self.logger.error(f"Error navegant al portal Agbar-Veolia: {page_err}")
                finally:
                    browser.close()

        except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as e:
            self.record_failure(f"Error de Playwright a Agbar-Veolia: {e}")
            self.logger.error(f"Error durant el web scraping d'Agbar-Veolia: {e}")

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
                d_str = em.date.strftime("%Y-%m-%d") if em.date else "Data desconeguda"
                any_str = str(em.date.year) if em.date else str(date.today().year)
                mes_str = f"{em.date.month:02d}" if em.date else "01"

                clean_name = re.sub(r"[^A-Za-z0-9_-]", "_", att.filename.replace(".pdf", "").replace(".PDF", ""))
                expected_filename = f"{any_str}_{mes_str}_{self.name}_{clean_name}.pdf"
                parsed_amt = parse_euro_amount(em.subject) or parse_euro_amount(em.body_text) or 0.0

                ja_al_nas = storage.exists(self.tipus.value, self.name, any_str, expected_filename)
                invoices.append(
                    {
                        "id": f"{em.msg_id}_{att.filename}",
                        "proveidor": self.name,
                        "tipus": self.tipus.value,
                        "data": d_str,
                        "numero": att.filename.replace(".pdf", ""),
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
        periode_raw = extract_regex_first(
            text,
            r"(?:Per[íi]ode\s*facturat|Per[íi]odo):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:a|fins\s*a|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        )
        dt_inici: date | None = None
        dt_fi: date | None = None
        if periode_raw:
            parts = re.split(r"\s*(?:a|fins\s*a|-)\s*", periode_raw)
            if len(parts) == 2:
                dt_inici = parse_date_catalan_spanish(parts[0])
                dt_fi = parse_date_catalan_spanish(parts[1])

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
