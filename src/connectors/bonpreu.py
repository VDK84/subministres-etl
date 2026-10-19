# pyright: reportMissingImports=false
from datetime import date
import logging
import re

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

logger = logging.getLogger("connector.bonpreu")


class BonpreuConnector(BaseConnector):
    """Connector per a Bonpreu Esclat Energia (Electricitat).

    Com que Bonpreu només notifica l'import per correu sense adjuntar el PDF,
    el connector utilitza el correu com a trigger i fa scraping del portal de clients
    per descarregar el PDF oficial i extreure el desglossament horari (Punta/Pla/Vall).
    """

    name = "bonpreu"
    tipus = SubministreType.ELECTRICITAT

    EMAIL_SENDERS: tuple[str, ...] = (
        "bonpreu",
        "energia.bonpreu.cat",
        "bonpreuenergia.cat",
        "factura@energia.bonpreu.cat",
    )

    PORTAL_LOGIN_URL = "https://clients.energia.bonpreu.cat/login"

    def is_enabled(self) -> bool:
        return settings.BONPREU_ENABLED

    def run(self, imap_client: ImapMailboxClient) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            return results

        self.logger.info("Executant connector Bonpreu Esclat Energia (Electricitat)...")

        # 1. Analitzar correus d'avís de factura
        emails = imap_client.fetch_recent_emails(sender_filters=list(self.EMAIL_SENDERS))
        self.logger.info(f"Trobats {len(emails)} correus d'avís de Bonpreu Energia")

        trobat_avis = False
        import_avis: float | None = None
        data_avis: date | None = None

        for em in emails:
            trobat_avis = True
            # Intentem extreure l'import anunciat al correu (ex: "import de 64,23 €")
            parsed_amt = parse_euro_amount(em.body_text) or parse_euro_amount(em.subject)
            if parsed_amt is not None and import_avis is None:
                import_avis = parsed_amt
            if em.date and data_avis is None:
                data_avis = em.date.date()

            # Comprovació de seguretat: si per cas un dia Bonpreu adjunta el PDF directament
            for att in em.attachments:
                if att.filename.lower().endswith(".pdf"):
                    self.logger.info(f"Detectat PDF adjunt directament a correu Bonpreu: {att.filename}")
                    metriques = self.extract_metrics_from_pdf(att.data)
                    if metriques:
                        res = self.save_and_ingest(att.data, metriques, origen="email_attachment")
                        results.append(res)

        if results:
            return results

        # 2. Si no hi ha PDF adjunt per correu, procedim a la descàrrega des del portal de clients
        if not settings.BONPREU_USER or not settings.BONPREU_PASSWORD:
            self.logger.warning(
                "Credencials de portal Bonpreu (BONPREU_USER / BONPREU_PASSWORD) no definides. "
                "No es pot fer scraping del portal per obtenir el PDF."
            )
            if trobat_avis and import_avis is not None:
                # Creem mètrica mínima d'avís si tenim l'import del correu
                data_final = data_avis or date.today()
                metriques_avis = FacturaMetriques(
                    proveidor=self.name,
                    tipus=self.tipus,
                    numero_factura=f"BONPREU-AVIS-{data_final.strftime('%Y%m')}",
                    data_emissio=data_final,
                    import_total=import_avis,
                )
                influx_ok = self.save_and_ingest(b"", metriques_avis, origen="email_notification")
                results.append(influx_ok)
            return results

        self.logger.info("Iniciant descàrrega de factures des del portal de Bonpreu Energia...")
        pdf_list = self.fetch_invoices_from_portal()
        for pdf_bytes, filename in pdf_list:
            metriques = self.extract_metrics_from_pdf(pdf_bytes)
            if metriques:
                res = self.save_and_ingest(pdf_bytes, metriques, origen="web_portal_scraping")
                results.append(res)

        return results

    def fetch_invoices_from_portal(self) -> list[tuple[bytes, str]]:
        """Autenticació al portal de Bonpreu via Playwright headless per descarregar factures PDF."""
        downloaded: list[tuple[bytes, str]] = []
        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import sync_playwright

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=settings.HEADLESS_BROWSER)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()

                self.logger.info(f"Obrint pàgina d'inici de sessió de Bonpreu: {self.PORTAL_LOGIN_URL}")
                page.goto(self.PORTAL_LOGIN_URL, timeout=settings.SCRAPER_TIMEOUT_MS)

                # Omplir formulari de credencials
                # Selectors típics de formulari (email / dni i contrasenya)
                page.fill(
                    "input[type='email'], input[name='username'], input[name='login']", settings.BONPREU_USER or ""
                )
                page.fill("input[type='password'], input[name='password']", settings.BONPREU_PASSWORD or "")
                page.click("button[type='submit'], input[type='submit']")

                page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)

                # Navegar a l'apartat de factures si cal
                if "factur" not in page.url.lower():
                    # Cercar enllaç o pestanya de factures
                    factura_link = page.query_selector("a[href*='factur'], button:has-text('Factur')")
                    if factura_link:
                        factura_link.click()
                        page.wait_for_load_state("networkidle")

                # Cerca botons de descàrrega de PDF
                download_buttons = page.query_selector_all(
                    "a[href*='.pdf'], button:has-text('Descarregar'), a:has-text('PDF')"
                )
                self.logger.info(f"Trobats {len(download_buttons)} enllaços de descàrrega de factura")

                for i, btn in enumerate(download_buttons[:3]):  # Limitem a les últimes 3
                    try:
                        with page.expect_download(timeout=15000) as download_info:
                            btn.click()
                        download = download_info.value
                        path = download.path()
                        if path:
                            with open(path, "rb") as f:
                                downloaded.append((f.read(), download.suggested_filename or f"bonpreu_{i}.pdf"))
                    except (PlaywrightError, TimeoutError, OSError) as err:
                        self.logger.warning(f"No s'ha pogut descarregar el botó {i}: {err}")

                browser.close()
        except ImportError:
            self.logger.warning(
                "Playwright no està instal·lat en aquest entorn. Cal instal·lar el paquet per fer scraping."
            )
        except (TimeoutError, OSError, RuntimeError) as e:
            self.logger.error(f"Error durant el web scraping de Bonpreu: {e}")

        return downloaded

    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Extreu mètriques detallades del PDF oficial de Bonpreu Esclat Energia."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # 1. Número de factura (ex: Factura núm. 2026E123456)
        num_factura = extract_regex_first(text, r"(?:Factura\s*n[úu]m\.?|N[úu]mero\s*factura:?)\s*([A-Za-z0-9/_-]+)")
        if not num_factura:
            num_factura = extract_regex_first(text, r"Factura\s+([0-9]{4}E[0-9]+)")

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

        # 4. Consums d'electricitat en kWh (Total i per períodes)
        consum_total = extract_float_value(
            text, r"(?:Consum\s*total|Energia\s*consumida|Total\s*consum):?\s*(\d+(?:[.,]\d{1,2})?)\s*kWh"
        )
        p1 = extract_float_value(text, r"(?:Punta|P1)[:\s]+(\d+(?:[.,]\d{1,2})?)\s*kWh")
        p2 = extract_float_value(text, r"(?:Pla|Llano|P2)[:\s]+(\d+(?:[.,]\d{1,2})?)\s*kWh")
        p3 = extract_float_value(text, r"(?:Vall|Valle|P3)[:\s]+(\d+(?:[.,]\d{1,2})?)\s*kWh")

        if consum_total is None and (p1 or p2 or p3):
            consum_total = (p1 or 0.0) + (p2 or 0.0) + (p3 or 0.0)

        # 5. Potència contractada (kW)
        potencia = extract_float_value(text, r"(?:Pot[èe]ncia\s*contractada|Potencia):?\s*(\d+(?:[.,]\d{1,2})?)\s*kW")

        # 6. Període facturat
        periode_raw = extract_regex_first(
            text,
            r"(?:Per[íi]ode\s*facturat|Per[íi]ode):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:a|fins\s*a|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        )
        dt_inici: date | None = None
        dt_fi: date | None = None
        if periode_raw:
            parts = re.split(r"\s*(?:a|fins\s*a|-)\s*", periode_raw)
            if len(parts) == 2:
                dt_inici = parse_date_catalan_spanish(parts[0])
                dt_fi = parse_date_catalan_spanish(parts[1])

        # 7. CUPS d'electricitat (ES00...)
        cups = extract_regex_first(text, r"(ES\d{16,20}[A-Za-z]{0,2})")

        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=num_factura or f"BONPREU-{date.today().isoformat()}",
            data_emissio=data_emissio or date.today(),
            periode_inici=dt_inici,
            periode_fi=dt_fi,
            dies_facturats=(dt_fi - dt_inici).days if (dt_inici and dt_fi) else None,
            import_total=float(total_eur),
            consum_kwh=consum_total,
            consum_kwh_punta=p1,
            consum_kwh_pla=p2,
            consum_kwh_vall=p3,
            potencia_contractada_kw=potencia,
            cups_o_referencia=cups,
        )
