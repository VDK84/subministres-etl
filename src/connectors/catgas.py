# pyright: reportMissingImports=false
import logging
import re
from datetime import date

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

logger = logging.getLogger("connector.catgas")


class CatgasConnector(BaseConnector):
    """Connector per a Catgas Energia (Gas natural).

    Extreu factures PDF rebudes per correu com a fitxer adjunt i en parseja consums i imports.
    """

    name = "catgas"
    tipus = SubministreType.GAS

    EMAIL_SENDERS: tuple[str, ...] = ("catgas", "catgas.cat", "catgas.es", "facturacio@catgas.cat")

    def is_enabled(self) -> bool:
        return settings.CATGAS_ENABLED

    def run(self, imap_client: ImapMailboxClient) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            return results

        self.logger.info("Executant connector Catgas (Gas natural)...")

        emails = imap_client.fetch_recent_emails(sender_filters=list(self.EMAIL_SENDERS))
        self.logger.info(f"Trobats {len(emails)} correus potencials de Catgas")

        for em in emails:
            for att in em.attachments:
                if not att.filename.lower().endswith(".pdf"):
                    continue

                self.logger.info(f"Processant fitxer adjunt de Catgas: {att.filename} de '{em.subject}'")
                metriques = self.extract_metrics_from_pdf(att.data)
                if not metriques:
                    metriques = self._fallback_metrics(att.data, em.date.date() if em.date else date.today())

                if metriques:
                    res = self.save_and_ingest(att.data, metriques, origen="email_attachment")
                    results.append(res)

        return results

    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Parseja el PDF de Catgas per obtenir dades de consum en kWh/m3 i import."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # 1. Número de factura (ex: Factura núm: FG2026/001234)
        num_factura = extract_regex_first(text, r"(?:Factura\s*n[úu]m\.?|N[úu]mero\s*de\s*factura:?)\s*([A-Za-z0-9/_-]+)")
        if not num_factura:
            num_factura = extract_regex_first(text, r"(FG\d{4}[/-]?\d+)")

        # 2. Data d'emissió
        data_str = extract_regex_first(text, r"(?:Data\s*emissi[óo]|Fecha\s*emisi[óo]n):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})")
        data_emissio = parse_date_catalan_spanish(data_str) if data_str else None

        # 3. Import total
        total_eur = extract_float_value(text, r"(?:Total\s*factura|Import\s*total|Total\s*a\s*pagar):?\s*(\d+(?:[.,]\d{1,2})?)\s*€?")
        if total_eur is None:
            total_eur = parse_euro_amount(text)

        if total_eur is None:
            return None

        # 4. Consum de gas en kWh i m3
        consum_kwh = extract_float_value(text, r"(?:Consum\s*facturat|Consumo|Energia\s*facturada):?\s*(\d+(?:[.,]\d{1,2})?)\s*kWh")
        if consum_kwh is None:
            consum_kwh = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*kWh")

        consum_m3 = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*m[3³]")

        # 5. Dates del període
        periode_raw = extract_regex_first(text, r"(?:Per[íi]ode\s*de\s*facturaci[óo]|Per[íi]ode):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:fins\s*a|a|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})")
        dt_inici: date | None = None
        dt_fi: date | None = None
        if periode_raw:
            parts = re.split(r"\s*(?:fins\s*a|a|-)\s*", periode_raw)
            if len(parts) == 2:
                dt_inici = parse_date_catalan_spanish(parts[0])
                dt_fi = parse_date_catalan_spanish(parts[1])

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
