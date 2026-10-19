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

logger = logging.getLogger("connector.veolia")


class VeoliaConnector(BaseConnector):
    """Connector per a Veolia / Aigües de Barcelona (Aigua).

    Extreu factures PDF rebudes per correu com a fitxer adjunt o descarrega del portal.
    """

    name = "veolia"
    tipus = SubministreType.AIGUA

    EMAIL_SENDERS: tuple[str, ...] = ("veolia", "aiguesdebarcelona", "agbar", "suac@veolia.com")

    def is_enabled(self) -> bool:
        return settings.VEOLIA_ENABLED

    def run(self, imap_client: ImapMailboxClient) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            return results

        self.logger.info("Executant connector Veolia (Aigua)...")

        # 1. Extracció via Email (IMAP)
        emails = imap_client.fetch_recent_emails(sender_filters=list(self.EMAIL_SENDERS))
        self.logger.info(f"Trobats {len(emails)} correus potencials de Veolia")

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

        return results

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
        data_str = extract_regex_first(text, r"(?:Data\s*d'emissi[óo]|Fecha\s*emisi[óo]n):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})")
        data_emissio = parse_date_catalan_spanish(data_str) if data_str else None

        # 3. Import total
        total_eur = extract_float_value(text, r"(?:Total\s*factura|Import\s*total|Total\s*a\s*pagar):?\s*(\d+(?:[.,]\d{1,2})?)\s*€?")
        if total_eur is None:
            total_eur = parse_euro_amount(text)

        # Si no tenim com a mínim import total, no és una factura vàlida
        if total_eur is None:
            return None

        # 4. Consum d'aigua en m3
        # Exemples: "Consum facturat: 12 m3" o "Consum del període: 14 m³"
        consum_m3 = extract_float_value(text, r"(?:Consum\s*facturat|Consumo\s*facturado|Consum\s*per[íi]ode):?\s*(\d+(?:[.,]\d{1,2})?)\s*m[3³]")
        if consum_m3 is None:
            consum_m3 = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*m[3³]")

        # 5. Dates del període
        periode_raw = extract_regex_first(text, r"(?:Per[íi]ode\s*facturat|Per[íi]odo):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:a|fins\s*a|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})")
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
