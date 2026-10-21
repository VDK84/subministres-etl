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
        emails.sort(key=lambda x: x.date or date.min, reverse=True)

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

        return results

    def fetch_available_invoices(self, imap_client: ImapMailboxClient) -> list[dict]:
        """Llista les factures de Veolia trobades a la bústia i el seu estat al NAS."""
        invoices: list[dict] = []
        if not self.is_enabled():
            return invoices

        emails = imap_client.fetch_recent_emails(sender_filters=list(self.EMAIL_SENDERS), days_back=730)
        emails.sort(key=lambda x: x.date or date.min, reverse=True)

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
