# pyright: reportMissingImports=false
import logging
import re
from datetime import date

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

    Extreu factures PDF rebudes per correu com a fitxer adjunt i en parseja consums i imports.
    """

    name = "catgas"
    tipus = SubministreType.GAS

    EMAIL_SENDERS: tuple[str, ...] = ("catgas", "catgas.cat", "catgas.es", "facturacio@catgas.cat")

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

        if source == "portal":
            self.report_progress(
                "⚠️ Catgas només rep factures per Correu IMAP (no disposa de scraper web). Cercant al correu...",
                status="warning",
            )

        days_back = 730 if mode == "all" else settings.DAYS_BACK
        self.report_progress(f"📧 Connectant a bústia IMAP per Catgas (últims {days_back} dies)...")

        emails = imap_client.fetch_recent_emails(
            sender_filters=list(self.EMAIL_SENDERS),
            days_back=days_back,
        )
        emails.sort(key=lambda x: x.date.timestamp() if x.date else 0.0, reverse=True)

        pdf_attachments = [
            (em, att) for em in emails for att in em.attachments if att.filename.lower().endswith(".pdf")
        ]
        fetch_limit = 1 if mode == "latest" else (limit if limit is not None else (None if mode == "all" else 3))
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

        return results

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
