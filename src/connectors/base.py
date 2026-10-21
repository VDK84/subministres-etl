# pyright: reportMissingImports=false
import logging
from abc import ABC, abstractmethod
from typing import Any

from src.config import settings
from src.email_client import ImapMailboxClient
from src.influx_writer import influx_writer
from src.models import FacturaMetriques, FacturaResult, SubministreType
from src.storage import storage

logger = logging.getLogger("connector.base")


class BaseConnector(ABC):
    """Classe base abstracta per als connectors de subministraments."""

    name: str = "base"
    tipus: SubministreType = SubministreType.ELECTRICITAT

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"connector.{self.name}")

    @abstractmethod
    def is_enabled(self) -> bool:
        """Determina si el connector està habilitat per configuració."""

    @abstractmethod
    def run(
        self,
        imap_client: ImapMailboxClient,
        mode: str = "normal",
        limit: int | None = None,
    ) -> list[FacturaResult]:
        """Executa l'ETL per a aquest connector: descàrrega, parseig, emmagatzematge i ingesta.

        Modes admesos:
          - "normal": processament segons configuració (days_back per defecte)
          - "latest": cerca exclusivament l'última factura disponible
          - "all": cerca tot l'històric disponible
        """

    def fetch_available_invoices(self, imap_client: ImapMailboxClient) -> list[dict[str, Any]]:
        """Llista les factures disponibles al proveïdor indicant si ja estan al NAS."""
        return []

    def save_and_ingest(
        self,
        pdf_bytes: bytes,
        metriques: FacturaMetriques,
        origen: str,
    ) -> FacturaResult:
        """Desa el fitxer PDF al volum NAS i envia les mètriques a InfluxDB."""
        any_str = str(metriques.data_emissio.year)
        mes_str = f"{metriques.data_emissio.month:02d}"
        clean_num = metriques.numero_factura.replace("/", "_").replace("\\", "_").replace(" ", "")
        filename = f"{any_str}_{mes_str}_{self.name}_{clean_num}.pdf"

        # Comprovar si ja existeix al NAS
        ja_existeix = storage.exists(self.tipus.value, self.name, any_str, filename)

        pdf_saved_path: str | None = None
        nou_descarregat = False

        if not ja_existeix or settings.DRY_RUN:
            path = storage.save_pdf(
                file_bytes=pdf_bytes,
                tipus=self.tipus.value,
                proveidor=self.name,
                any_str=any_str,
                filename=filename,
            )
            pdf_saved_path = str(path)
            nou_descarregat = True
        else:
            self.logger.info(f"La factura {filename} ja existeix al NAS Samba. Ometent sobreescriptura.")
            dest_dir = storage.get_destination_dir(self.tipus.value, self.name, any_str)
            pdf_saved_path = str(dest_dir / filename)

        # Enviar mètriques a InfluxDB
        influx_ok = influx_writer.write_factura_metrics(metriques)
        errors: list[str] = []
        if not influx_ok and settings.INFLUXDB_ENABLED and not settings.DRY_RUN:
            errors.append("Error en enviar mètriques a InfluxDB")

        return FacturaResult(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=metriques.numero_factura,
            data_factura=metriques.data_emissio,
            pdf_path=pdf_saved_path,
            metriques=metriques,
            origen=origen,
            nou_descarregat=nou_descarregat,
            errors=errors,
        )

    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Parseig específic del PDF per extreure les mètriques del proveïdor."""
        return None
