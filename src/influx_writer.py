# pyright: reportMissingImports=false
import csv
import io
import logging
from datetime import UTC, datetime, time

import requests
import urllib3.exceptions
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from src.config import settings
from src.models import FacturaMetriques

logger = logging.getLogger("influx_writer")


class InfluxMetricsWriter:
    """Gestiona l'escriptura de dades de facturació a InfluxDB v2."""

    def __init__(self) -> None:
        self.url = settings.INFLUXDB_URL
        self.token = settings.INFLUXDB_TOKEN
        self.org = settings.INFLUXDB_ORG
        self.bucket = settings.INFLUXDB_BUCKET

    def is_configured(self) -> bool:
        return bool(settings.INFLUXDB_ENABLED and self.url and self.token and self.bucket)

    def write_factura_metrics(self, metriques: FacturaMetriques) -> bool:
        """Escriu les mètriques d'una factura com a Point de sèrie temporal a InfluxDB."""
        if not self.is_configured():
            logger.info("InfluxDB no està configurat o està desactivat. Ometent escriptura de mètriques.")
            return False

        if settings.DRY_RUN:
            logger.info(
                f"[DRY-RUN] S'escriurien mètriques a InfluxDB per {metriques.proveidor} "
                f"({metriques.numero_factura}) -> Total: {metriques.import_total} €"
            )
            return True

        # Convertim la data de la factura a datetime a migdia UTC per a la sèrie temporal
        dt = datetime.combine(metriques.data_emissio, time(12, 0, 0), tzinfo=UTC)

        point = (
            Point("factures_subministres")
            .tag("proveidor", metriques.proveidor)
            .tag("tipus", metriques.tipus.value)
            .tag("numero_factura", metriques.numero_factura)
            .tag("any", str(metriques.data_emissio.year))
            .tag("mes", f"{metriques.data_emissio.month:02d}")
            .field("import_total", float(metriques.import_total))
            .time(dt)
        )

        if metriques.titular:
            point.tag("titular", metriques.titular)
        if metriques.cups_o_referencia:
            point.tag("referencia", metriques.cups_o_referencia)

        # Camps opcionals monetaris
        if metriques.import_base is not None:
            point.field("import_base", float(metriques.import_base))
        if metriques.import_iva is not None:
            point.field("import_iva", float(metriques.import_iva))
        if metriques.dies_facturats is not None:
            point.field("dies_facturats", int(metriques.dies_facturats))

        # Camps opcionals de consum
        if metriques.consum_m3 is not None:
            point.field("consum_m3", float(metriques.consum_m3))
        if metriques.consum_kwh is not None:
            point.field("consum_kwh", float(metriques.consum_kwh))
        if metriques.consum_kwh_punta is not None:
            point.field("consum_kwh_punta", float(metriques.consum_kwh_punta))
        if metriques.consum_kwh_pla is not None:
            point.field("consum_kwh_pla", float(metriques.consum_kwh_pla))
        if metriques.consum_kwh_vall is not None:
            point.field("consum_kwh_vall", float(metriques.consum_kwh_vall))
        if metriques.potencia_contractada_kw is not None:
            point.field("potencia_contractada_kw", float(metriques.potencia_contractada_kw))

        # Camps telefonia
        if metriques.total_gb_consumits is not None:
            point.field("total_gb_consumits", float(metriques.total_gb_consumits))
        if metriques.num_linies_mobils is not None:
            point.field("num_linies_mobils", int(metriques.num_linies_mobils))

        # Tags i camps addicionals
        for k, v in metriques.tags_extra.items():
            point.tag(k, v)
        for k, num in metriques.fields_extra.items():
            point.field(k, float(num))

        try:
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
                write_api.write(bucket=self.bucket, org=self.org, record=point)
                logger.info(
                    f"Mètriques enviades a InfluxDB [{self.bucket}] per factura {metriques.numero_factura} "
                    f"de {metriques.proveidor}"
                )
                return True
        except (InfluxDBError, urllib3.exceptions.HTTPError, OSError) as e:
            logger.error(f"Error escrivint a InfluxDB: {e}")
            return False

    def delete_factura_metrics(self, proveidor: str, numero_factura: str | None = None) -> bool:
        """Elimina mètriques d'una factura a InfluxDB per evitar dades errònies a Grafana."""
        if not self.is_configured():
            return False

        if settings.DRY_RUN:
            logger.info(f"[DRY-RUN] S'eliminarien mètriques a InfluxDB per {proveidor} ({numero_factura})")
            return True

        predicate = f'_measurement="factures_subministres" and proveidor="{proveidor.lower()}"'
        if numero_factura:
            predicate += f' and numero_factura="{numero_factura}"'

        try:
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                delete_api = client.delete_api()
                delete_api.delete(
                    start="1970-01-01T00:00:00Z",
                    stop="2035-01-01T00:00:00Z",
                    predicate=predicate,
                    bucket=self.bucket,
                    org=self.org,
                )
                logger.info(f"Mètriques eliminades a InfluxDB per {proveidor} ({numero_factura or 'totes'}).")
                return True
        except (InfluxDBError, urllib3.exceptions.HTTPError, OSError) as err:
            logger.warning(f"Error eliminant mètriques d'InfluxDB: {err}")
            return False

    def get_factures_amounts(self) -> dict[str, float]:
        """Recupera un mapa {numero_factura: import_total} d'InfluxDB mitjançant l'API REST."""
        if not self.is_configured():
            return {}

        url = f"{self.url}/api/v2/query?org={self.org}"
        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        }
        flux_body = (
            f'from(bucket: "{self.bucket}") '
            "|> range(start: -10y) "
            '|> filter(fn: (r) => r["_measurement"] == "factures_subministres" and r["_field"] == "import_total")'
        )
        result_map: dict[str, float] = {}
        try:
            resp = requests.post(url, headers=headers, data=flux_body, timeout=4)
            if resp.status_code == 200:
                reader = csv.DictReader(io.StringIO(resp.text))
                for row in reader:
                    num = row.get("numero_factura")
                    val_str = row.get("_value")
                    if num and val_str:
                        try:
                            result_map[str(num).strip().lower()] = float(val_str)
                        except ValueError:
                            continue
        except (OSError, requests.exceptions.RequestException) as err:
            logger.debug(f"No s'ha pogut obtenir mapa d'imports d'InfluxDB via REST: {err}")
        return result_map


influx_writer = InfluxMetricsWriter()
