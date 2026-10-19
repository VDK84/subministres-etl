# pyright: reportMissingImports=false
import argparse
import logging
import sys

from rich.console import Console
from rich.table import Table

from src.config import settings
from src.connectors import (
    BaseConnector,
    BonpreuConnector,
    CatgasConnector,
    DigiConnector,
    VeoliaConnector,
)
from src.email_client import imap_client
from src.models import FacturaResult

console = Console()


def configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def print_summary_table(results: list[FacturaResult]) -> None:
    table = Table(title="Resum Factures de Subministraments Processades")
    table.add_column("Proveïdor", style="cyan", no_wrap=True)
    table.add_column("Tipus", style="magenta")
    table.add_column("Núm. Factura", style="green")
    table.add_column("Data", style="yellow")
    table.add_column("Import (€)", justify="right", style="bold")
    table.add_column("Origen", style="blue")
    table.add_column("Desat NAS", justify="center")
    table.add_column("Estat", style="bold")

    for r in results:
        import_str = f"{r.metriques.import_total:.2f} €" if r.metriques else "N/A"
        data_str = r.data_factura.isoformat()
        nas_status = "Nou" if r.nou_descarregat else ("Existent" if r.pdf_path else "Sense PDF")
        status = "[green]OK[/green]" if not r.errors else f"[red]Error: {', '.join(r.errors)}[/red]"

        table.add_row(
            r.proveidor.upper(),
            r.tipus.value,
            r.numero_factura,
            data_str,
            import_str,
            r.origen,
            nas_status,
            status,
        )

    console.print(table)


def run_etl(connector_filter: str | None = None) -> int:
    logger = logging.getLogger("etl_main")
    logger.info("Iniciant cicle ETL de factures de subministraments...")

    all_connectors: list[BaseConnector] = [
        VeoliaConnector(),
        CatgasConnector(),
        BonpreuConnector(),
        DigiConnector(),
    ]

    if connector_filter:
        connectors_to_run = [c for c in all_connectors if c.name.lower() == connector_filter.lower()]
        if not connectors_to_run:
            logger.error(f"Connector '{connector_filter}' no trobat.")
            return 1
    else:
        connectors_to_run = [c for c in all_connectors if c.is_enabled()]

    logger.info(f"Connectors actius a executar: {[c.name for c in connectors_to_run]}")

    all_results: list[FacturaResult] = []
    has_errors = False

    for connector in connectors_to_run:
        logger.info(f"--- Iniciant processament: {connector.name.upper()} ({connector.tipus.value}) ---")
        try:
            results = connector.run(imap_client)
            all_results.extend(results)
            for res in results:
                if res.errors:
                    has_errors = True
        except (OSError, RuntimeError, ValueError) as e:
            logger.error(f"Error crític executant el connector {connector.name}: {e}")
            has_errors = True

    if all_results:
        print_summary_table(all_results)
    else:
        logger.info("No s'ha trobat cap factura nova en el període especificat.")

    logger.info("Cicle ETL finalitzat.")
    return 1 if has_errors else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL Subministres - Extracció de factures per a NAS i InfluxDB")
    parser.add_argument("--connector", type=str, help="Executar només un connector específic (veolia, catgas, bonpreu, digi)")
    parser.add_argument("--days-back", type=int, help="Nombre de dies enrere per buscar correus/factures")
    parser.add_argument("--dry-run", action="store_true", help="Simulació: no guarda fitxers ni escriu a InfluxDB")
    parser.add_argument("--log-level", type=str, default="INFO", help="Nivell de logging (DEBUG, INFO, WARNING, ERROR)")

    args = parser.parse_args()

    if args.dry_run:
        settings.DRY_RUN = True
    if args.days_back:
        settings.DAYS_BACK = args.days_back
    if args.log_level:
        settings.LOG_LEVEL = args.log_level

    configure_logging(settings.LOG_LEVEL)
    sys.exit(run_etl(connector_filter=args.connector))


if __name__ == "__main__":
    main()
