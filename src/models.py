# pyright: reportMissingImports=false
from datetime import date
from enum import Enum

from pydantic import BaseModel, Field


class SubministreType(str, Enum):
    AIGUA = "aigua"
    GAS = "gas"
    ELECTRICITAT = "electricitat"
    TELEFONIA = "telefonia"


class FacturaMetriques(BaseModel):
    """Mètriques extretes d'una factura per ser guardades a InfluxDB."""
    proveidor: str
    tipus: SubministreType
    numero_factura: str
    data_emissio: date
    periode_inici: date | None = None
    periode_fi: date | None = None
    dies_facturats: int | None = None
    
    # Imports monetaris (€)
    import_total: float
    import_base: float | None = None
    import_iva: float | None = None
    
    # Consums i dades específiques
    consum_m3: float | None = None             # Per a aigua o gas (m³)
    consum_kwh: float | None = None            # Per a gas o electricitat (kWh)
    consum_kwh_punta: float | None = None      # P1 (Llum)
    consum_kwh_pla: float | None = None        # P2 (Llum)
    consum_kwh_vall: float | None = None       # P3 (Llum)
    potencia_contractada_kw: float | None = None
    
    # Telefonia
    total_gb_consumits: float | None = None
    num_linies_mobils: int | None = None
    
    # Metadades addicionals
    cups_o_referencia: str | None = None
    titular: str | None = None
    tags_extra: dict[str, str] = Field(default_factory=dict)
    fields_extra: dict[str, float] = Field(default_factory=dict)


class FacturaResult(BaseModel):
    """Resultat del processament d'una factura."""
    proveidor: str
    tipus: SubministreType
    numero_factura: str
    data_factura: date
    pdf_path: str | None = None
    metriques: FacturaMetriques | None = None
    origen: str  # 'email_attachment' o 'web_portal_scraping'
    nou_descarregat: bool = False
    errors: list[str] = Field(default_factory=list)
