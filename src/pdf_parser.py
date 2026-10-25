# pyright: reportMissingImports=false
import io
import logging
import re
from datetime import date
from typing import Any

from dateutil import parser as date_parser
from pypdf import PdfReader
from pypdf.errors import PdfReadError

logger = logging.getLogger("pdf_parser")


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """Extreu tot el text pla d'un fitxer PDF en memòria."""
    text_parts: list[str] = []
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text_parts.append(extracted)
    except (PdfReadError, OSError, ValueError) as e:
        logger.warning(f"Error extreient text del PDF: {e}")
        return ""
    return "\n".join(text_parts)


def normalize_amount_str(val_str: str) -> float | None:
    """Normalitza una cadena numèrica monetària (ex: '25,00', '9.92', '1.234,56') a float de forma robusta."""
    clean = val_str.strip()
    if not clean:
        return None
    if "," in clean:
        # Format europeu/espanyol estàndard: punts són milers, coma és decimal
        clean = clean.replace(".", "").replace(",", ".")
    elif "." in clean:
        parts = clean.split(".")
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Format float estàndard (ex: 9.92, 25.00): el punt és el separador decimal!
            pass
        elif len(parts) == 2 and len(parts[1]) == 3:
            # Milers sense decimals (ex: 1.000)
            clean = clean.replace(".", "")
        else:
            clean = clean.replace(".", "")

    try:
        return float(clean)
    except ValueError:
        return None


def parse_euro_amount(text: str) -> float | None:
    """Extreu l'import total monetari (ex: 45,67 € o 9.92 €) prioritzant etiquetes de total."""
    # 1. Prioritzar etiquetes explícites de total amb decimals
    total_patterns = [
        r"(?:TOTAL\s+A\s+PAGAR|TOTAL\s+FACTURA|IMPORT\s+TOTAL|IMPORTE\s+TOTAL)[^0-9]{0,40}?(\d{1,4}[.,]\d{2})\s*€",
        r"(\d{1,4}[.,]\d{2})\s*€\s*(?:TOTAL\s+FACTURA|TOTAL\s+A\s+PAGAR)",
        r"TOTAL[:\s]+(\d{1,4}[.,]\d{2})\s*€?",
    ]
    for pat in total_patterns:
        match = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if match:
            val = normalize_amount_str(match.group(1))
            if val is not None:
                return val

    # 2. Fallback: cercar imports amb decimals explícits seguits de €
    matches = re.findall(r"(\d{1,4}[.,]\d{2})\s*€", text)
    if matches:
        # En moltes factures l'import total és l'últim o el més rellevant amb decimals
        return normalize_amount_str(matches[-1])
    return None


def parse_date_catalan_spanish(date_str: str) -> date | None:
    """Intenta parsejar dates típiques en català o castellà (ex: 15/09/2026, 15 d'octubre de 2026, 09 d'oct. 2026)."""
    months_map = {
        "gener": "01",
        "enero": "01",
        "gen": "01",
        "ene": "01",
        "febrer": "02",
        "febrero": "02",
        "feb": "02",
        "març": "03",
        "marzo": "03",
        "mar": "03",
        "abril": "04",
        "abr": "04",
        "maig": "05",
        "mayo": "05",
        "mai": "05",
        "may": "05",
        "juny": "06",
        "junio": "06",
        "jun": "06",
        "juliol": "07",
        "julio": "07",
        "jul": "07",
        "agost": "08",
        "agosto": "08",
        "ago": "08",
        "setembre": "09",
        "septiembre": "09",
        "set": "09",
        "sep": "09",
        "octubre": "10",
        "oct": "10",
        "novembre": "11",
        "noviembre": "11",
        "nov": "11",
        "desembre": "12",
        "diciembre": "12",
        "des": "12",
        "dic": "12",
    }
    clean = date_str.lower().strip()
    # Netejar connectors en català/castellà com "d'", "d’", "de ", "del "
    clean = re.sub(r"\bd\s*['’]\s*", "", clean)
    clean = re.sub(r"\bde\s+", "", clean)
    clean = re.sub(r"\bdel\s+", "", clean)

    for cat_es, num in months_map.items():
        clean = re.sub(rf"\b{cat_es}\.?\b", num, clean)
    clean = re.sub(r"[^\w\s/.-]", " ", clean)

    try:
        dt = date_parser.parse(clean, dayfirst=True)
        return dt.date()
    except (date_parser.ParserError, ValueError, OverflowError):
        return None


def extract_regex_first(text: str, pattern: str, group: int = 1) -> str | None:
    """Cerca un patró regex i retorna el grup capturat o None."""
    m = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    return m.group(group).strip() if m else None


def extract_date_range(text: str, pattern: str) -> tuple[date | None, date | None]:
    """Cerca un patró regex amb dos grups de captura de data i retorna (data_inici, data_fi)."""
    m = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    if m and len(m.groups()) >= 2:
        dt_inici = parse_date_catalan_spanish(m.group(1).strip())
        dt_fi = parse_date_catalan_spanish(m.group(2).strip())
        return dt_inici, dt_fi
    return None, None


def extract_float_value(text: str, pattern: str, group: int = 1) -> float | None:
    """Cerca un valor numèric amb regex i el converteix a float de forma robusta."""
    val = extract_regex_first(text, pattern, group)
    if not val:
        return None
    return normalize_amount_str(val)


class PDFDataExtractor:
    """Utilitat general per extreure dades comunes d'una factura en PDF."""

    @staticmethod
    def extract_metadata(pdf_bytes: bytes) -> dict[str, Any]:
        text = extract_text_from_pdf_bytes(pdf_bytes)
        return {
            "text": text,
            "total_amount": parse_euro_amount(text),
        }
