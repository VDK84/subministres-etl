# pyright: reportMissingImports=false
import io
import re
from datetime import date
from typing import Any

from dateutil import parser as date_parser
from pypdf import PdfReader
from pypdf.errors import PdfReadError


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
        text_parts.append(f"Error extracting text: {e}")
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
    """Extreu el primer import en format monetari (ex: 45,67 € o 9.92€) i el converteix a float."""
    match = re.search(r"(\d+(?:[.,]\d{1,2})?)\s*€", text)
    if not match:
        match = re.search(r"TOTAL[:\s]+(\d+(?:[.,]\d{1,2})?)", text, re.IGNORECASE)
    if match:
        return normalize_amount_str(match.group(1))
    return None


def parse_date_catalan_spanish(date_str: str) -> date | None:
    """Intenta parsejar dates típiques en català o castellà (ex: 15/09/2026, 15 d'octubre de 2026)."""
    months_map = {
        "gener": "01",
        "enero": "01",
        "febrer": "02",
        "febrero": "02",
        "març": "03",
        "marzo": "03",
        "abril": "04",
        "maig": "05",
        "mayo": "05",
        "juny": "06",
        "junio": "06",
        "juliol": "07",
        "julio": "07",
        "agost": "08",
        "agosto": "08",
        "setembre": "09",
        "septiembre": "09",
        "octubre": "10",
        "novembre": "11",
        "noviembre": "11",
        "desembre": "12",
        "diciembre": "12",
    }
    clean = date_str.lower()
    for cat_es, num in months_map.items():
        clean = re.sub(rf"\b{cat_es}\b", num, clean)
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
