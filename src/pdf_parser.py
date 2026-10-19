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


def parse_euro_amount(text: str) -> float | None:
    """Extreu el primer import en format monetari (ex: 45,67 € o 45.67€) i el converteix a float."""
    match = re.search(r"(\d+(?:[.,]\d{1,2})?)\s*€", text)
    if not match:
        match = re.search(r"TOTAL[:\s]+(\d+(?:[.,]\d{1,2})?)", text, re.IGNORECASE)
    if match:
        val_str = match.group(1).replace(".", "").replace(",", ".")
        try:
            return float(val_str)
        except ValueError:
            return None
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


def extract_float_value(text: str, pattern: str, group: int = 1) -> float | None:
    """Cerca un valor numèric amb regex i el converteix a float."""
    val = extract_regex_first(text, pattern, group)
    if not val:
        return None
    clean_val = val.replace(".", "").replace(",", ".") if "," in val else val
    try:
        return float(clean_val)
    except ValueError:
        return None


class PDFDataExtractor:
    """Utilitat general per extreure dades comunes d'una factura en PDF."""

    @staticmethod
    def extract_metadata(pdf_bytes: bytes) -> dict[str, Any]:
        text = extract_text_from_pdf_bytes(pdf_bytes)
        return {
            "text": text,
            "total_amount": parse_euro_amount(text),
        }
