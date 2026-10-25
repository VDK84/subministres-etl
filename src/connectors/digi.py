# pyright: reportMissingImports=false
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    HAVE_PLAYWRIGHT = True
except ImportError:
    HAVE_PLAYWRIGHT = False
    PlaywrightError = Exception  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore
    sync_playwright = None  # type: ignore

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

logger = logging.getLogger("connector.digi")


def decode_digi_shifted_font(text: str) -> str:
    """Descodifica el text de PDFs antics de Digi que tenen el mapa de caràcters de la font desplaçat."""
    res = []
    for ch in text:
        code = ord(ch)
        if 0x14 <= code <= 0x1D:
            res.append(str(code - 0x14))
        elif 0x00 <= code <= 0x60:
            target = code + 28
            if 32 <= target <= 126:
                res.append(chr(target))
            else:
                res.append(ch)
        elif ch in ("\u019d", "Ɲ"):
            res.append("€")
        else:
            res.append(ch)
    return "".join(res)


class DigiConnector(BaseConnector):
    """Connector per a Digi Mobil (Fibra i Telefonia Mòbil).

    Digi no adjunta la factura en PDF al correu electrònic, només notifica l'import.
    El connector detecta l'avís i connecta al portal 'Mi Digi' per descarregar el PDF oficial.
    """

    name = "digi"
    tipus = SubministreType.TELEFONIA

    EMAIL_SENDERS: tuple[str, ...] = (
        "digimobil.es",
        "factura@digimobil.es",
        "digi",
        "clientes@digimobil.es",
    )

    PORTAL_LOGIN_URL = "https://midigi.digimobil.es"

    def is_enabled(self) -> bool:
        return settings.DIGI_ENABLED

    def run(
        self,
        imap_client: ImapMailboxClient,
        mode: str = "normal",
        limit: int | None = None,
        source: str = "auto",
    ) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            self.report_progress("Connector Digi desactivat per configuració.", status="warning")
            return results

        if self.is_in_cooldown():
            return results

        days_back = 730 if mode == "all" else settings.DAYS_BACK
        fetch_limit = 1 if mode == "latest" else (limit if limit is not None else (None if mode == "all" else 3))

        # 1. Extracció via Email (IMAP)
        if source in ("auto", "email"):
            self.report_progress(f"📧 Connectant a bústia IMAP per Digi (últims {days_back} dies)...")
            emails = imap_client.fetch_recent_emails(
                sender_filters=list(self.EMAIL_SENDERS),
                days_back=days_back,
            )
            emails.sort(key=lambda x: x.date.timestamp() if x.date else 0.0, reverse=True)

            pdf_attachments = [
                (em, att) for em in emails for att in em.attachments if att.filename.lower().endswith(".pdf")
            ]
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
                    f"📧 Processant PDF de Digi ({att.filename})...",
                    current=idx,
                    total=total_pdfs,
                )
                metriques = self.extract_metrics_from_pdf(att.data)
                if metriques:
                    res = self.save_and_ingest(att.data, metriques, origen="email_attachment")
                    results.append(res)

            if source == "email":
                return results
            if results and mode != "all":
                return results

        # 2. Descàrrega des de Mi Digi si no hi ha PDF adjunt
        if source in ("auto", "portal"):
            if not settings.DIGI_USER or not settings.DIGI_PASSWORD:
                msg = "❌ Faltan credencials DIGI_USER / DIGI_PASSWORD al Secret per entrar a Mi Digi."
                self.report_progress(msg, status="error")
                if source == "portal":
                    raise RuntimeError(msg)
                return results

            self.report_progress("🌐 Iniciant descàrrega de factures des del portal Mi Digi...")
            pdf_list = self.fetch_invoices_from_portal(limit=fetch_limit)
            total_dl = len(pdf_list)
            for idx, (pdf_bytes, filename) in enumerate(pdf_list, start=1):
                self.report_progress(
                    f"💾 Analitzant i desant factura {filename} al NAS i InfluxDB...",
                    current=idx,
                    total=total_dl,
                )
                metriques = self.extract_metrics_from_pdf(pdf_bytes)
                if metriques:
                    res = self.save_and_ingest(pdf_bytes, metriques, origen="web_portal_scraping")
                    results.append(res)
                    if mode == "latest":
                        return results

        return results

    def fetch_invoices_from_portal(
        self,
        limit: int | None = 3,
        otp_code: str | None = None,
    ) -> list[tuple[bytes, str]]:
        """Autenticació a Mi Digi amb Playwright, persistència de sessió i suport 2FA."""
        downloaded: list[tuple[bytes, str]] = []
        if self.is_in_cooldown():
            return downloaded

        if not HAVE_PLAYWRIGHT or sync_playwright is None:
            self.logger.warning("Playwright no està instal·lat en aquest entorn.")
            return downloaded

        session_dir = Path(settings.NAS_MOUNT_PATH) / ".sessions"
        session_file = session_dir / "digi_session.json"
        try:
            session_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        portal_url = settings.DIGI_PORTAL_URL
        invoices_url = "https://midigi.digimobil.es/invoice"

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=settings.HEADLESS_BROWSER)
                try:
                    context_kwargs: dict[str, Any] = {"accept_downloads": True}
                    if session_file.exists():
                        self.logger.info("Carregant sessió prèvia de Mi Digi des del NAS...")
                        context_kwargs["storage_state"] = str(session_file)

                    context = browser.new_context(**context_kwargs)
                    page = context.new_page()

                    # 1. Intentem anar directament a la pàgina de factures
                    self.logger.info(f"Obrint Mi Digi: {invoices_url}")
                    page.goto(invoices_url, timeout=settings.SCRAPER_TIMEOUT_MS)
                    page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)

                    # 2. Si ens redirigeix al login, la sessió no és vàlida i cal autenticar
                    if "login" in page.url.lower():
                        self.logger.info(f"Sessió no activa a Mi Digi. Anant a login: {portal_url}")
                        page.goto(portal_url, timeout=settings.SCRAPER_TIMEOUT_MS)
                        page.fill(
                            "input[name='username'], input[type='text'], input[type='email']",
                            settings.DIGI_USER or "",
                        )
                        page.fill("input[type='password'], input[name='password']", settings.DIGI_PASSWORD or "")
                        page.click("button[type='submit'], input[type='submit']")
                        page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)

                        # 3. Detecció de 2FA (SMS / Codi de verificació)
                        is_2fa = (
                            "verif" in page.url.lower()
                            or "security" in page.url.lower()
                            or page.query_selector(
                                "input[name*='code'], input[type='tel'], input[placeholder*='código']"
                            )
                            is not None
                        )

                        if is_2fa:
                            if otp_code:
                                self.logger.info(f"Aplicant codi 2FA proporcionat: {otp_code}")
                                code_input = page.query_selector(
                                    "input[name*='code'], input[type='tel'], input[type='text']"
                                )
                                if code_input:
                                    code_input.fill(otp_code)
                                    page.click(
                                        "button[type='submit'], button:has-text('Verificar'), button:has-text('Continuar')"
                                    )
                                    page.wait_for_load_state("networkidle", timeout=settings.SCRAPER_TIMEOUT_MS)
                            else:
                                self.logger.warning(
                                    "🔐 Mi Digi requereix codi 2FA (SMS/Email). "
                                    "L'estat del connector queda com a 'Pendent de 2FA'. "
                                    "Pots introduir el codi des de la web per validar la sessió."
                                )
                                try:
                                    screenshot_path = session_dir / "digi_2fa_prompt.png"
                                    page.screenshot(path=str(screenshot_path))
                                except (OSError, RuntimeError):
                                    pass
                                return downloaded

                    # 4. Si hem accedit a les factures amb èxit, desem la sessió al NAS
                    if "invoice" in page.url.lower() or "factur" in page.url.lower():
                        try:
                            context.storage_state(path=str(session_file))
                            self.logger.info("✅ Sessió de Mi Digi desada al NAS per a futures consultes!")
                        except (OSError, RuntimeError):
                            pass

                    # 5. Cerca botons de descàrrega de factures (segons el DOM real de Mi DIGI.html)
                    download_buttons = page.query_selector_all(
                        "a[data-gtm-category='invoice'], a:has-text('Descargar'), button:has-text('Descargar'), a[href*='.pdf']"
                    )
                    total_found = len(download_buttons)
                    max_dl = min(limit, total_found) if limit is not None else total_found
                    self.report_progress(
                        f"🌐 Trobats {total_found} enllaços de descàrrega a Mi Digi. Iniciant {max_dl}...",
                        current=0,
                        total=max_dl,
                    )

                    for i, btn in enumerate(download_buttons[:max_dl], start=1):
                        self.report_progress(
                            f"⬇️ Descarregant factura {i} de {max_dl} del portal Mi Digi...",
                            current=i,
                            total=max_dl,
                        )
                        try:
                            with page.expect_download(timeout=15000) as download_info:
                                btn.click()
                            download = download_info.value
                            path = download.path()
                            if path:
                                with open(path, "rb") as f:
                                    downloaded.append(
                                        (f.read(), download.suggested_filename or f"digi_factura_{i}.pdf")
                                    )
                        except PlaywrightTimeoutError:
                            self.report_progress(
                                f"⏱️ Timeout (15s) descarregant factura {i}/{max_dl}. Saltant a la següent...",
                                current=i,
                                total=max_dl,
                                status="warning",
                            )
                        except (PlaywrightError, TimeoutError, OSError) as err:
                            self.logger.warning(f"No s'ha pogut descarregar la factura {i}: {err}")

                except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as page_err:
                    self.record_failure(f"Error de navegació a Mi Digi: {page_err}")
                    self.logger.error(f"Error navegant al portal Mi Digi: {page_err}")
                finally:
                    browser.close()

        except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as e:
            self.record_failure(f"Error de Playwright a Mi Digi: {e}")
            self.logger.error(f"Error durant el web scraping de Mi Digi: {e}")

        if downloaded:
            self.record_success()

        return downloaded

    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Parseja el PDF de factura de Digi Mobil."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # Si el PDF té la codificació de font desplaçada de Digi (patró (+*' o *%'896%)
        if "(+*'" in text or "*%'896%" in text:
            text = decode_digi_shifted_font(text)

        # 1. Número de factura (ex: FACTURA Número: DGFC2210546467)
        num_factura = extract_regex_first(
            text, r"(?:FACTURA\s+N[úuʱ]mero|N[úu]mero\s*de\s*factura|Factura\s*n[úu]m\.?):?\s*([A-Za-z0-9/_-]+)"
        )
        if not num_factura:
            num_factura = extract_regex_first(text, r"\b(DGFC\d{6,15})\b")
        if not num_factura:
            num_factura = extract_regex_first(text, r"Factura\s+([A-Za-z0-9/_-]{6,20})")

        # 2. Període facturat (ex: "16 de septiembre de 2026 a 15 de octubre de 2026" o "16/11/2021 - 15/12/2021")
        dt_inici, dt_fi = extract_date_range(
            text,
            r"(?:Per[íi]odo\s*(?:de\s*)?(?:consumo|facturaci[óo]n)|Per[íi]ode\s*facturat|Per[íi]ode):?\s*"
            r"(\d{1,2}(?:\s*(?:de|d'|/|-)\s*|\s+)[a-zA-ZÀ-ÿ0-9.]+(?:\s*(?:de|del|/|-)\s*|\s+)\d{2,4})"
            r"\s*(?:al|fins\s*a|a|-)\s*"
            r"(\d{1,2}(?:\s*(?:de|d'|/|-)\s*|\s+)[a-zA-ZÀ-ÿ0-9.]+(?:\s*(?:de|del|/|-)\s*|\s+)\d{2,4})",
        )

        # 3. Data d'emissió (suportant tant dd/mm/yyyy com "16 de octubre de 2026")
        data_str = extract_regex_first(
            text,
            r"(?:Fecha\s*(?:de\s*)?emisi[óoʬ]n|Data\s*(?:d')?emissi[óo]|Fecha\s*factura):?\s*\n?\s*"
            r"(\d{1,2}(?:\s*(?:de|d'|/|-)\s*|\s+)[a-zA-ZÀ-ÿ0-9.]+(?:\s*(?:de|del|/|-)\s*|\s+)\d{2,4})",
        )
        data_emissio = parse_date_catalan_spanish(data_str) if data_str else None

        # Si no ha trobat l'etiqueta explícita, usem la data de fi del període facturat
        if not data_emissio and dt_fi:
            data_emissio = dt_fi

        # Si encara no tenim data, busquem qualsevol data explícita al primer terç del document
        if not data_emissio:
            any_date_str = extract_regex_first(
                text[:1500],
                r"\b(\d{1,2}\s+de\s+[a-zA-ZÀ-ÿ]+\s+de\s+20\d{2}|\d{1,2}[/-]\d{1,2}[/-]20\d{2})\b",
            )
            if any_date_str:
                data_emissio = parse_date_catalan_spanish(any_date_str)

        # 4. Import total (cercant específicament els patrons d'import amb coma decimal de Digi)
        total_eur: float | None = None
        digi_total_patterns = [
            r"(?:TOTAL\s+A\s+PAGAR|TOTAL\s+FACTURA|Importe\s+total|Total\s+factura)[^0-9€]{0,60}?(\d{1,3}[.,]\d{2})\s*€",
            r"(\d{1,3}[.,]\d{2})\s*€\s*(?:TOTAL\s+FACTURA|TOTAL\s+A\s+PAGAR)",
            r"TOTAL\s+SERVICIOS[^0-9€]{0,40}?(\d{1,3}[.,]\d{2})\s*€",
        ]
        for pat in digi_total_patterns:
            val = extract_float_value(text, pat)
            if val is not None and 0.5 <= val <= 400.0:
                total_eur = val
                break

        if total_eur is None:
            all_euro_matches = re.findall(r"\b(\d{1,3},\d{2})\s*€", text)
            valid_amounts = []
            for m_str in all_euro_matches:
                v = extract_float_value(m_str, r"(\d{1,3},\d{2})")
                if v is not None and 1.0 <= v <= 400.0:
                    valid_amounts.append(v)
            if valid_amounts:
                total_eur = max(valid_amounts)

        if total_eur is None:
            return None

        # 5. Dades addicionals de telefonia
        total_gb = extract_float_value(text, r"(\d+(?:[.,]\d{1,2})?)\s*GB")

        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=num_factura or f"DIGI-{date.today().isoformat()}",
            data_emissio=data_emissio or date.today(),
            periode_inici=dt_inici,
            periode_fi=dt_fi,
            dies_facturats=(dt_fi - dt_inici).days if (dt_inici and dt_fi) else None,
            import_total=total_eur,
            total_gb_consumits=total_gb,
        )

    def fetch_available_invoices(self, imap_client: ImapMailboxClient) -> list[dict]:
        """Llista les factures de Digi trobades a la bústia i al NAS."""
        invoices: list[dict] = []
        if not self.is_enabled():
            return invoices

        from src.storage import storage

        emails = imap_client.fetch_recent_emails(sender_filters=list(self.EMAIL_SENDERS), days_back=730)
        emails.sort(key=lambda x: x.date.timestamp() if x.date else 0.0, reverse=True)

        for em in emails:
            d_str = em.date.strftime("%Y-%m-%d") if em.date else "Data desconeguda"
            any_str = str(em.date.year) if em.date else str(date.today().year)
            mes_str = f"{em.date.month:02d}" if em.date else "01"

            # 1. Si porta PDF adjunt
            pdf_found = False
            for att in em.attachments:
                if att.filename.lower().endswith(".pdf"):
                    pdf_found = True
                    metriques = self.extract_metrics_from_pdf(att.data)
                    if metriques:
                        clean_num = metriques.numero_factura.replace("/", "_").replace("\\", "_").replace(" ", "")
                        exp_name = f"{any_str}_{mes_str}_{self.name}_{clean_num}.pdf"
                        ja_al_nas = storage.exists(self.tipus.value, self.name, any_str, exp_name)
                        invoices.append(
                            {
                                "id": f"{em.msg_id}_{att.filename}",
                                "proveidor": self.name,
                                "tipus": self.tipus.value,
                                "data": str(metriques.data_emissio),
                                "numero": metriques.numero_factura,
                                "import": metriques.import_total,
                                "subject": em.subject,
                                "exists_in_nas": ja_al_nas,
                            }
                        )
                    break

            if not pdf_found:
                parsed_amt = parse_euro_amount(em.body_text) or parse_euro_amount(em.subject) or 0.0
                dest_dir = storage.get_destination_dir(self.tipus.value, self.name, any_str)
                existing_pdfs = list(dest_dir.glob(f"{any_str}_{mes_str}_{self.name}_*.pdf"))
                ja_al_nas = len(existing_pdfs) > 0
                inv_num = existing_pdfs[0].stem.split(f"_{self.name}_")[-1] if ja_al_nas else f"AVIS-{any_str}{mes_str}"

                invoices.append(
                    {
                        "id": f"{em.msg_id}_digi_{any_str}_{mes_str}",
                        "proveidor": self.name,
                        "tipus": self.tipus.value,
                        "data": d_str,
                        "numero": inv_num,
                        "import": parsed_amt,
                        "subject": em.subject,
                        "exists_in_nas": ja_al_nas,
                    }
                )

        # Afegir també les factures que ja tenim al volum NAS (descarregades del portal web)
        nas_root = Path(settings.NAS_MOUNT_PATH) / self.tipus.value / self.name
        existing_nums = {str(inv.get("numero", "")).lower() for inv in invoices}

        if nas_root.exists():
            for pdf_path in sorted(nas_root.glob("**/*.pdf"), reverse=True):
                stem = pdf_path.stem
                parts = stem.split(f"_{self.name}_")
                num = parts[-1] if len(parts) > 1 else stem
                if num.lower() not in existing_nums:
                    existing_nums.add(num.lower())
                    any_folder = pdf_path.parent.name
                    invoices.append(
                        {
                            "id": f"nas_{stem}",
                            "proveidor": self.name,
                            "tipus": self.tipus.value,
                            "data": f"{any_folder}-01-01",
                            "numero": num,
                            "import": 0.0,
                            "subject": f"Factura {num} al NAS",
                            "exists_in_nas": True,
                        }
                    )
        return invoices
