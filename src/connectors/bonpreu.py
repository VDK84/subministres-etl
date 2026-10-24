# pyright: reportMissingImports=false
import logging
import re
import time
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

logger = logging.getLogger("connector.bonpreu")


class BonpreuConnector(BaseConnector):
    """Connector per a Bonpreu Esclat Energia (Electricitat).

    Com que Bonpreu només notifica l'import per correu sense adjuntar el PDF,
    el connector utilitza el correu com a trigger i fa scraping del portal de clients
    per descarregar el PDF oficial i extreure el desglossament horari (Punta/Pla/Vall).
    """

    name = "bonpreu"
    tipus = SubministreType.ELECTRICITAT

    EMAIL_SENDERS: tuple[str, ...] = (
        "bonpreu",
        "energia.bonpreu.cat",
        "bonpreuenergia.cat",
        "factura@energia.bonpreu.cat",
    )

    PORTAL_LOGIN_URL = "https://energia.bonpreuesclat.cat/"

    def is_enabled(self) -> bool:
        return settings.BONPREU_ENABLED

    def run(
        self,
        imap_client: ImapMailboxClient,
        mode: str = "normal",
        limit: int | None = None,
        source: str = "auto",
    ) -> list[FacturaResult]:
        results: list[FacturaResult] = []
        if not self.is_enabled():
            self.report_progress("Connector Bonpreu desactivat per configuració.", status="warning")
            return results

        if self.is_in_cooldown():
            return results

        days_back = 730 if mode == "all" else settings.DAYS_BACK
        fetch_limit = 1 if mode == "latest" else (limit if limit is not None else (None if mode == "all" else 3))

        # 1. Extracció via Email (IMAP)
        if source in ("auto", "email"):
            self.report_progress(f"📧 Connectant a bústia IMAP per Bonpreu (últims {days_back} dies)...")
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
                f"📧 Trobats {len(emails)} correus d'avís i {total_pdfs} factures PDF adjuntes.",
                current=0,
                total=total_pdfs,
            )

            for idx, (em, att) in enumerate(pdf_attachments, start=1):
                self.report_progress(
                    f"📧 Processant PDF adjunt Bonpreu ({att.filename})...",
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

        # 2. Descàrrega des del portal de clients amb Playwright
        if source in ("auto", "portal"):
            if not settings.BONPREU_USER or not settings.BONPREU_PASSWORD:
                msg = "❌ Faltan credencials BONPREU_USER / BONPREU_PASSWORD al Secret per entrar al portal web."
                self.report_progress(msg, status="error")
                if source == "portal":
                    raise RuntimeError(msg)
                return results

            self.report_progress("🌐 Iniciant descàrrega de factures des del portal de Bonpreu Energia...")
            pdf_list = self.fetch_invoices_from_portal(imap_client=imap_client, limit=fetch_limit)
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
        imap_client: ImapMailboxClient | None = None,
        limit: int | None = 3,
        otp_code: str | None = None,
    ) -> list[tuple[bytes, str]]:
        """Autenticació a Bonpreu Energia via Keycloak, suport 2FA (IMAP automàtic + manual) i sessió NAS."""
        downloaded: list[tuple[bytes, str]] = []
        if self.is_in_cooldown():
            return downloaded

        if not HAVE_PLAYWRIGHT or sync_playwright is None:
            self.logger.warning(
                "Playwright no està instal·lat en aquest entorn. Cal instal·lar el paquet per fer scraping."
            )
            return downloaded

        session_dir = Path(settings.NAS_MOUNT_PATH) / ".sessions"
        session_file = session_dir / "bonpreu_session.json"
        try:
            session_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        portal_url = "https://energia.bonpreuesclat.cat/factures"
        try:
            self.report_progress("🌐 Iniciant navegador headless Chromium per a Bonpreu...")
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=settings.HEADLESS_BROWSER)
                try:
                    context_kwargs: dict[str, Any] = {"accept_downloads": True}
                    if session_file.exists():
                        self.report_progress("🌐 Carregant sessió prèvia de Bonpreu des del NAS...")
                        context_kwargs["storage_state"] = str(session_file)

                    context = browser.new_context(**context_kwargs)
                    page = context.new_page()

                    self.report_progress(f"🌐 Obrint portal de factures de Bonpreu ({portal_url})...")
                    page.goto(portal_url, wait_until="domcontentloaded", timeout=settings.SCRAPER_TIMEOUT_MS)
                    page.wait_for_timeout(3000)

                    # 1. Acceptar cookies o eliminar overlay si bloqueja
                    try:
                        page.evaluate(
                            "() => { const el = document.querySelector('#onetrust-consent-sdk'); if (el) el.remove(); }"
                        )
                    except (PlaywrightError, OSError):
                        pass

                    # 2. Si ens redirigeix al Keycloak de Bonpreu, cal fer login
                    if (
                        "keycloak" in page.url.lower()
                        or "login" in page.url.lower()
                        or "protocol/openid" in page.url.lower()
                        or page.query_selector("#kc-form-login")
                        or page.query_selector("input#username")
                    ):
                        self.report_progress(f"🔑 Sessió no activa. Iniciant login per a '{settings.BONPREU_USER}'...")

                        # Eliminar overlay de cookies abans d'interactuar
                        page.evaluate(
                            "() => { const el = document.querySelector('#onetrust-consent-sdk'); if (el) el.remove(); }"
                        )
                        page.wait_for_timeout(500)

                        page.fill("input#username, input[name='username']", settings.BONPREU_USER or "")
                        page.fill("input#password, input[name='password']", settings.BONPREU_PASSWORD or "")

                        with page.expect_navigation(wait_until="domcontentloaded", timeout=settings.SCRAPER_TIMEOUT_MS):
                            page.evaluate(
                                "() => { const f = document.querySelector('#kc-form-login'); if (f) f.submit(); }"
                            )
                        page.wait_for_timeout(3000)

                        # 3. Detecció de pantalla de 2FA (Verifica la teva identitat / kc-email-totp-code-login-form)
                        is_2fa = (
                            "totp" in page.url.lower()
                            or "identitat" in page.title().lower()
                            or page.query_selector("#kc-email-totp-code-login-form") is not None
                            or page.query_selector("input#code") is not None
                        )

                        if is_2fa:
                            self.report_progress("🔐 Bonpreu sol·licita codi 2FA per correu...", status="warning")
                            code_to_use = otp_code

                            # PLA A: Intentar recuperar el codi 2FA automàticament de la bústia IMAP
                            if not code_to_use and imap_client and imap_client.is_configured():
                                self.report_progress("📧 Cercant codi 2FA de Bonpreu a la bústia de correu...")
                                code_to_use = self._fetch_otp_code_from_email(imap_client, timeout_seconds=75)

                            # Si tenim el codi (automàtic o proporcionat per l'usuari)
                            if code_to_use:
                                self.report_progress(f"🔐 Aplicant codi 2FA a Bonpreu: {code_to_use}...")
                                page.fill("input#code", code_to_use)
                                with page.expect_navigation(
                                    wait_until="domcontentloaded", timeout=settings.SCRAPER_TIMEOUT_MS
                                ):
                                    page.evaluate(
                                        "() => { const f = document.querySelector('#kc-email-totp-code-login-form'); if (f) f.submit(); }"
                                    )
                                page.wait_for_timeout(3500)
                            else:
                                msg = "🔐 Bonpreu requereix codi 2FA enviat per correu. Introdueix-lo o torna a provar."
                                self.record_failure(msg)
                                self.report_progress(f"⚠️ {msg}", status="warning")
                                return downloaded

                    # 4. Si hem accedit amb èxit, desem la sessió al NAS
                    if "keycloak" not in page.url.lower():
                        try:
                            context.storage_state(path=str(session_file))
                            self.logger.info("✅ Sessió de Bonpreu desada al NAS per a futures execucions!")
                        except (OSError, RuntimeError):
                            pass

                    # 5. Assegurar que som a la pàgina de factures
                    if "factur" not in page.url.lower():
                        self.report_progress("🌐 Accedint a l'apartat de factures de Bonpreu...")
                        page.goto(portal_url, wait_until="domcontentloaded", timeout=settings.SCRAPER_TIMEOUT_MS)
                        page.wait_for_timeout(3000)

                    # 6. Cerca botons de descàrrega de PDF
                    download_buttons = page.query_selector_all(
                        "button:has-text('Descarregar pdf'), button:has-text('Descarregar'), a[href*='.pdf'], a:has-text('PDF'), button:has-text('PDF')"
                    )
                    total_found = len(download_buttons)
                    max_dl = min(limit, total_found) if limit is not None else total_found
                    self.report_progress(
                        f"🌐 Trobats {total_found} enllaços de descàrrega a Bonpreu. Iniciant {max_dl}...",
                        current=0,
                        total=max_dl,
                    )

                    for i, btn in enumerate(download_buttons[:max_dl], start=1):
                        self.report_progress(
                            f"⬇️ Descarregant factura {i} de {max_dl} del portal Bonpreu...",
                            current=i,
                            total=max_dl,
                        )
                        try:
                            with page.expect_download(timeout=20000) as download_info:
                                page.evaluate("el => el.click()", btn)
                            download = download_info.value
                            path = download.path()
                            if path:
                                with open(path, "rb") as f:
                                    downloaded.append((f.read(), download.suggested_filename or f"bonpreu_{i}.pdf"))
                                self.report_progress(
                                    f"✅ Factura {i}/{max_dl} descarregada correctament!",
                                    current=i,
                                    total=max_dl,
                                )
                        except PlaywrightTimeoutError:
                            self.report_progress(
                                f"⏱️ Timeout (20s) descarregant factura {i}/{max_dl}. Saltant a la següent...",
                                current=i,
                                total=max_dl,
                                status="warning",
                            )
                        except (PlaywrightError, TimeoutError, OSError) as err:
                            self.logger.warning(f"No s'ha pogut descarregar el botó {i}: {err}")

                except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as page_err:
                    self.record_failure(f"Error de navegació a Bonpreu: {page_err}")
                    self.logger.error(f"Error navegant al portal de Bonpreu: {page_err}")
                finally:
                    browser.close()

        except (PlaywrightError, PlaywrightTimeoutError, TimeoutError, OSError, RuntimeError) as e:
            self.record_failure(f"Error de Playwright a Bonpreu: {e}")
            self.logger.error(f"Error durant el web scraping de Bonpreu: {e}")

        if downloaded:
            self.record_success()

        return downloaded

    def _fetch_otp_code_from_email(
        self,
        imap_client: ImapMailboxClient,
        timeout_seconds: int = 75,
    ) -> str | None:
        """Cerca el codi 2FA de 6 dígits enviat per Bonpreu a la bústia IMAP."""
        start_time = time.time()
        self.logger.info(f"Esperant arribada del correu 2FA de Bonpreu (fins a {timeout_seconds}s)...")
        while time.time() - start_time < timeout_seconds:
            emails = imap_client.fetch_recent_emails(
                sender_filters=["bonpreu", "info@bonpreu.cat"],
                days_back=1,
            )
            for em in emails:
                subject_clean = em.subject.lower()
                body_clean = em.body_text
                # Comprovar si és el correu de verificació d'identitat
                if "identitat" in subject_clean or "verificaci" in subject_clean or "código" in subject_clean:
                    match = re.search(r"\b(\d{6})\b", body_clean)
                    if match:
                        code = match.group(1)
                        self.logger.info(f"✅ Codi 2FA de Bonpreu capturat del correu: {code}")
                        return code
            time.sleep(3)
        return None

    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Extreu mètriques detallades del PDF oficial de Bonpreu Esclat Energia."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # 1. Número de factura (ex: Factura núm. 2026E123456)
        num_factura = extract_regex_first(text, r"(?:Factura\s*n[úu]m\.?|N[úu]mero\s*factura:?)\s*([A-Za-z0-9/_-]+)")
        if not num_factura:
            num_factura = extract_regex_first(text, r"Factura\s+([0-9]{4}E[0-9]+)")

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

        # 4. Consums d'electricitat en kWh (Total i per períodes)
        consum_total = extract_float_value(
            text, r"(?:Consum\s*total|Energia\s*consumida|Total\s*consum):?\s*(\d+(?:[.,]\d{1,2})?)\s*kWh"
        )
        p1 = extract_float_value(text, r"(?:Punta|P1)[:\s]+(\d+(?:[.,]\d{1,2})?)\s*kWh")
        p2 = extract_float_value(text, r"(?:Pla|Llano|P2)[:\s]+(\d+(?:[.,]\d{1,2})?)\s*kWh")
        p3 = extract_float_value(text, r"(?:Vall|Valle|P3)[:\s]+(\d+(?:[.,]\d{1,2})?)\s*kWh")

        if consum_total is None and (p1 or p2 or p3):
            consum_total = (p1 or 0.0) + (p2 or 0.0) + (p3 or 0.0)

        # 5. Potència contractada (kW)
        potencia = extract_float_value(text, r"(?:Pot[èe]ncia\s*contractada|Potencia):?\s*(\d+(?:[.,]\d{1,2})?)\s*kW")

        # 6. Període facturat
        dt_inici, dt_fi = extract_date_range(
            text,
            r"(?:Per[íi]ode\s*facturat|Per[íi]ode):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:a|fins\s*a|al|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        )

        # 7. CUPS d'electricitat (ES00...)
        cups = extract_regex_first(text, r"(ES\d{16,20}[A-Za-z]{0,2})")

        return FacturaMetriques(
            proveidor=self.name,
            tipus=self.tipus,
            numero_factura=num_factura or f"BONPREU-{date.today().isoformat()}",
            data_emissio=data_emissio or date.today(),
            periode_inici=dt_inici,
            periode_fi=dt_fi,
            dies_facturats=(dt_fi - dt_inici).days if (dt_inici and dt_fi) else None,
            import_total=total_eur,
            consum_kwh=consum_total,
            consum_kwh_punta=p1,
            consum_kwh_pla=p2,
            consum_kwh_vall=p3,
            potencia_contractada_kw=potencia,
            cups_o_referencia=cups,
        )

    def fetch_available_invoices(self, imap_client: ImapMailboxClient) -> list[dict]:
        """Llista les factures de Bonpreu trobades per avisos de correu i al NAS."""
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
            parsed_amt = parse_euro_amount(em.body_text) or parse_euro_amount(em.subject) or 0.0

            dest_dir = storage.get_destination_dir(self.tipus.value, self.name, any_str)
            existing_pdfs = list(dest_dir.glob(f"{any_str}_{mes_str}_{self.name}_*.pdf"))
            ja_al_nas = len(existing_pdfs) > 0

            inv_num = existing_pdfs[0].stem.split(f"_{self.name}_")[-1] if ja_al_nas else f"AVIS-{any_str}{mes_str}"

            invoices.append(
                {
                    "id": f"{em.msg_id}_bonpreu_{any_str}_{mes_str}",
                    "proveidor": self.name,
                    "tipus": self.tipus.value,
                    "data": d_str,
                    "numero": inv_num,
                    "import": parsed_amt,
                    "subject": em.subject,
                    "exists_in_nas": ja_al_nas,
                }
            )
        return invoices
