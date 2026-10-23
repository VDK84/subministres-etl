# 🛠️ Pla d'Acció i Refactorització per a l'Agent IA (`subministres-etl`)

Aquest document conté les instruccions detallades, correccions de *bugs*, millores de seguretat i optimitzacions de rendiment que cal aplicar al repositori `subministres-etl`. 

> **Instruccions per a l'Agent:** Aplica les tasques següents en ordre. Verifica que tots els tests o comprovacions de tipus (`ruff`, `pyright`) continuïn passant després de cada bloc de canvis.

### 📊 Estat d'Execució
- **🔴 BLOC 1 (Bugs Lògics i Connectors):** ✅ Aplicat i verificat (v0.14.0)
- **🛡️ BLOC 2 (Seguretat, XSS, Path Traversal & 50MB Limit):** ✅ Aplicat i verificat (v0.14.0)
- **⚡ BLOC 3 (Asincronia, Event Loop & IMAP Headers):** ✅ Aplicat i verificat (v0.15.0)
- **🧹 BLOC 4 (Lifespan, Neteja de Dependències & Helm CronJob):** ✅ Aplicat i verificat (v0.15.0)

---

## 🔴 BLOC 1: Correcció d'Errors Lògics i Bugs Crítics (✅ Aplicat)

### 1.1. Arreglar l'extracció de dates del període facturat (Regex)
* **Fitxers afectats:** `src/pdf_parser.py`, `src/connectors/bonpreu.py`, `src/connectors/catgas.py`, `src/connectors/digi.py`, `src/connectors/veolia.py`
* **Problema:** `extract_regex_first` només retorna `m.group(1)`. Les expressions regulars dels connectors tenen 2 grups de captura per a la data d'inici i la data de fi, de manera que la segona data es perd sempre i `dt_inici`, `dt_fi` i `dies_facturats` sempre són `None`.
* **Solució:** Crea una funció dedicada `extract_date_range` a `src/pdf_parser.py` i utilitza-la als 4 connectors.

**Afegir a `src/pdf_parser.py`:**
```python
def extract_date_range(text: str, pattern: str) -> tuple[date | None, date | None]:
    """Cerca un patró regex amb dos grups de captura de data i retorna (data_inici, data_fi)."""
    m = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    if m and len(m.groups()) >= 2:
        dt_inici = parse_date_catalan_spanish(m.group(1).strip())
        dt_fi = parse_date_catalan_spanish(m.group(2).strip())
        return dt_inici, dt_fi
    return None, None
```

**Reemplaçar als 4 connectors (`bonpreu.py`, `catgas.py`, `digi.py`, `veolia.py`):**
```python
# Exemple per a bonpreu.py / veolia.py (adaptar el patró segons el connector):
dt_inici, dt_fi = extract_date_range(
    text,
    r"(?:Per[íi]ode\s*facturat|Per[íi]ode):?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:a|fins\s*a|al|-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
)
```

---

### 1.2. Evitar la creació de fitxers PDF buits (`0 bytes`) al NAS
* **Fitxer afectat:** `src/connectors/base.py`
* **Problema:** Quan `bonpreu.py` o `digi.py` reben un avís per email però no tenen credencials de portal, criden `self.save_and_ingest(b"", metriques_avis, ...)`, generant un fitxer `.pdf` corrupte de 0 bytes al NAS Samba.
* **Solució:** Modificar `save_and_ingest` perquè només desi al NAS si `pdf_bytes` conté dades.

**Modificar a `src/connectors/base.py`:**
```python
    def save_and_ingest(
        self,
        pdf_bytes: bytes,
        metriques: FacturaMetriques,
        origen: str,
    ) -> FacturaResult:
        """Desa el fitxer PDF al volum NAS (si n'hi ha) i envia les mètriques a InfluxDB."""
        any_str = str(metriques.data_emissio.year)
        mes_str = f"{metriques.data_emissio.month:02d}"
        clean_num = metriques.numero_factura.replace("/", "_").replace("\\", "_").replace(" ", "")
        filename = f"{any_str}_{mes_str}_{self.name}_{clean_num}.pdf"

        pdf_saved_path: str | None = None
        nou_descarregat = False

        if pdf_bytes:
            ja_existeix = storage.exists(self.tipus.value, self.name, any_str, filename)
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
        else:
            self.logger.info(f"Sense contingut PDF per a {metriques.numero_factura} (origen={origen}). Ometent escriptura al NAS.")
```

---

### 1.3. Persistir l'estat del Circuit Breaker a la Web UI (Singleton de Connectors)
* **Fitxer afectat:** `src/web/app.py`
* **Problema:** `get_all_connectors_map()` crea noves instàncies cada vegada que es crida, esborrant `self.consecutive_failures` i `self.cooldown_until`.
* **Solució:** Mantenir una única instància global dels connectors en memòria.

**Modificar a `src/web/app.py`:**
```python
CONNECTORS_MAP: dict[str, BaseConnector] = {
    "veolia": VeoliaConnector(),
    "catgas": CatgasConnector(),
    "bonpreu": BonpreuConnector(),
    "digi": DigiConnector(),
}

def get_all_connectors_map() -> dict[str, BaseConnector]:
    return CONNECTORS_MAP
```

---

### 1.4. Corregir el `case-sensitivity` en esborrar factures d'InfluxDB
* **Fitxer afectat:** `src/web/app.py`
* **Problema:** A `delete_invoice`, es converteix `numero_factura` a minúscules amb `.lower()`, però a InfluxDB el tag `numero_factura` es desa amb majúscules (ex: `FG2026_001234`), fent que el predicat d'eliminació no coincideixi mai.
* **Solució:** Extreure el número de factura preservant majúscules i minúscules.

**Modificar a `src/web/app.py` (dins `delete_invoice`):**
```python
    stem = Path(rel_path).stem
    numero_factura = stem
    for p in ["bonpreu", "digi", "veolia", "catgas"]:
         match = re.split(rf"_{p}_", stem, maxsplit=1, flags=re.IGNORECASE)
        if len(match) == 2:
            numero_factura = match[1]
            break
```

---

### 1.5. Evitar duplicació de PDFs en correus `.eml` imbricats
* **Fitxer afectat:** `src/email_client.py`
* **Problema:** Quan es troba un `.eml` adjunt, els seus PDFs s'afegeixen a `attachments` del correu pare amb `attachments.extend(sub_parsed.attachments)` i alhora s'itera sobre `nested_emails` a `fetch_recent_emails`.
* **Solució:** Eliminar la línia `attachments.extend(sub_parsed.attachments)` dins de `_parse_message` a `src/email_client.py`, deixant que cada `ParsedEmail` (pare o fill) gestioni únicament els seus propis adjunts.

---

## 🛡️ BLOC 2: Seguretat i Prevenció de Vulnerabilitats (✅ Aplicat)

### 2.1. Prevenir XSS (Cross-Site Scripting) als fragments HTML
* **Fitxer afectat:** `src/web/app.py`
* **Problema:** S'injecten cadenes de logs i metadades de correus directament en HTML sense escapar.
* **Solució:** Importar `html` i aplicar `html.escape()` a totes les variables dinàmiques dins de `get_connector_history` i `upload_invoice_files`.

**Modificar a `src/web/app.py`:**
```python
import html

# Dins de get_connector_history:
inv_id = html.escape(str(inv.get("id", "")))
inv_data = html.escape(str(inv.get("data", "-")))
inv_num = html.escape(str(inv.get("numero", "-")))
```

---

### 2.2. Corregir validació de Path Traversal i Zip Bomb
* **Fitxers afectats:** `src/storage.py`, `src/web/app.py`
* **Problema 1:** `str(target).startswith(str(base_path))` és vulnerable a col·lisions de prefix de carpeta (ex: `/data/factures_altres`).
* **Problema 2:** `/api/upload` llegeix fitxers i descomprimeix ZIPs en memòria sense límit de mida.
* **Solució:** Usar `Path.is_relative_to()` i limitar la mida dels fitxers pujats/descomprimits.

**Modificar a `src/storage.py` (i replicar a `view_pdf` i `download_pdf` de `src/web/app.py`):**
```python
    def delete_pdf(self, rel_path: str) -> bool:
        """Elimina un fitxer del NAS Samba de forma segura."""
        base_resolved = self.base_path.resolve()
        target = (base_resolved / rel_path).resolve()
        if not target.is_relative_to(base_resolved) or not target.is_file():
            return False
        if settings.DRY_RUN:
            logger.info(f"[DRY-RUN] S'eliminaria el fitxer a: {target}")
            return True
        target.unlink(missing_ok=True)
        logger.info(f"Fitxer eliminat correctament del NAS: {target}")
        return True
```

**Modificar a `src/web/app.py` (dins `upload_invoice_files`):**
```python
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB màxim

content = await file.read(MAX_UPLOAD_BYTES + 1)
if len(content) > MAX_UPLOAD_BYTES:
    raise HTTPException(status_code=413, detail="El fitxer supera el límit màxim de 50 MB.")

# I dins del bucle del ZIP:
for zinfo in z.infolist():
    if zinfo.is_dir() or zinfo.file_size > MAX_UPLOAD_BYTES:
        continue
```

---

## ⚡ BLOC 3: Optimització de Rendiment i Asincronia (✅ Aplicat)

### 3.1. Desbloquejar l'Event Loop de FastAPI
* **Fitxer afectat:** `src/web/app.py`
* **Problema:** Els endpoints `async def index` i `async def get_connector_history` fan crides síncrones bloquejants (lectura de NAS Samba, peticions `requests` a InfluxDB i connexions `imaplib`). Això congela l'Event Loop i atura l'emissió per WebSocket mentre duren.
* **Solució:** Executar les funcions pesades en un fil separat amb `asyncio.to_thread`.

**Modificar a `src/web/app.py`:**
```python
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Pàgina principal amb dashboard operatiu i controls."""
    invoices = await asyncio.to_thread(list_nas_invoices)
    # ... resta del codi igual
```

```python
@app.get("/api/history/{connector}")
async def get_connector_history(connector: str):
    """Consulta les factures històriques disponibles sense bloquejar l'event loop."""
    connectors = get_all_connectors_map()
    conn = connectors.get(connector.lower())
    if not conn:
        raise HTTPException(status_code=404, detail=f"Proveïdor '{connector}' desconegut.")

    invoices = await asyncio.to_thread(conn.fetch_available_invoices, imap_client)
    # ... resta del codi igual
```

---

### 3.2. Optimitzar `ImapMailboxClient` per no descarregar correus innecessaris
* **Fitxer afectat:** `src/email_client.py`
* **Problema:** `client.fetch(mail_id, "(RFC822)")` descarrega el correu sencer (amb tots els adjunts en binari) per a tots els correus dels últims 60/730 dies abans de comprovar si el remitent (`From`) o l'assumpte (`Subject`) coincideixen amb `sender_filters`.
* **Solució:** Descarregar primer només les capçaleres (`BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)]`), filtrar en memòria i descarregar el cos complet `(RFC822)` només dels correus que coincideixin.

**Modificar a `src/email_client.py` (dins `fetch_recent_emails`):**
```python
                for mail_id in mail_ids:
                    # 1. Baixar només capçaleres per filtrar ràpidament
                    if sender_filters:
                        _, hdr_data = client.fetch(mail_id, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)])")
                        if hdr_data and hdr_data[0] and isinstance(hdr_data[0][1], bytes):
                            hdr_msg = email.message_from_bytes(hdr_data[0][1])
                            h_sender = decode_str(hdr_msg.get("From", "")).lower()
                            h_subject = decode_str(hdr_msg.get("Subject", "")).lower()
                            # Nota: si l'usuari reenvia correus .eml, potser l'assumpte conté "Fwd:" o el proveïdor
                            if not any(f.lower() in h_sender or f.lower() in h_subject for f in sender_filters):
                                continue

                    # 2. Baixar el correu complet només si ha passat el filtre
                    _, msg_data = client.fetch(mail_id, "(RFC822)")
```

---

## 🧹 BLOC 4: Neteja de Codi, Dependències i Infraestructura (✅ Aplicat)

### 4.1. Migrar `@app.on_event("startup")` a `lifespan`
* **Fitxer afectat:** `src/web/app.py`
* **Problema:** `on_event("startup")` està deprecat a FastAPI.
* **Solució:** Usar `asynccontextmanager` per al `lifespan`.

**Modificar a `src/web/app.py`:**
```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()
    await asyncio.to_thread(auto_annotate_k8s_pod)
    yield

app = FastAPI(title="Subministres ETL", version=settings.APP_VERSION, lifespan=lifespan)
```

---

### 4.2. Eliminar dependències no utilitzades
* **Fitxers afectats:** `requirements.txt`, `pyproject.toml`
* **Acció:** Eliminar les dependències que no s'importen en cap lloc del codi per reduir la mida de la imatge Docker i la superfície d'atac:
  * `pdfplumber>=0.11.0`
  * `pyyaml>=6.0.1`
  * `beautifulsoup4>=4.12.3`
  * `tenacity>=8.3.0`

---

### 4.3. Corregir el CronJob al Helm Chart
* **Fitxer afectat:** `helm/subministres-etl/templates/cronjob.yaml` i `helm/subministres-etl/values.yaml`
* **Problema:** El `Dockerfile` té `CMD ["uvicorn", "src.web.app:app", ...]`. Si es desplega en mode `cronjob` sense especificar `args` o `command`, el Job arrenca el servidor web Uvicorn i no acaba mai.
* **Solució:** Definir `command: ["subministres-etl"]` per defecte a `cronjob.yaml`.

**Modificar a `helm/subministres-etl/templates/cronjob.yaml`:**
```yaml
          containers:
            - name: {{ .Chart.Name }}
              image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
              imagePullPolicy: {{ .Values.image.pullPolicy }}
              command: ["subministres-etl"]
              {{- if .Values.args }}
              args:
                {{- toYaml .Values.args | nindent 16 }}
              {{- end }}
```