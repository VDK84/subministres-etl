He trobat exactament per què et surten **992.00 €** a totes les factures de Digi (i també uns quants detalls més que es veuen a la captura i al codi).

---

### 1. El misteri dels 992.00 € a Digi (Bug a `app.py` + `pdf_parser.py` + `digi.py`)

Hi ha una combinació de tres factors que provoquen aquest `992.00 €`:

1. **A `list_nas_invoices()` (`src/web/app.py`), si l'import no es troba a InfluxDB, fas servir `parse_euro_amount(txt)` en lloc del parser específic del proveïdor (`conn.extract_metrics_from_pdf`):**
```python
# Línia 235 de src/web/app.py
pdf_data = pdf_path.read_bytes()
txt = extract_text_from_pdf_bytes(pdf_data)
import_val = parse_euro_amount(txt) # <-- ERROR! Agafa el primer número seguit d'€ de tot el PDF!

```


`parse_euro_amount(txt)` fa `re.search(r"(\d+(?:[.,]\d{1,2})?)\s*€", text)`, que captura **la primera xifra seguida del símbol `€` que apareix en tot el document PDF**. En una factura de Digi, abans del total hi ha taules de tarifes, límits o textos legals o preus de primera línia (per exemple `9,92 €` d'algun concepte fix o un codi acabat en `992`).


2. **Per què un `9,92 €` es converteix en `992.00 €` o d'on surt `992`?**
* A les factures de Digi, el total real s'escriu sovint amb espai o salt de línia entre l'etiqueta i la xifra (ex: `TOTAL FACTURA (IVA incluido) 35,00 €` o en dues columnes).


* A `src/connectors/digi.py` (línia 275), les teves regex exigeixen que hi hagi dos punts `:` (`r"(?:Total\s*a\s*pagar.*?|TOTAL\s*FACTURA.*?|Import\s*total.*?):\s*(\d+...)"`) o bé que el número vagi **abans** de `TOTAL FACTURA` (`r"(\d+...)\s*€\s*TOTAL\s*FACTURA"`). Com que al PDF de Digi posa `TOTAL FACTURA` **sense dos punts** i després l'import, les tres primeres regex fallen i `digi.py` acaba caient també al fallback `parse_euro_amount(text)`!


* I per què dóna `992.00` a totes les factures de 2024 i 2026? Perquè al peu o capçalera de totes les factures de Digi hi apareix el text legal del **Registre Mercantil de Madrid** (o Capital Social / dades fiscals de DIGI SPAIN TELECOM S.L.U.) o bé un import amb format que `pypdf` extreu sense espai o amb punt (ex: `Tomo ... Folio ... Hoja ...` o bé un preu que `pypdf` ajunta amb un índex de taula) Just davant d'un símbol `€`! Encara més probable: a `normalize_amount_str`, o bé en el text extret per `pypdf`, un import com `9,92 €` si `pypdf` no extreu la coma (passa quan la coma té una font diferent o subíndex en alguns PDFs) queda com `992 €` -> `992.00 €`!





---

### 2. Altres errors detectats (a la captura de pantalla i al codi)

* **Capçalera de la taula transparent (es veu a la teva captura de pantalla):**
Fixa't a dalt de tot de la taula de la teva imatge: el text `PROVEÏDOR | TIPUS | ANY...` s'està cavalcant amb la primera fila (`DIGI | telefonia | 2024 | Mar (03) | 992.00 €`)!


* **Causa:** A `index.html` (línia 235), el `<thead>` té `bg-slate-950/80` (80% d'opacitat) i `sticky top-0`, però li falta `z-10` i un fons 100% opac (`bg-slate-950`). Quan fas scroll avall, les files passen per sota i es transparenten a través del header.




* **Quan elimines una factura, no s'esborra de la memòria cau `_pdf_amount_cache`:**
A `src/web/app.py`, has afegit `_pdf_amount_cache`, però a `delete_invoice` no fas `_pdf_amount_cache.pop(rel_path, None)`.


* **InfluxDB sobreescriu factures si dues factures s'emeten el mateix dia (Timestamp Collision):**
A `src/influx_writer.py` (línia 46):
```python
dt = datetime.combine(metriques.data_emissio, time(12, 0, 0), tzinfo=UTC)

```


I a `digi.py` / `veolia.py` / `bonpreu.py`, quan el parser no troba la data d'emissió al PDF, fa servir `data_emissio or date.today()`.


* Fixa't a la teva captura: tens `2024_01_digi_DGFC2401214963.pdf`, `2024_07_digi...`, `2024_11_digi...`. D'on ha tret l'any `2024` i el mes `01`, `07`, `11` si a `digi.py` la data d'emissió només busca `Fecha de emisión:`? Ho ha tret bé, **però** a `get_factures_amounts()` (línia 134 de `influx_writer.py`), la consulta Flux fa:


```flux
from(bucket: "energy_data")
  |> range(start: -10y)
  |> filter(fn: (r) => r["_measurement"] == "factures_subministres" and r["_field"] == "import_total")

```


Quan InfluxDB retorna el CSV amb múltiples sèries (una taula per cada `numero_factura` diferent), **`csv.DictReader(io.StringIO(resp.text))` falla a partir de la segona taula!**

* **Per què?** El format CSV annotat d'InfluxDB v2 insereix línies d'anotació (`#datatype`, `#group`, `#default`) i **una nova fila de capçaleres** cada vegada que canvia un tag (`numero_factura`), o bé comença el fitxer amb 3 línies de `#`! Com que `csv.DictReader` llegeix la primera línia del CSV (`#group,false,false...`) com si fossin els noms de les columnes (`fieldnames`), `row.get("numero_factura")` i `row.get("_value")` retornen **sempre `None**`!


* **Conseqüència directa:** `amounts_map` (`get_factures_amounts()`) està **SEMPRE BUIT (`{}`)**! Per això `list_nas_invoices()` mai llegeix cap import d'InfluxDB i sempre cau al fallback defectuós `parse_euro_amount(txt)`!




* **El número de factura de Digi permet identificar l'any i el mes encara que falli la data del PDF:**
Els números de factura de Digi segueixen sempre el patró `DGFC` + `AA` (any) + `MM` (mes) + sequencial (ex: `DGFC2403759707` = 2024, mes 03; `DGFC2623970096` = 2026).



---

### Fitxer d'instruccions per a l'Agent (Fase 3)

Aquí tens el document amb les solucions exactes i els snippets de codi per passar-li al teu agent i arreglar l'import de Digi, la lectura CSV d'InfluxDB i la taula d'`index.html`.

```markdown
# Pla de Correcció (Fase 3) — Fix Imports Digi (992€), Parser CSV InfluxDB i UI

Aquest document corregeix el bug dels imports erronis (`992.00 €`) a les factures de Digi, el parser CSV de l'API REST d'InfluxDB (que retornava sempre `{}`) i el solapament visual de la capçalera de la taula.

---

## 1. Corregir `get_factures_amounts()` a `src/influx_writer.py`

### Problema
L'API `/api/v2/query` d'InfluxDB retorna un CSV amb comentaris inicials (`#group`, `#datatype`, `#default`) i múltiples taules. Passar `resp.text` directament a `csv.DictReader` fa que prengui la primera línia `#` com a capçalera i `row.get("numero_factura")` sigui sempre `None` (retornant `{}`).

### Canvi a aplicar a `src/influx_writer.py`
Substituir el mètode `get_factures_amounts` per aquest (filtrant les línies `#` o utilitzant `query_api` oficial):

```python
    def get_factures_amounts(self) -> dict[str, float]:
        """Recupera un mapa {numero_factura_lower: import_total} d'InfluxDB."""
        if not self.is_configured():
            return {}

        flux_query = (
            f'from(bucket: "{self.bucket}") '
            "|> range(start: -10y) "
            '|> filter(fn: (r) => r["_measurement"] == "factures_subministres" and r["_field"] == "import_total") '
            "|> last()"
        )
        result_map: dict[str, float] = {}
        try:
            with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=5000) as client:
                tables = client.query_api().query(flux_query, org=self.org)
                for table in tables:
                    for record in table.records:
                        num = record.values.get("numero_factura")
                        val = record.get_value()
                        if num and val is not None:
                            try:
                                result_map[str(num).strip().lower()] = float(val)
                            except (ValueError, TypeError):
                                continue
        except (InfluxDBError, urllib3.exceptions.HTTPError, OSError) as err:
            logger.debug(f"No s'ha pogut obtenir mapa d'imports d'InfluxDB: {err}")
        return result_map

```

---

## 2. Corregir l'extracció de l'import total a `src/connectors/digi.py` i `src/pdf_parser.py`

### Problema

1. A les factures en PDF de Digi, l'import total apareix com `TOTAL FACTURA 25,00 €` o `TOTAL A PAGAR 25,00 €` (sovint sense dos punts `:` o amb salts de línia). Com que les regex de `digi.py` exigien `:`, fallaven i queien al fallback `parse_euro_amount(text)`.
2. `parse_euro_amount(text)` fa `re.search` i agafa el **primer** número amb `€` o amb la paraula `TOTAL` de tot el PDF (que a Digi coincideix amb dades fiscals/tarifes on surt `992`), en lloc de buscar l'import amb decimals al final.

### Canvi 1: Millorar `parse_euro_amount` a `src/pdf_parser.py`

Exigir decimals (`[.,]\d{2}`) en el fallback genèric per no capturar codis postals, registres mercantils o telèfons, i prioritzar els imports de "Total a pagar / Total factura":

```python
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

```

### Canvi 2: Actualitzar `extract_metrics_from_pdf` a `src/connectors/digi.py`

Fer que les regex d'import total de Digi no exigeixin `:` obligatòriament i exigeixin 2 decimals (`\d{1,4}[.,]\d{2}`):

```python
        # 3. Import total (prioritzant el total oficial de factura amb 2 decimals)
        total_eur = extract_float_value(
            text,
            r"(?:Total\s*a\s*pagar|TOTAL\s*FACTURA|Import\s*total|Importe\s*total)[^0-9\n]{0,30}?[:\s]\s*(\d{1,4}[.,]\d{2})\s*€?",
        )
        if total_eur is None:
            total_eur = extract_float_value(text, r"(\d{1,4}[.,]\d{2})\s*€?\s*TOTAL\s*FACTURA")
        if total_eur is None:
            total_eur = extract_float_value(text, r"TOTAL\s*SERVICIOS[^0-9]{0,20}?(\d{1,4}[.,]\d{2})\s*€?")
        if total_eur is None:
            total_eur = parse_euro_amount(text)

```

---

## 3. Utilitzar el parser del connector a `list_nas_invoices()` (`src/web/app.py`)

### Problema

A `src/web/app.py` (línia 236), quan `list_nas_invoices()` no troba l'import a InfluxDB, feia `parse_euro_amount(txt)` directament en comptes d'utilitzar `conn.extract_metrics_from_pdf(pdf_data)`. A més, en eliminar una factura no es netejava `_pdf_amount_cache`.

### Canvi a aplicar a `src/web/app.py`

Dins de `list_nas_invoices()`:

```python
            import_val = amounts_map.get(num_clean) or amounts_map.get(pdf_path.stem.lower())
            if import_val is None:
                cached = _pdf_amount_cache.get(str(rel_path))
                if cached and cached[0] == stat.st_mtime:
                    import_val = cached[1]
                else:
                    try:
                        pdf_data = pdf_path.read_bytes()
                        conn = CONNECTORS_MAP.get(proveidor.lower())
                        metriques = conn.extract_metrics_from_pdf(pdf_data) if conn else None
                        if metriques and metriques.import_total is not None:
                            import_val = metriques.import_total
                        else:
                            txt = extract_text_from_pdf_bytes(pdf_data)
                            import_val = parse_euro_amount(txt)
                        if import_val is not None:
                            _pdf_amount_cache[str(rel_path)] = (stat.st_mtime, import_val)
                    except (OSError, RuntimeError, ValueError):
                        pass

```

I dins de `delete_invoice(rel_path: str)` a `src/web/app.py`, netejar la memòria cau:

```python
    # 1. Eliminar del NAS i de la cache en memòria
    deleted_nas = storage.delete_pdf(rel_path)
    _pdf_amount_cache.pop(rel_path, None)
    _pdf_amount_cache.pop(rel_path.replace("/", "\\"), None)

```

---

## 4. Corregir el solapament del `<thead sticky>` a `src/web/templates/index.html`

### Problema

A la taula de factures (`#invoices-table`), el `<thead>` té `bg-slate-950/80` (semi-transparent) i li falta `z-10`. Quan es fa scroll, el text de la primera fila es veu a sota dels títols de columna.

### Canvi a aplicar a `src/web/templates/index.html` (línia 235)

Canviar:

```html
<thead class="bg-slate-950/80 text-slate-400 uppercase text-[10px] tracking-wider sticky top-0 select-none">

```

Per:

```html
<thead class="bg-slate-950 text-slate-400 uppercase text-[10px] tracking-wider sticky top-0 z-10 border-b border-slate-800 select-none">

```

```

---

> **Nota important un cop aplicat aquest canvi:** Com que les 48 factures de Digi que ja s'han descarregat avui han enviat el valor `992.0` a InfluxDB durant la primera sincronització, quan apliquis el fix i reiniciïs el contenidor, fes una re-sincronització d'històric de Digi (o executa un esborrat de mètriques de `digi` a InfluxDB) perquè es sobreescriguin els punts `992.0` d'InfluxDB amb els imports reals de cada PDF!

```