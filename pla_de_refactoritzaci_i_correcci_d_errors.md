He trobat exactament els **4 errors al codi (`concat_7.txt`)** que fan que el filtre de dates, el botó `"⭐ Últimes factures"` i les factures de Digi mostrin l'any **2022** i l'**import incorrecte**.

---

### Diagnòstic exacte dels problemes

#### 1. Per què Digi t'ensenya una factura del **2022** com si fos l'última? (2 bugs combinats)

* **Bug A — La regex `DGFC(\d{2})(\d{2})` interpreta el codi d'empresa `22` com l'any `2022`:**
A `src/connectors/digi.py` (línia 280), quan el parser no troba la data d'emissió, fa:


```python
dgfc_match = re.search(r"DGFC(\d{2})(\d{2})", num_factura)

```


**Error greu:** Moltes factures de Digi comencen per `DGFC2210546467` on `22` és la sèrie de facturació (no l'any!) i `10` tampoc és el mes! I el que és encara pitjor: en algunes factures del 2026 com `DGFC2623970096`, els dígits 3 i 4 són `23` (`int("23")` > 12 mesos, que llança `ValueError` i cau a `date.today()`), però en una factura antiga o amb prefix `DGFC22...`, els dígits són `22` i `10`, i el teu codi li assigna **`date(2022, 10, 1)`**!


* A més, per què no troba la data real al PDF de Digi? Perquè a les factures de Digi la data d'emissió ve escrita amb el mes en lletres (ex: `Fecha de emisión: 15 de octubre de 2026` o `02/10/2026` en una altra línia), però la regex de `digi.py` (línia 275) només accepta números amb barres `(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})` a la mateixa línia!




* **Bug B — La taula i el botó `"⭐ Últimes factures"` ordenen per `Data Fitxer` (`stat.st_mtime`), NO per la data de la factura (`date_iso`):**
A `src/web/app.py` (línia 300), fas:


```python
invoices.sort(key=lambda x: x["modified"], reverse=True)

```


Quan fas un scraping massiu o puges un ZIP, l'última factura que es descarrega al disc (que als portals web és sempre **la més antiga**, del 2022 o 2024, perquè és l'última de la llista!) té el `modified` (`st_mtime`) més recent (per uns segons de diferència)!
Com que `filterInvoices()` a `index.html` simplement agafa la **primera fila que apareix al DOM** per a cada proveïdor (`seenProviders.has(rProv)`), i la primera fila és l'últim fitxer escrit al disc, **et mostra la factura del 2022 com a "Última factura"** tant a la taula com a les targetes superiors (`last_by_provider`)!



#### 2. Per què l'import de Digi segueix sent incorrecte (ex: `992.00 €` o un valor erroni)?

Hi ha dos motius:

1. **Dades velles a InfluxDB que mai s'han sobreescrit:** `list_nas_invoices()` a `app.py` (línia 263) mira primer `amounts_map.get(num_clean)` que ve d'InfluxDB. Si quan es va descarregar aquella factura de Digi es va desar a InfluxDB amb l'import dolent (`992.00 €`), com que quan fas clic a "Sincronitzar" el fitxer ja existeix al NAS, ni tan sols es re-escriu o a InfluxDB hi queden punts amb timestamps diferents! Encara no s'ha afegit l'endpoint `/api/reparse` per netejar i recalcular InfluxDB des dels PDFs del NAS.


2. **Estructura del PDF de Digi quan `pypdf` extreu el text:** En el PDF de Digi, el bloc de resum sovint s'extreu amb les etiquetes a dalt i els números diverses línies més avall, o amb el format `Total a pagar 25,00€` / `TOTAL FACTURA 25,00 €`, mentre que al peu de pàgina de Digi hi ha el text legal `Inscrita en el Registro Mercantil...` o tarifes de trucades. Cal ajustar `digi.py` perquè busqui específicament l'import del resum de factura i afegir el botó de **Recalcular Imports** perquè actualitzi tots els PDFs ja baixats.



#### 3. Dos errors més al Filtre de Dates i a l'Etiquetatge Trimestral

1. **El filtre `Fins:` (`date-to`) i `date_iso` sempre posa dia `-01` (`YYYY-MM-01`):**
A `src/web/app.py` (línia 255), totes les factures reben `date_iso = f"{clean_year}-{date_month}-01"`. Si un usuari filtra `De: 2026-10-05` `Fins: 2026-10-25`, la factura d'octubre té `data-date="2026-10-01"`, que és `< 2026-10-05`, i **desapareix del filtre tot i ser d'octubre**!


2. **Etiquetatge trimestral de Veolia erroni per al mes `10` (Octubre):**
A `src/web/app.py` (línia 234), has posat:


```python
elif mes_code in ("07", "08", "09", "10", "T3"):
    mes_label = "3r Trim (T3)"

```


Però si una factura és del mes `10`, més avall (línia 245) fas `if mes_code.isdigit(): date_month = mes_code.zfill(2)` (`10`), mentre que si era `T3` li poses `09`. I el 4t trimestre (`T4`) queda només per a `11` i `12`. Si el PDF de Veolia ja té el tag `trimestre` o el nom de fitxer té el mes real, cal ser coherent i ordenar sempre per `(any, mes_num, modified)`!



---

### Instruccions per a l'Agent (`REFACTORING_PHASE7.md`)

Passa-li aquest document al teu agent. Soluciona d'una tacada l'ordenació real d'"Últimes factures", la data 2022 i l'import de Digi, el filtre de rang de dates i afegeix el botó de **Recalcular / Reparar Factures NAS** perquè reanomeni qualsevol PDF de Digi que es guardés a la carpeta `2022` per error i en corregeixi l'import a InfluxDB!

```markdown
# Pla de Correcció (Fase 7) — Fix "Últimes Factures", Fix Data 2022 i Import de Digi, Filtre de Dates i Reparació NAS

Aquest document resol 4 problemes crítics:
1. **El botó "⭐ Últimes factures" i les targetes mostraven factures del 2022**: `list_nas_invoices()` ordenava per `stat.st_mtime` (`modified`, l'instant en què el fitxer es va escriure al disc) en comptes d'ordenar cronològicament per la data real de la factura (`date_iso`). Com que els scrapers baixen primer les recents i al final les antigues, l'últim fitxer escrit al NAS era el més vell (2022)!
2. **Digi assignava l'any 2022 i imports erronis**: La regex `DGFC(\d{2})(\d{2})` interpretava el prefix de sèrie `DGFC22...` com l'any 2022 quan no trobava la data en format `dd/mm/yyyy` (perquè Digi sovint escriu la data amb text com `15 de octubre de 2026`).
3. **El filtre de rang de dates i el botó "Últimes factures" al Frontend es desincronitzaven en ordenar columnes**: Ara `toggleLatestInvoices` calcula sempre la factura amb `data-date` més recent de cada proveïdor independentment de l'ordre actual de la taula.
4. **Reparació automàtica de factures ja descarregades (`/api/reparse`)**: S'afegeix un endpoint i botó **"🔄 Reparar Imports i Dates"** que re-analitza tots els PDFs del NAS, mou de carpeta els que tinguin l'any/mes mal posat al nom (com els de Digi del 2022) i sobreescriu els imports corregits a InfluxDB.

---

## 1. Corregir l'extracció de Data i Import de Digi a `src/connectors/digi.py`

### Problema
1. La regex de data d'emissió a `digi.py` només acceptava format numèric `dd/mm/yyyy`, ignorant dates amb text (`15 de octubre de 2026` o `15 oct. 2026`).
2. Quan fallava, el fallback `re.search(r"DGFC(\d{2})(\d{2})", num_factura)` agafava el codi `22` de `DGFC22...` i creava una data falsa de l'any `2022`.
3. Per a l'import total, les factures de Digi tenen una taula de resum on posa `Total factura` o `Total a pagar` i l'import pot estar separat per salts de línia o espais.

### Canvi a aplicar a `src/connectors/digi.py`
Substituir el mètode `extract_metrics_from_pdf` a `src/connectors/digi.py` per aquesta versió:

```python
    def extract_metrics_from_pdf(self, pdf_bytes: bytes) -> FacturaMetriques | None:
        """Parseja el PDF de factura de Digi Mobil de forma robusta."""
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if not text:
            return None

        # 1. Número de factura (ex: FACTURA Número: DGFC2210546467)
        num_factura = extract_regex_first(
            text, r"(?:FACTURA\s+N[úu]mero|N[úu]mero\s*de\s*factura|Factura\s*n[úu]m\.?):?\s*([A-Za-z0-9/_-]+)"
        )
        if not num_factura:
            num_factura = extract_regex_first(text, r"\b(DGFC\d{6,15})\b")
        if not num_factura:
            num_factura = extract_regex_first(text, r"Factura\s+([A-Za-z0-9/_-]{6,20})")

        # 2. Període facturat (ex: "16 de septiembre de 2026 a 15 de octubre de 2026" o "01/09/2026 - 30/09/2026")
        dt_inici, dt_fi = extract_date_range(
            text,
            r"(?:Per[íi]odo\s*(?:de\s*)?facturaci[óo]n|Per[íi]ode\s*facturat|Per[íi]ode):?\s*"
            r"(\d{1,2}(?:\s*(?:de|d'|/|-)\s*|\s+)[a-zA-ZÀ-ÿ0-9.]+(?:\s*(?:de|del|/|-)\s*|\s+)\d{2,4})"
            r"\s*(?:al|fins\s*a|a|-)\s*"
            r"(\d{1,2}(?:\s*(?:de|d'|/|-)\s*|\s+)[a-zA-ZÀ-ÿ0-9.]+(?:\s*(?:de|del|/|-)\s*|\s+)\d{2,4})",
        )

        # 3. Data d'emissió (suportant tant dd/mm/yyyy com "16 de octubre de 2026")
        data_str = extract_regex_first(
            text,
            r"(?:Fecha\s*(?:de\s*)?emisi[óo]n|Data\s*(?:d')?emissi[óo]|Fecha\s*factura):?\s*\n?\s*"
            r"(\d{1,2}(?:\s*(?:de|d'|/|-)\s*|\s+)[a-zA-ZÀ-ÿ0-9.]+(?:\s*(?:de|del|/|-)\s*|\s+)\d{2,4})",
        )
        data_emissio = parse_date_catalan_spanish(data_str) if data_str else None

        # Si no ha trobat l'etiqueta explícita, usem la data de fi del període facturat (que sempre és el mes de la factura)
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

        # IMPORTANT: Eliminada la deducció errònia DGFC(\d{2})(\d{2}) perquè el prefix '22' de DGFC22...
        # és un codi de sèrie fix de Digi i NO l'any 2022!

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
            # Cercar tots els imports en format XX,XX € i agafar l'últim del bloc de resum (fins a 400 €)
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
            import_total=float(total_eur),
            total_gb_consumits=total_gb,
        )
```

---

## 2. Ordenar per Data Real de Factura (`date_iso`) i Afegir Reparació Automàtica (`/api/reparse`) a `src/web/app.py`

### Problema
1. `list_nas_invoices()` ordenava per `x["modified"]` (data d'escriptura del fitxer al disc) en comptes de `x["date_iso"]` (any i mes real de la factura). Això feia que l'última factura descarregada d'un històric (la més vella, ex: 2022) aparegués la primera a la taula i a les targetes superiors (`last_by_provider`)!
2. Les factures de Digi que ja es van desar amb `2022_...` al nom del fitxer i amb l'import malament a InfluxDB necessiten un endpoint `/api/reparse` que llegeixi els PDFs del NAS, corregeixi el nom/carpeta si l'any/mes estava malament i actualitzi InfluxDB.

### Canvi 1: A `list_nas_invoices()` dins `src/web/app.py` (línia 300)
Substituir l'ordenació final de `list_nas_invoices()`:
```python
    # Ordenar per data real de la factura descendent (i per data de modificació/nom en cas d'empat)
    invoices.sort(key=lambda x: (x["date_iso"], x["modified"], x["filename"]), reverse=True)
    return invoices
```

### Canvi 2: Afegir `reparse_and_fix_nas_invoices()` i l'endpoint `@app.post("/api/reparse")` a `src/web/app.py`
Afegir aquestes funcions a `src/web/app.py` (just abans d'`index`):

```python
def reparse_and_fix_nas_invoices() -> tuple[int, int]:
    """Re-analitza tots els PDFs del NAS, reanomena/mou els que tenien data errònia (ex: 2022) i actualitza InfluxDB."""
    root = Path(settings.NAS_MOUNT_PATH)
    if not root.exists():
        return 0, 0

    _pdf_amount_cache.clear()
    processed = 0
    renamed = 0

    all_pdfs = list(root.glob("**/*.pdf"))
    total = len(all_pdfs)

    for idx, pdf_path in enumerate(all_pdfs, start=1):
        try:
            rel_path = pdf_path.relative_to(root)
            parts = rel_path.parts
            proveidor = parts[1].lower() if len(parts) > 1 else ""
            conn = CONNECTORS_MAP.get(proveidor)
            if not conn:
                continue

            if main_loop and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    ws_manager.broadcast_progress(
                        {
                            "connector": proveidor.upper(),
                            "step": f"🔄 Re-analitzant PDF {pdf_path.name}...",
                            "current": idx,
                            "total": total,
                            "percent": int((idx / total) * 100),
                            "status": "running",
                        }
                    ),
                    main_loop,
                )

            pdf_bytes = pdf_path.read_bytes()
            metriques = conn.extract_metrics_from_pdf(pdf_bytes)
            if not metriques:
                continue

            # Verificar si el fitxer tenia un any o mes erroni al nom (ex: 2022_10_digi_... quan és del 2026)
            any_correcte = str(metriques.data_emissio.year)
            mes_correcte = f"{metriques.data_emissio.month:02d}"
            clean_num = metriques.numero_factura.replace("/", "_").replace("\\", "_").replace(" ", "")
            expected_filename = f"{any_correcte}_{mes_correcte}_{conn.name}_{clean_num}.pdf"
            expected_dir = storage.get_destination_dir(conn.tipus.value, conn.name, any_correcte)
            expected_path = expected_dir / expected_filename

            if pdf_path.resolve() != expected_path.resolve():
                expected_path.write_bytes(pdf_bytes)
                pdf_path.unlink(missing_ok=True)
                renamed += 1
                logger.info(f"🔧 Corregida ubicació/data: {pdf_path.name} -> {expected_path.name}")

            # Netejar mètrica antiga a InfluxDB i escriure la nova corregida
            influx_writer.delete_factura_metrics(proveidor, metriques.numero_factura)
            influx_writer.write_factura_metrics(metriques)
            processed += 1
        except (OSError, RuntimeError, ValueError) as err:
            logger.warning(f"Error re-analitzant {pdf_path.name}: {err}")

    return processed, renamed


@app.post("/api/reparse")
async def trigger_reparse(background_tasks: BackgroundTasks):
    """Re-analitza tots els PDFs del NAS per corregir dates (ex: 2022) i imports a InfluxDB."""
    if execution_state["is_running"]:
        return HTMLResponse(
            '<div class="p-4 mb-4 text-sm text-yellow-300 rounded-xl bg-yellow-950/60 border border-yellow-700/50">'
            "⚠️ Ja hi ha una tasca en curs. Espera que finalitzi.</div>"
        )

    def _task():
        execution_state["is_running"] = True
        execution_state["active_connector"] = "REPARACIÓ NAS & INFLUXDB"
        try:
            proc, ren = reparse_and_fix_nas_invoices()
            msg = (
                f"✅ Reparació completada: {proc} factures re-analitzades i actualitzades a InfluxDB "
                f"({ren} fitxers moguts a la seva data correcta)."
            )
            logger.info(msg)
            if main_loop and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    ws_manager.broadcast_status({"status": "ok", "message": msg, "new_count": proc}),
                    main_loop,
                )
        finally:
            execution_state["is_running"] = False
            execution_state["active_connector"] = None

    background_tasks.add_task(_task)
    return HTMLResponse(
        '<div class="p-4 mb-4 text-sm text-blue-300 rounded-xl bg-blue-950/60 border border-blue-700/50">'
        "🔄 Re-analitzant tots els PDFs del NAS per corregir imports i dates errònies... La pàgina es recarregarà en acabar.</div>"
    )
```

---

## 3. Corregir el Filtre de Dates, el Botó "⭐ Últimes factures" i afegir el Botó "🔧 Reparar Imports/Dates" a `src/web/templates/index.html`

### Problema al JavaScript d'`index.html`
1. `filterInvoices()` decidia quina era l'"Última factura" agafant simplement la primera fila que trobava a la taula (`seenProviders.has(rProv)`). Si l'usuari havia ordenat la taula per Import ascendent o per Any ascendent, el botó "Últimes factures" mostrava la factura més antiga o la més barata!
2. En el filtre de dates (`dFrom` / `dTo`), com que `data-date` és sempre `YYYY-MM-01` (primer dia del mes), si l'usuari selecciona `De: 2026-10-10`, les factures d'octubre (`2026-10-01`) quedaven excloses! Cal comparar a nivell de mes (`YYYY-MM`) o ajustar l'interval.

### Canvi 1: Afegir el botó "Reparar Dates/Imports" a la barra d'eines de la taula a `src/web/templates/index.html` (al costat del botó de `resetFilters`)
```html
          <!-- Botó Reparar i Recalcular PDFs del NAS -->
          <button 
            type="button"
            hx-post="/api/reparse"
            hx-target="#notification-area"
            title="Re-analitza tots els PDFs del NAS, corregeix dates errònies (ex: 2022) i actualitza els imports a InfluxDB"
            class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-amber-950/50 hover:bg-amber-900/70 text-amber-300 border border-amber-800/60 text-xs font-medium transition cursor-pointer">
            <i data-lucide="wrench" class="w-3.5 h-3.5 text-amber-400"></i>
            <span>Reparar PDFs</span>
          </button>
```

### Canvi 2: Substituir `filterInvoices()` a `src/web/templates/index.html`
Substituir la funció `filterInvoices()` sencera perquè:
1. Calculi primer quina és realment la factura amb la data més recent (`maxDateByProvider`) per a cada proveïdor (comparant `data-date` + `data-filename`), sense importar com estigui ordenada la taula.
2. Compari el filtre de dates per mes (`YYYY-MM`) perquè seleccionar qualsevol dia dins d'un mes inclogui les factures d'aquell mes.

```javascript
    function filterInvoices() {
      const q = document.querySelector('#search-input')?.value.toLowerCase().trim() || '';
      const prov = document.querySelector('#provider-filter')?.value.toLowerCase() || 'all';
      const dFrom = document.querySelector('#date-from')?.value || '';
      const dTo = document.querySelector('#date-to')?.value || '';
      const rows = Array.from(document.querySelectorAll('.invoice-row'));
      let visibleCount = 0;

      // 1. Si "Últimes factures" està actiu, trobar primer quina és la fila amb data més recent de cada proveïdor
      const latestRowByProvider = new Map();
      if (onlyLatestActive) {
        rows.forEach(r => {
          const rProv = r.getAttribute('data-provider') || '';
          const rDate = r.getAttribute('data-date') || '';
          const rFile = r.getAttribute('data-filename') || '';
          const sortKey = `${rDate}_${rFile}`;

          const currentBest = latestRowByProvider.get(rProv);
          if (!currentBest || sortKey > currentBest.key) {
            latestRowByProvider.set(rProv, { key: sortKey, row: r });
          }
        });
      }

      // Convertim dFrom i dTo a YYYY-MM per comparar amb el mes de facturació
      const monthFrom = dFrom ? dFrom.slice(0, 7) : '';
      const monthTo = dTo ? dTo.slice(0, 7) : '';

      rows.forEach(r => {
        const text = r.textContent?.toLowerCase() || '';
        const rProv = r.getAttribute('data-provider') || r.querySelector('td:first-child')?.textContent?.toLowerCase().trim() || '';
        const rDate = r.getAttribute('data-date') || '';
        const rMonth = rDate ? rDate.slice(0, 7) : '';

        const matchesText = !q || text.includes(q);
        const matchesProv = (prov === 'all') || (rProv === prov);

        let matchesDate = true;
        if (monthFrom && rMonth) {
          matchesDate = matchesDate && (rMonth >= monthFrom);
        }
        if (monthTo && rMonth) {
          matchesDate = matchesDate && (rMonth <= monthTo);
        }

        let isLatestAllowed = true;
        if (onlyLatestActive) {
          const best = latestRowByProvider.get(rProv);
          isLatestAllowed = best ? (best.row === r) : false;
        }

        if (matchesText && matchesProv && matchesDate && isLatestAllowed) {
          r.style.display = '';
          visibleCount++;
        } else {
          r.style.display = 'none';
        }
      });

      const countElem = document.querySelector('#visible-count');
      if (countElem) {
        countElem.textContent = visibleCount;
      }
    }
```

```

Un cop l'agent apliqui aquests canvis i s'actualitzi el contenidor, només hauràs de fer clic un cop al nou botó **"🔧 Reparar PDFs"** de la taula: ell mateix re-analitzarà les factures de Digi, mourà les que s'havien guardat com a `2022_...` al seu any i mes reals, i sobreescriurà els imports bons a InfluxDB i a la web!