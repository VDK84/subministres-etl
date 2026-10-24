El codi del repositori (`concat_6.txt`) ha fet un salt de qualitat brutal: ja tens els 4 scrapers amb Playwright operatius (incloent el 2FA automàtic per correu a Bonpreu i Veolia, i la selecció de pestanya GAS a l'Oficina Virtual de Catgas), la barra de progrés `X/N` per WebSocket i els botons separats de Correu i Portal Web.

Tot i això, revisant al mil·límetre la connexió entre el **codi Python**, **InfluxDB** i el **Dashboard de Grafana (CaC)**, hi ha **errors crítics a les consultes Flux del Dashboard** que faran que els gràfics de barres i la taula fallin o mostrin dades buides, a més d'un parell de detalls als connectors que et permetran treure molt més suc a Grafana.

---

### Part 1: Errors Crítics al Dashboard de Grafana (i com corregir-los)

1. **Els panells de BarChart (Panells 6, 8 i 9) es trencaran o ajuntaran totes les factures:**
* A les queries dels Panells 6, 8 i 9 fas un `|> keep(columns: ["_time", "_value", "proveidor"])` **abans** de fer `|> aggregateWindow(every: 1mo, fn: sum, createEmpty: false)`.
* **El problema a Flux (InfluxDB v2):** Per defecte, cada factura té els tags `numero_factura`, `any`, `mes`, `referencia`, etc.. En Flux, cada combinació de tags és una taula separada. Quan fas `keep(columns: ["_time", "_value", "proveidor"])`, elimines les columnes `numero_factura`, `any` i `mes` de les taules, però **les taules segueixen dividides internament per l'agrupació antiga**! Si no fas un `|> group(columns: ["proveidor"])` (als panells 6 i 9) o `|> group(columns: ["_field"])` (al panell 8) abans d'`aggregateWindow`, Grafana rebrà desenes de sèries soltes amb el mateix nom (`bonpreu`, `bonpreu`...) en lloc d'una sola sèrie mensual per proveïdor.


* A més, canviant aquests panells a **`timeseries`** amb `"drawStyle": "bars"` i `timeSrc: "_start"`, Grafana pinta l'eix temporal mensual (`1mo`) de forma nativa, apila les barres perfecte i permet superposar l'any anterior!


2. **El filtre desplegable `$proveidor` no funciona quan selecciones `"All"`:** * Al JSON tens la variable `proveidor` amb `"includeAll": true` i `"current": { "text": "All", "value": "$__all" }`.
* Quan selecciones `"All"`, Grafana **no substitueix `"$proveidor"` pel text `"All"**`, sinó que hi injecta una llista o expressió regular. Per tant, la condició `r.proveidor == "$proveidor"` falla quan selecciones `All`!
* **Solució:** Posar `"allValue": "All"` a la definició de la variable `proveidor` dins de `templating.list`.


3. **El Panell 10 (Taula `"📑 Registre de Factures Processades"`) pot donar error si alguna columna encara no existeix:**
* En Flux, si en el rang de temps seleccionat cap factura té el tag `"titular"` (que ara mateix cap dels teus 4 connectors extreu), `keep()` pot fallar si la columna no existeix a l'esquema d'aquell *chunk*. Fent `|> pivot(...) |> group()` sense un `keep()` rígid sobre columnes inexistents, la taula mostrarà totes les columnes reals disponibles (incloent `consum_kwh_punta`, `consum_kwh_pla`, `consum_kwh_vall`, `potencia_contractada_kw` i `total_gb_consumits`).





---

### Part 2: 3 Millores al Codi Python (`concat_6.txt`) per a velocitat i dades

1. **A `digi.py` i `veolia.py`, `fetch_available_invoices()` no llista els PDFs que ja són al NAS si s'han baixat pel Portal Web:**
* A `bonpreu.py` i `catgas.py` l'agent ha afegit el bloc final `# Afegir també les factures que ja tenim al volum NAS`, però **s'ha deixat de posar aquest mateix bloc al final de `fetch_available_invoices` a `digi.py` i a `veolia.py**`.




2. **A `veolia.py` i `digi.py`, el scraper web no omet d'entrada les factures que ja són al NAS:**
* A `bonpreu.py` i `catgas.py`, abans de clicar el botó de descàrrega es comprova `if ja_al_nas: continue`. En canvi, a `veolia.py` entra a cada URL de factura i baixa el PDF encara que `inv_num_hint` ja existeixi al NAS. Comprovar-ho abans de fer `page.goto(inv_url)` t'estalviarà 20 segons per factura!




3. **Càlcul automàtic de mètriques derivades (`cost_diari_eur` i `preu_unitari`) a `influx_writer.py`:**
* Abans d'enviar el `Point` a InfluxDB, si `dies_facturats > 0`, podem guardar directament el camp `cost_diari_eur = import_total / dies_facturats`, `preu_kwh = import_total / consum_kwh` i `preu_m3 = import_total / consum_m3`. Això fa que les consultes de Grafana siguin instantànies!





---

### Part 3: Potenciació del Dashboard amb Variació Interanual (YoY) i Costos Unitaris

Sense eliminar cap dels teus panells actuals, afegim al Dashboard:

1. **📉 Gràfic Comparatiu Interanual (YoY — Mes actual vs. Mateix mes de l'any passat):** Un panell on les barres verdes mostren la despesa de cada mes d'enguany i una **línia discontínua blava/taronja (amb `timeShift(duration: 1y)` a Flux)** hi superposa exactament què vas pagar el mateix mes de l'any passat!
2. **📊 KPI de Variació Interanual Acumulada (€ i % YoY):** Targeta comparant la despesa dels últims 12 mesos respecte als 12 mesos anteriors.
3. **🏦 KPI de Despesa Acumulada en el Període Seleccionat (€)**.
4. **📈 Evolució del Cost Real de l'Energia (€/kWh tot inclòs de Llum i Gas, i €/m³ d'Aigua)**.

---

### Document complet d'instruccions per a l'Agent (`REFACTORING_PHASE6.md`)

Copia aquest bloc Markdown i passa-li directament al teu agent perquè apliqui les millores tant al codi Python com al manifest del Dashboard de Grafana:

```markdown
# Pla de Millora (Fase 6) — Dashboard Grafana amb Variació Interanual (YoY), Costos Unitaris i Optimització NAS a Veolia/Digi

Aquest document conté dues parts:
1. **Optimitzacions al codi Python (`veolia.py`, `digi.py` i `influx_writer.py`)**: Omissió ràpida de factures ja existents al NAS abans de descarregar-les amb Playwright, llistat complet al modal d'Històric i càlcul automàtic de mètriques derivades (`cost_diari_eur`, `preu_kwh`, `preu_m3`).
2. **ConfigMap del Dashboard de Grafana CaC (`06-subministres-factures.yaml`) corregit i potenciat**: Soluciona el bug del filtre `$proveidor = All`, corregeix l'agrupació de les sèries mensuals amb `group()` i afegeix la **Comparativa Interanual (YoY amb `timeShift(duration: 1y)`)** i el **Cost Real Unitari (€/kWh i €/m³)**.

---

## 1. Millores al Codi Python (`veolia.py`, `digi.py` i `influx_writer.py`)

### 1.1. Ometre factures ja descarregades al NAS a `src/connectors/veolia.py` i `src/connectors/digi.py`
1. A **`src/connectors/veolia.py`**, dins `fetch_invoices_from_portal`, just abans del bucle `for i, inv_url in enumerate(invoice_urls[:max_dl], start=1):`, comprovar si el número de factura (`inv_num_hint`) ja existeix al NAS per no tornar-lo a descarregar:
```python
                    nas_root = Path(settings.NAS_MOUNT_PATH) / self.tipus.value / self.name
                    existing_files = list(nas_root.glob("**/*.pdf")) if nas_root.exists() else []

                    for i, inv_url in enumerate(invoice_urls[:max_dl], start=1):
                        inv_num_hint = inv_url.split("/")[-3] if len(inv_url.split("/")) >= 3 else f"factura_{i}"
                        if any(inv_num_hint in f.name for f in existing_files):
                            self.report_progress(
                                f"⏭️ Factura Veolia {inv_num_hint} ja és al NAS. Ometent ({i}/{max_dl}).",
                                current=i,
                                total=max_dl,
                            )
                            continue
```

2. Tant a **`src/connectors/veolia.py`** com a **`src/connectors/digi.py`**, al final de `fetch_available_invoices(self, imap_client: ImapMailboxClient)`, afegir el mateix bloc d'exploració del volum NAS que ja tenen `bonpreu.py` i `catgas.py` perquè el modal d'Històric mostri també les factures descarregades via Portal Web:
```python
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
```

### 1.2. Enriquir `write_factura_metrics` a `src/influx_writer.py` amb ràtios d'eficiència
Dins de `write_factura_metrics` a `src/influx_writer.py`, just abans del bloc `try: with InfluxDBClient...`, afegir el càlcul automàtic de cost diari i preu unitari:
```python
        # Càlcul automàtic de ràtios unitaris per a Grafana
        if metriques.dies_facturats and metriques.dies_facturats > 0:
            point.field("cost_diari_eur", round(float(metriques.import_total) / int(metriques.dies_facturats), 4))
        if metriques.consum_kwh and metriques.consum_kwh > 0:
            point.field("preu_kwh_real", round(float(metriques.import_total) / float(metriques.consum_kwh), 4))
        if metriques.consum_m3 and metriques.consum_m3 > 0:
            point.field("preu_m3_real", round(float(metriques.import_total) / float(metriques.consum_m3), 4))
```

---

## 2. ConfigMap del Dashboard de Grafana Corregit i Potenciat (amb Variació Interanual YoY)

### Millores clau del nou JSON de Grafana:
- **Fix del filtre `$proveidor = All`**: S'afegeix `"allValue": "All"` a la variable de template perquè el filtre funcioni tant en seleccionar `"All"` com un proveïdor concret.
- **Fix d'agrupació Flux (`|> group(...)`)**: Totes les consultes amb `aggregateWindow(every: 1mo, ...)` agrupen prèviament per `proveidor` o `_field` i usen `timeSrc: "_start"` per alinear cada barra al seu mes natural.
- **Nou Panell ID 13 — 🔄 Comparativa Interanual (YoY: Enguany vs. Any Passat)**: Superposa amb barres verdes la despesa mensual actual (refId `A`) i amb una línia taronja discontínua la despesa del mateix mes de fa 1 any (`|> timeShift(duration: 1y)`, refId `B`).
- **Nou Panell ID 14 — 📊 Variació Interanual (Últims 12m vs. 12m anteriors)**: Calcula la diferència percentual (%) entre la despesa acumulada dels darrers 12 mesos i la dels 12 mesos previs.
- **Nou Panell ID 12 — 📈 Cost Real Unitari de l'Energia (€/kWh Llum i Gas)**.

### Fitxer YAML complet del ConfigMap:

```yaml
---
# yamllint disable rule:line-length
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboard-subministres-factures
  namespace: monitoring
  labels:
    grafana_dashboard: "1"
data:
  06-subministres-factures.json: |
    {
      "annotations": { "list": [] },
      "editable": true,
      "fiscalYearStartMonth": 0,
      "graphTooltip": 1,
      "id": null,
      "links": [],
      "liveNow": false,
      "panels": [
        {
          "collapsed": false,
          "gridPos": { "h": 1, "w": 24, "x": 0, "y": 0 },
          "id": 100,
          "title": "💶 Resum de Despesa en Subministraments (Última Facturació, Acumulat & Variació Interanual)",
          "type": "row"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "mode": "thresholds" },
              "mappings": [],
              "thresholds": {
                "mode": "absolute",
                "steps": [
                  { "color": "green", "value": null },
                  { "color": "yellow", "value": 150 },
                  { "color": "red", "value": 250 }
                ]
              },
              "unit": "currencyEUR"
            },
            "overrides": []
          },
          "gridPos": { "h": 4, "w": 4, "x": 0, "y": 1 },
          "id": 1,
          "options": {
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
            "textMode": "auto"
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> group(columns: [\"proveidor\"])\n  |> last()\n  |> group()\n  |> sum()",
              "refId": "A"
            }
          ],
          "title": "💰 Total Últimes Factures",
          "type": "stat"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "fixedColor": "blue", "mode": "fixed" },
              "mappings": [],
              "thresholds": { "mode": "absolute", "steps": [{ "color": "blue", "value": null }] },
              "unit": "currencyEUR"
            },
            "overrides": []
          },
          "gridPos": { "h": 4, "w": 3, "x": 4, "y": 1 },
          "id": 2,
          "options": {
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
            "textMode": "auto"
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r.proveidor == \"veolia\" and r._field == \"import_total\")\n  |> group(columns: [\"proveidor\"])\n  |> sort(columns: [\"_time\"])\n  |> keep(columns: [\"_time\", \"_value\"])",
              "refId": "A"
            }
          ],
          "title": "💧 Aigua (Veolia)",
          "type": "stat"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "fixedColor": "orange", "mode": "fixed" },
              "mappings": [],
              "thresholds": { "mode": "absolute", "steps": [{ "color": "orange", "value": null }] },
              "unit": "currencyEUR"
            },
            "overrides": []
          },
          "gridPos": { "h": 4, "w": 3, "x": 7, "y": 1 },
          "id": 3,
          "options": {
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
            "textMode": "auto"
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r.proveidor == \"catgas\" and r._field == \"import_total\")\n  |> group(columns: [\"proveidor\"])\n  |> sort(columns: [\"_time\"])\n  |> keep(columns: [\"_time\", \"_value\"])",
              "refId": "A"
            }
          ],
          "title": "🔥 Gas (Catgas)",
          "type": "stat"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "fixedColor": "yellow", "mode": "fixed" },
              "mappings": [],
              "thresholds": { "mode": "absolute", "steps": [{ "color": "yellow", "value": null }] },
              "unit": "currencyEUR"
            },
            "overrides": []
          },
          "gridPos": { "h": 4, "w": 3, "x": 10, "y": 1 },
          "id": 4,
          "options": {
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
            "textMode": "auto"
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r.proveidor == \"bonpreu\" and r._field == \"import_total\")\n  |> group(columns: [\"proveidor\"])\n  |> sort(columns: [\"_time\"])\n  |> keep(columns: [\"_time\", \"_value\"])",
              "refId": "A"
            }
          ],
          "title": "⚡ Llum (Bonpreu)",
          "type": "stat"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "fixedColor": "purple", "mode": "fixed" },
              "mappings": [],
              "thresholds": { "mode": "absolute", "steps": [{ "color": "purple", "value": null }] },
              "unit": "currencyEUR"
            },
            "overrides": []
          },
          "gridPos": { "h": 4, "w": 3, "x": 13, "y": 1 },
          "id": 5,
          "options": {
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
            "textMode": "auto"
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r.proveidor == \"digi\" and r._field == \"import_total\")\n  |> group(columns: [\"proveidor\"])\n  |> sort(columns: [\"_time\"])\n  |> keep(columns: [\"_time\", \"_value\"])",
              "refId": "A"
            }
          ],
          "title": "📱 Fibra & Mòbil (Digi)",
          "type": "stat"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "fixedColor": "emerald", "mode": "fixed" },
              "mappings": [],
              "thresholds": { "mode": "absolute", "steps": [{ "color": "emerald", "value": null }] },
              "unit": "currencyEUR"
            },
            "overrides": []
          },
          "gridPos": { "h": 4, "w": 4, "x": 16, "y": 1 },
          "id": 11,
          "options": {
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": { "calcs": ["sum"], "fields": "", "values": false },
            "textMode": "auto"
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> filter(fn: (r) => \"$proveidor\" == \"All\" or r.proveidor == \"$proveidor\")\n  |> group()\n  |> sum()",
              "refId": "A"
            }
          ],
          "title": "🏦 Acumulat (Període)",
          "type": "stat"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "mode": "thresholds" },
              "decimals": 1,
              "mappings": [],
              "thresholds": {
                "mode": "absolute",
                "steps": [
                  { "color": "green", "value": null },
                  { "color": "yellow", "value": 0 },
                  { "color": "red", "value": 10 }
                ]
              },
              "unit": "percent"
            },
            "overrides": []
          },
          "gridPos": { "h": 4, "w": 4, "x": 20, "y": 1 },
          "id": 14,
          "options": {
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
            "textMode": "auto"
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "curr = from(bucket: \"energy_data\")\n  |> range(start: -1y, stop: now())\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> filter(fn: (r) => \"$proveidor\" == \"All\" or r.proveidor == \"$proveidor\")\n  |> group()\n  |> sum()\n  |> map(fn: (r) => ({ r with join_key: \"k\" }))\n\nprev = from(bucket: \"energy_data\")\n  |> range(start: -2y, stop: -1y)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> filter(fn: (r) => \"$proveidor\" == \"All\" or r.proveidor == \"$proveidor\")\n  |> group()\n  |> sum()\n  |> map(fn: (r) => ({ r with join_key: \"k\" }))\n\njoin(tables: {curr: curr, prev: prev}, on: [\"join_key\"])\n  |> filter(fn: (r) => r._value_prev > 0.0)\n  |> map(fn: (r) => ({ _value: ((r._value_curr - r._value_prev) / r._value_prev) * 100.0 }))",
              "refId": "A"
            }
          ],
          "title": "🔄 Variació Interanual (12m YoY)",
          "type": "stat"
        },
        {
          "collapsed": false,
          "gridPos": { "h": 1, "w": 24, "x": 0, "y": 5 },
          "id": 101,
          "title": "📊 Evolució Històrica, Distribució i Comparativa Interanual (YoY)",
          "type": "row"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "custom": {
                "drawStyle": "bars",
                "fillOpacity": 85,
                "gradientMode": "none",
                "lineWidth": 1,
                "stacking": { "group": "A", "mode": "normal" }
              },
              "mappings": [],
              "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] },
              "unit": "currencyEUR"
            },
            "overrides": [
              { "matcher": { "id": "byName", "options": "veolia" }, "properties": [{ "id": "color", "value": { "fixedColor": "#3b82f6", "mode": "fixed" } }, { "id": "displayName", "value": "💧 Veolia (Aigua)" }] },
              { "matcher": { "id": "byName", "options": "catgas" }, "properties": [{ "id": "color", "value": { "fixedColor": "#f97316", "mode": "fixed" } }, { "id": "displayName", "value": "🔥 Catgas (Gas)" }] },
              { "matcher": { "id": "byName", "options": "bonpreu" }, "properties": [{ "id": "color", "value": { "fixedColor": "#eab308", "mode": "fixed" } }, { "id": "displayName", "value": "⚡ Bonpreu (Llum)" }] },
              { "matcher": { "id": "byName", "options": "digi" }, "properties": [{ "id": "color", "value": { "fixedColor": "#a855f7", "mode": "fixed" } }, { "id": "displayName", "value": "📱 Digi (Fibra/Mòbil)" }] }
            ]
          },
          "gridPos": { "h": 8, "w": 16, "x": 0, "y": 6 },
          "id": 6,
          "options": {
            "legend": { "calcs": ["sum", "mean"], "displayMode": "table", "placement": "right", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> filter(fn: (r) => \"$proveidor\" == \"All\" or r.proveidor == \"$proveidor\")\n  |> group(columns: [\"proveidor\"])\n  |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false, timeSrc: \"_start\")",
              "refId": "A"
            }
          ],
          "title": "📅 Despesa Mensual Apilada per Proveïdor (€)",
          "type": "timeseries"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "mode": "palette-classic" },
              "mappings": [],
              "unit": "currencyEUR"
            },
            "overrides": [
              { "matcher": { "id": "byName", "options": "veolia" }, "properties": [{ "id": "color", "value": { "fixedColor": "#3b82f6", "mode": "fixed" } }] },
              { "matcher": { "id": "byName", "options": "catgas" }, "properties": [{ "id": "color", "value": { "fixedColor": "#f97316", "mode": "fixed" } }] },
              { "matcher": { "id": "byName", "options": "bonpreu" }, "properties": [{ "id": "color", "value": { "fixedColor": "#eab308", "mode": "fixed" } }] },
              { "matcher": { "id": "byName", "options": "digi" }, "properties": [{ "id": "color", "value": { "fixedColor": "#a855f7", "mode": "fixed" } }] }
            ]
          },
          "gridPos": { "h": 8, "w": 8, "x": 16, "y": 6 },
          "id": 7,
          "options": {
            "displayLabels": ["percent", "value"],
            "legend": { "displayMode": "table", "placement": "bottom", "values": ["value", "percent"] },
            "pieType": "donut",
            "reduceOptions": { "calcs": ["sum"], "fields": "", "values": false },
            "tooltip": { "mode": "single" }
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> group(columns: [\"proveidor\"])\n  |> sum()",
              "refId": "A"
            }
          ],
          "title": "🥧 Repartiment Total de la Despesa",
          "type": "piechart"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "custom": {
                "drawStyle": "bars",
                "fillOpacity": 65,
                "gradientMode": "none",
                "lineWidth": 2
              },
              "mappings": [],
              "unit": "currencyEUR"
            },
            "overrides": [
              {
                "matcher": { "id": "byName", "options": "Període Actual" },
                "properties": [
                  { "id": "color", "value": { "fixedColor": "#10b981", "mode": "fixed" } },
                  { "id": "custom.drawStyle", "value": "bars" }
                ]
              },
              {
                "matcher": { "id": "byName", "options": "Any Anterior (YoY)" },
                "properties": [
                  { "id": "color", "value": { "fixedColor": "#f59e0b", "mode": "fixed" } },
                  { "id": "custom.drawStyle", "value": "line" },
                  { "id": "custom.lineStyle", "value": { "dash": [10, 6], "fill": "dash" } },
                  { "id": "custom.fillOpacity", "value": 10 },
                  { "id": "custom.showPoints", "value": "always" }
                ]
              }
            ]
          },
          "gridPos": { "h": 7, "w": 24, "x": 0, "y": 14 },
          "id": 13,
          "options": {
            "legend": { "calcs": ["sum", "mean"], "displayMode": "table", "placement": "right", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> filter(fn: (r) => \"$proveidor\" == \"All\" or r.proveidor == \"$proveidor\")\n  |> group()\n  |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false, timeSrc: \"_start\")\n  |> map(fn: (r) => ({ r with serie: \"Període Actual\" }))\n  |> group(columns: [\"serie\"])",
              "refId": "A"
            },
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "import \"experimental\"\nfrom(bucket: \"energy_data\")\n  |> range(start: experimental.subDuration(d: 1y, from: v.timeRangeStart), stop: experimental.subDuration(d: 1y, from: v.timeRangeStop))\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"import_total\")\n  |> filter(fn: (r) => \"$proveidor\" == \"All\" or r.proveidor == \"$proveidor\")\n  |> group()\n  |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false, timeSrc: \"_start\")\n  |> timeShift(duration: 1y, columns: [\"_start\", \"_stop\", \"_time\"])\n  |> map(fn: (r) => ({ r with serie: \"Any Anterior (YoY)\" }))\n  |> group(columns: [\"serie\"])",
              "refId": "B"
            }
          ],
          "title": "🔄 Comparativa Interanual Mensual (YoY: Període Actual vs. Mateix Mes de l'Any Anterior)",
          "type": "timeseries"
        },
        {
          "collapsed": false,
          "gridPos": { "h": 1, "w": 24, "x": 0, "y": 21 },
          "id": 102,
          "title": "⚡💧🔥 Consums Físics i Preus Unitaris Reals",
          "type": "row"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "custom": {
                "drawStyle": "bars",
                "fillOpacity": 75,
                "gradientMode": "none",
                "lineWidth": 1,
                "stacking": { "group": "A", "mode": "normal" }
              },
              "mappings": [],
              "unit": "kwh"
            },
            "overrides": [
              { "matcher": { "id": "byName", "options": "consum_kwh_punta" }, "properties": [{ "id": "color", "value": { "fixedColor": "#ef4444", "mode": "fixed" } }, { "id": "displayName", "value": "🔴 P1 Punta" }] },
              { "matcher": { "id": "byName", "options": "consum_kwh_pla" }, "properties": [{ "id": "color", "value": { "fixedColor": "#f59e0b", "mode": "fixed" } }, { "id": "displayName", "value": "🟡 P2 Pla" }] },
              { "matcher": { "id": "byName", "options": "consum_kwh_vall" }, "properties": [{ "id": "color", "value": { "fixedColor": "#10b981", "mode": "fixed" } }, { "id": "displayName", "value": "🟢 P3 Vall" }] }
            ]
          },
          "gridPos": { "h": 8, "w": 8, "x": 0, "y": 22 },
          "id": 8,
          "options": {
            "legend": { "calcs": ["sum", "mean"], "displayMode": "table", "placement": "bottom", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r.proveidor == \"bonpreu\")\n  |> filter(fn: (r) => r._field == \"consum_kwh_punta\" or r._field == \"consum_kwh_pla\" or r._field == \"consum_kwh_vall\")\n  |> group(columns: [\"_field\"])\n  |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false, timeSrc: \"_start\")",
              "refId": "A"
            }
          ],
          "title": "⚡ Consum Elèctric Bonpreu per Trams (kWh)",
          "type": "timeseries"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "custom": {
                "drawStyle": "bars",
                "fillOpacity": 70,
                "gradientMode": "none",
                "lineWidth": 1
              },
              "mappings": [],
              "unit": "m3"
            },
            "overrides": [
              { "matcher": { "id": "byName", "options": "veolia" }, "properties": [{ "id": "color", "value": { "fixedColor": "#0284c7", "mode": "fixed" } }, { "id": "displayName", "value": "💧 Aigua Veolia (m³)" }] },
              { "matcher": { "id": "byName", "options": "catgas" }, "properties": [{ "id": "color", "value": { "fixedColor": "#ea580c", "mode": "fixed" } }, { "id": "displayName", "value": "🔥 Gas Catgas (m³)" }] }
            ]
          },
          "gridPos": { "h": 8, "w": 8, "x": 8, "y": 22 },
          "id": 9,
          "options": {
            "legend": { "calcs": ["sum", "mean"], "displayMode": "table", "placement": "bottom", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and r._field == \"consum_m3\")\n  |> group(columns: [\"proveidor\"])\n  |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false, timeSrc: \"_start\")",
              "refId": "A"
            }
          ],
          "title": "💧🔥 Consums de Fluid: Aigua & Gas (m³)",
          "type": "timeseries"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "custom": {
                "drawStyle": "line",
                "fillOpacity": 20,
                "lineInterpolation": "smooth",
                "lineWidth": 2,
                "showPoints": "always"
              },
              "decimals": 3,
              "mappings": [],
              "unit": "currencyEUR"
            },
            "overrides": [
              { "matcher": { "id": "byName", "options": "bonpreu" }, "properties": [{ "id": "color", "value": { "fixedColor": "#eab308", "mode": "fixed" } }, { "id": "displayName", "value": "⚡ Llum (€/kWh tot inclòs)" }] },
              { "matcher": { "id": "byName", "options": "catgas" }, "properties": [{ "id": "color", "value": { "fixedColor": "#f97316", "mode": "fixed" } }, { "id": "displayName", "value": "🔥 Gas (€/kWh tot inclòs)" }] }
            ]
          },
          "gridPos": { "h": 8, "w": 8, "x": 16, "y": 22 },
          "id": 12,
          "options": {
            "legend": { "calcs": ["mean", "lastNotNull"], "displayMode": "table", "placement": "bottom", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\" and (r._field == \"import_total\" or r._field == \"consum_kwh\"))\n  |> pivot(rowKey: [\"_time\", \"numero_factura\", \"proveidor\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n  |> filter(fn: (r) => exists r.consum_kwh and r.consum_kwh > 0.0)\n  |> map(fn: (r) => ({ _time: r._time, proveidor: r.proveidor, _value: r.import_total / r.consum_kwh }))\n  |> group(columns: [\"proveidor\"])\n  |> sort(columns: [\"_time\"])",
              "refId": "A"
            }
          ],
          "title": "📈 Cost Real de l'Energia (€/kWh amb impostos i fix)",
          "type": "timeseries"
        },
        {
          "collapsed": false,
          "gridPos": { "h": 1, "w": 24, "x": 0, "y": 30 },
          "id": 103,
          "title": "📋 Detall Històric de Factures",
          "type": "row"
        },
        {
          "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
          "fieldConfig": {
            "defaults": {
              "color": { "mode": "thresholds" },
              "custom": { "align": "auto", "displayMode": "auto" },
              "mappings": []
            },
            "overrides": [
              { "matcher": { "id": "byName", "options": "import_total" }, "properties": [{ "id": "unit", "value": "currencyEUR" }, { "id": "displayName", "value": "Import Total" }] },
              { "matcher": { "id": "byName", "options": "consum_kwh" }, "properties": [{ "id": "unit", "value": "kwh" }, { "id": "displayName", "value": "Consum (kWh)" }] },
              { "matcher": { "id": "byName", "options": "consum_m3" }, "properties": [{ "id": "unit", "value": "m3" }, { "id": "displayName", "value": "Consum (m³)" }] },
              { "matcher": { "id": "byName", "options": "proveidor" }, "properties": [{ "id": "custom.displayMode", "value": "color-text" }, { "id": "displayName", "value": "Proveïdor" }] },
              { "matcher": { "id": "byName", "options": "numero_factura" }, "properties": [{ "id": "displayName", "value": "Núm. Factura" }] }
            ]
          },
          "gridPos": { "h": 9, "w": 24, "x": 0, "y": 31 },
          "id": 10,
          "options": {
            "footer": { "countRows": false, "fields": ["import_total", "consum_kwh", "consum_m3"], "reducer": ["sum"], "show": true },
            "showHeader": true,
            "sortBy": [{ "desc": true, "displayName": "Time" }]
          },
          "pluginVersion": "10.0.0",
          "targets": [
            {
              "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
              "query": "from(bucket: \"energy_data\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"factures_subministres\")\n  |> filter(fn: (r) => \"$proveidor\" == \"All\" or r.proveidor == \"$proveidor\")\n  |> pivot(rowKey:[\"_time\", \"numero_factura\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n  |> group()\n  |> sort(columns: [\"_time\"], desc: true)",
              "refId": "A"
            }
          ],
          "title": "📑 Registre de Factures Processades",
          "type": "table"
        }
      ],
      "refresh": "1m",
      "schemaVersion": 39,
      "tags": ["subministres", "factures", "energia", "influxdb", "aigua", "gas", "telefonia"],
      "templating": {
        "list": [
          {
            "allValue": "All",
            "current": { "selected": true, "text": "All", "value": "$__all" },
            "datasource": { "type": "influxdb", "uid": "PD04B56A68C70BDF8" },
            "definition": "import \"influxdata/influxdb/schema\"\nschema.tagValues(bucket: \"energy_data\", tag: \"proveidor\", predicate: (r) => r._measurement == \"factures_subministres\")",
            "hide": 0,
            "includeAll": true,
            "label": "Proveïdor",
            "multi": false,
            "name": "proveidor",
            "query": "import \"influxdata/influxdb/schema\"\nschema.tagValues(bucket: \"energy_data\", tag: \"proveidor\", predicate: (r) => r._measurement == \"factures_subministres\")",
            "refresh": 1,
            "type": "query"
          }
        ]
      },
      "time": { "from": "now-1y", "to": "now" },
      "timepicker": {
        "refresh_intervals": ["1m", "5m", "15m", "1h", "6h", "1d"]
      },
      "timezone": "browser",
      "title": "Subministres & Facturació Domèstica",
      "uid": "subministres-facturacio",
      "version": 3
    }
```

```