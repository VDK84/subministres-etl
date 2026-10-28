# subministres-etl 💧⚡🔥📱

[![Build, Scan & Publish Docker Image](https://github.com/VDK84/subministres-etl/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/VDK84/subministres-etl/actions/workflows/docker-publish.yml)
[![CodeQL Analysis](https://github.com/VDK84/subministres-etl/actions/workflows/codeql.yml/badge.svg)](https://github.com/VDK84/subministres-etl/actions/workflows/codeql.yml)
[![SonarCloud Analysis](https://github.com/VDK84/subministres-etl/actions/workflows/sonarcloud.yml/badge.svg)](https://github.com/VDK84/subministres-etl/actions/workflows/sonarcloud.yml)

ETL autònom, modular i reactiu per a la recol·lecció, processament, emmagatzematge al NAS Samba i ingesta a **InfluxDB v2** de factures de subministraments de la llar (**Aigua, Gas, Llum i Telefonia**).

Dissenyat per executar-se com a contenidor web i **CronJob** a Kubernetes (**K3s**) gestionat via GitOps amb **ArgoCD**, protegint els secrets mitjançant **OpenBao** i **External Secrets Operator (ESO)**.

---

## 🏛️ Arquitectura i Estratègia d'Extracció

L'aplicació implementa una estratègia híbrida intel·ligent que optimitza recursos, preserva la privacitat i garanteix la descàrrega autònoma de tots els documents:

1. **Privacitat i bústia dedicada**:
   - Es crea una bústia de correu dedicada exclusivament a subministraments.
   - S'aplica una regla de reenviament (*forwarding*) des del correu personal cap a aquesta bústia només per als correus dels proveïdors. D'aquesta manera, l'aplicació no té mai accés al correu personal.
   - **Optimització IMAP Header Peek**: El client IMAP consulta només les capçaleres de text (`BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)]`) en mil·lisegons i descarrega exclusivament el cos complet dels correus que realment són factures candidates.

2. **Doble canal d'extracció per a tots els proveïdors (Correu vs. Portal Web)**:
   - **Veolia (Aigua)**:
     - *Canal Correu*: Detecció i descàrrega directa de fitxers adjunts PDF.
     - *Canal Portal*: Scraping amb Chromium headless a l'Oficina Virtual d'Agbar/Veolia (`https://agbar.veolia.cat/ca/group/sgab/mis-facturas`), accés a cada vista de detall i descàrrega del PDF oficial. Suport per a 2FA per email i gestió de facturació trimestral (`T1..T4`).
   - **Catgas Energia (Gas Natural)**:
     - *Canal Correu*: Extracció de factures adjuntes en PDF.
     - *Canal Portal*: Scraping a l'Oficina Virtual de Catgas (`https://oficinavirtual.catgas.cat`), selecció automàtica de la pestanya de factures de **Gas** i descàrrega multipàgina dels PDFs oficials.
   - **Bonpreu Esclat Energia (Electricitat)**:
     - *Canal Correu*: Reconeixement de correus d'avís de factura per a ingesta de contingut.
     - *Canal Portal*: Scraping a l'Àrea de Clients de Bonpreu (`https://energia.bonpreuesclat.cat/factures`) mitjançant Keycloak, **resolució automàtica del codi 2FA d'un sol ús (OTP)** des de la bústia de correu, navegació multipàgina (`SEGÜENT`) i descàrrega de tots els PDFs.
   - **Digi Mobil (Telefonia / Fibra)**:
     - *Canal Correu*: Reconeixement d'avisos i factures adjuntes.
     - *Canal Portal*: Scraping al portal Mi Digi (`https://midigi.digimobil.es/invoice`) amb descàrrega de documents oficials. Suport avançat per a descodificació de PDFs antics amb mapa de glifs desplaçat (Type 3 / Identity-H).

3. **Persistència de Sessió al NAS i Gestió de 2FA (Zero Bloquejos)**:
   - **Sessió persistent (`storage_state`)**: Les cookies i tokens de sessió dels portals es guarden al NAS a `/data/factures/.sessions/`. D'aquesta manera, les execucions periòdiques entren directament a l'àrea de factures sense tornar a fer login ni demanar 2FA durant setmanes o mesos.
   - **Detecció i captura de 2FA**: Si la sessió caduca i el portal requereix codi 2FA per correu, el sistema el cerca automàticament a la bústia IMAP i l'introdueix sense intervenció humana. Si requereix SMS, desa una captura de seguretat a `.sessions/` i deixa el connector en estat `2FA_REQUIRED`.
   - **Protecció amb Circuit Breaker**: Si es produeixen 3 intents fallits consecutius, el connector activa automàticament una pausa preventiva de 60 minuts per protegir el compte de bloquejos.

---

## 🏥 Centre de Salut i Incidències (`src/health.py`)

L'aplicació incorpora un sistema d'observabilitat persistent compartit entre el **CronJob nocturn** de Kubernetes i la **Web UI**:

- **Persistència compartida al NAS**: L'estat de salut, les dates d'últim èxit i l'històric d'incidències es desen a `/data/factures/.status/health_state.json`. Encara que el pod es reiniciï, la informació no es perd.
- **Classificador intel·ligent d'incidències**:
  - 🔐 **`2FA_REQUIRED`**: Verificació 2FA requerida (SMS o codi de correu pendent).
  - 🔑 **`AUTH_ERROR`**: Error de credencials o formulari de login.
  - 📄 **`PARSER_ERROR`**: Possibles canvis de disseny o format en el PDF.
  - ⏱️ **`TIMEOUT`**: Temps d'espera esgotat al portal web de la companyia.
  - 🗄️ **`INFLUX_ERROR`**: Error d'escriptura a InfluxDB.
  - ⛔ **`CIRCUIT_BREAKER`**: Pausa preventiva per intents fallits consecutius.
- **Control Visual Global i per Proveïdor**:
  - **Header Global**: Botó `✅ Sistema OK` (verd) o `⚠️ N Incidències` (ambre/vermell) amb accés al panell central d'incidències.
  - **Cards Superiors**: Insígnia individual clicable (`OK` / `⚠️ N`) i data exacta d'**`Últim èxit:`** (ex: `2026-10-25 07:52`).
  - **Botó "Netejar incidències"**: Acció per marcar els avisos resolts amb un sol clic.

---

## 📦 Destinacions de les Dades

### 1. NAS Samba (`//192.168.1.2/hdd2404/factures`)

Els PDFs es guarden organitzats de forma estructurada, deduplicada i ordenada cronològicament:

```text
/data/factures/
├── aigua/
│   └── veolia/
│       └── 2026/
│           └── 2026_10_veolia_22272026A100046989.pdf
├── gas/
│   └── catgas/
│       └── 2026/
│           └── 2026_10_catgas_FG26086862.pdf
├── electricitat/
│   └── bonpreu/
│       └── 2026/
│           └── 2026_10_bonpreu_F26612441.pdf
├── telefonia/
│   └── digi/
│       └── 2026/
│           └── 2026_09_digi_DGFC2629430164.pdf
└── .status/
    └── health_state.json
```

### 2. InfluxDB v2 (Sèries Temporals per a Dashboards de Grafana)

Les dades s'envien al bucket `energy_data` amb el measurement `factures_subministres`:

- **Tags**:
  - `proveidor`: `veolia`, `catgas`, `bonpreu`, `digi`
  - `tipus`: `aigua`, `gas`, `electricitat`, `telefonia`
  - `numero_factura`: número identificatiu oficial
  - `any`: YYYY
  - `mes`: MM
  - `trimestre`: `T1`, `T2`, `T3`, `T4` (per a factures trimestrals)
  - `referencia`: CUPS o número de contracte/pòlissa
- **Fields**:
  - `import_total`: Import total facturat en €
  - `import_base`, `import_iva`: Desglossament monetari
  - `consum_m3`: Metres cúbics consumits (aigua i gas)
  - `consum_kwh`: Consum total elèctric o de gas
  - `consum_kwh_punta` (P1), `consum_kwh_pla` (P2), `consum_kwh_vall` (P3)
  - `potencia_contractada_kw`: Potència contractada
  - `total_gb_consumits`: Dades mòbils consumides
  - `num_linies_mobils`: Nombre de línies mòbils
  - `dies_facturats`: Nombre de dies del període
  - **Mètriques derivades calculades automàticament**:
    - `cost_diari_eur`: `import_total / dies_facturats` (€/dia)
    - `preu_kwh_real`: `import_total / consum_kwh` (€/kWh tot inclòs)
    - `preu_m3_real`: `import_total / consum_m3` (€/m³)

---

## 📊 Dashboard de Grafana CaC (18 Panells)

El repositori `argocd-bootstrap` gestiona de manera declarativa el ConfigMap `grafana-dashboard-subministres-factures`:

- **Targetes de Resum de la Darrera Factura**:
  - Consulta independent amb finestra ampliada `range(start: -2y, stop: now())` i `last()` perquè la targeta mostri sempre la darrera factura emesa d'Aigua, Gas, Llum i Fibra sense dependre del zoom temporal.
- **🔄 Panell ID 13 — Comparativa Interanual Mensual (YoY: Enguany vs. Any Passat)**:
  - Superposa amb **barres verdes** la despesa de cada mes d'enguany i amb una **línia discontínua taronja** (`timeShift(duration: 1y)`) la despesa exacta del mateix mes de l'any anterior.
- **📊 Panell ID 14 — KPI Variació Interanual (% YoY)**:
  - Percentatge d'increment o estalvi entre els darrers 12 mesos i els 12 mesos anteriors amb colors semafòrics.
- **📈 Panell ID 12 — Cost Real Unitari de l'Energia (€/kWh tot inclòs)**:
  - Evolució del preu real del kWh d'electricitat (Bonpreu) i gas (Catgas).
- **Filtre desplegable de proveïdor**: Suport complet per a `"All"` amb `"allValue": "All"`.

---

## 🖥️ Interfície Web Operativa (FastAPI + Tailwind CSS + WebSocket)

La interfície web a `https://subministres.rusc.local` incorpora controls avançats de gestió:

- **Canals independents per Targeta**:
  - `📧 Correu`: Cerca ultraràpida a la bústia IMAP (no aixeca navegador).
  - `🌐 Portal Web`: Scraping directe del portal oficial amb Playwright.
  - `📚 Explorar Històric (730 dies)`: Modal amb el catàleg de totes les factures i botó "Sincronitzar Tot l'Històric" (`mode="all"`).
- **Recompte de Factures per Proveïdor**: Indicador verd en font mono a cada targeta (ex: `16 factures`, `57 factures`).
- **Traçabilitat en Temps Real per WebSocket (`/ws/logs`)**:
  - Connexió nativa sense polling HTTP, amb heartbeat automàtic (`ping` cada 20s).
  - Barra de progrés visual amb comptador `X / N (percentatge%)`, acció actual i avís immediat de timeouts.
  - Banner automàtic de recàrrega de pàgina quan es detecta una nova versió del servidor.
- **Filtre per Rang de Dates i Mode "⭐ Últimes factures"**:
  - Controls de calendari natiu `De:` i `Fins:` que filtren coherentment per mes (`YYYY-MM`).
  - Botó toggle "⭐ Últimes factures" que aïlla la darrera factura de cada proveïdor independentment de l'ordre de les columnes.
  - Botó de reset per netejar la cerca i els filtres amb un sol clic.
- **Eina de Reparació del NAS (`🔧 Reparar PDFs`)**:
  - Re-analitza tots els documents del disc, reanomena i mou a la carpeta corresponent els fitxers amb data desalineada i purga InfluxDB abans de re-ingerir les dades netes.
- **Pujada Multi-format (PDF, EML, ZIP)**: Dropzone amb desempaquetament recursiu a memòria, filtratge per magic bytes `%PDF` i límit de seguretat de 50 MB contra Zip Bombs.
- **Control d'Accessibilitat (A- / A+)**: Zoom de font (100% a 150%) amb persistència a `localStorage`.

### Endpoints de l'API Web

| Mètode | Endpoint | Descripció |
| :--- | :--- | :--- |
| `GET` | `/` | Pàgina principal del panell de control |
| `POST` | `/api/sync` | Dispara sincronització (`connector`, `mode`, `source="email"\|"portal"\|"auto"`) |
| `POST` | `/api/sync/latest` | Cerca i descarrega exclusivament l'última factura |
| `GET` | `/api/history/{conn}` | Modal d'històric i comparativa amb el NAS (730 dies) |
| `GET` | `/api/incidents` | Retorna el contingut del Pop-up del Centre d'Incidències |
| `POST` | `/api/incidents/clear` | Neteja les incidències actives d'un o tots els proveïdors |
| `POST` | `/api/reparse` | Re-analitza els PDFs del NAS, corregeix noms/dates i purga InfluxDB |
| `GET` | `/ws/logs` | Canal WebSocket bidireccional per a logs en viu, progrés X/N i heartbeat |
| `POST` | `/api/upload` | Pujada de fitxers (.pdf, .eml, .zip) amb classificació automàtica |
| `DELETE` | `/api/invoice/{path}` | Elimina el PDF del NAS Samba i suprimeix les mètriques d'InfluxDB |
| `GET` | `/api/pdf/view/{path}` | Serveix el fitxer PDF inline per al visor del navegador |
| `GET` | `/api/pdf/download/{path}` | Descarrega el fitxer PDF directament |

---

## 🔐 Configuració de Secrets a OpenBao

Els secrets es sincronitzen a Kubernetes mitjançant **External Secrets Operator (ESO)** contra el `ClusterSecretStore` anomenat `openbao-cluster-secret-store`.

A la ruta `rusc/data/subministres` d'OpenBao es defineixen les claus:

```json
{
  "imap_host": "imap.el-teu-proveidor.com",
  "imap_user": "subministres@el-teu-domini.cat",
  "imap_password": "contrasenya_o_app_password",
  "bonpreu_user": "usuari_bonpreu",
  "bonpreu_password": "password_bonpreu",
  "digi_user": "telefon_o_email_digi",
  "digi_password": "password_digi",
  "veolia_user": "email_agbar_veolia",
  "veolia_password": "password_veolia",
  "catgas_user": "nif_catgas",
  "catgas_password": "password_catgas"
}
```

---

## ☸️ Desplegament GitOps a K3s (ArgoCD)

Al repositori `argocd-bootstrap` es troba la declaració completa del servei:

- **`apps/42-subministres-etl.yaml`**: ArgoCD Application (syncWave: `3`, namespace: `subministres`).
- **`manifests/subministres-etl/`**:
  - `01-external-secret-samba.yaml`: Credencials SMB per muntar el recurs Samba.
  - `02-external-secret-app.yaml`: Credencials de l'aplicació des d'OpenBao.
  - `03-pv-samba-rw.yaml` & `04-pvc-samba-rw.yaml`: Volum persistent SMB (`//192.168.1.2/hdd2404/factures`).
  - `05-configmap.yaml`: Configuració no sensible de l'aplicació.
  - `05-rbac.yaml`: `ServiceAccount`, `Role` i `RoleBinding` per a l'auto-anotació nativa del Pod en arrencar (`app.kubernetes.io/version`).
  - `06-deployment.yaml`: Deployment del servidor web FastAPI + Uvicorn corrent com a usuari no-root `appuser` (UID 1000).
  - `07-service.yaml` & `08-ingressroute.yaml`: Exposició interna i Ingress Traefik HTTPS a `https://subministres.rusc.local`.
  - `09-cronjob.yaml`: CronJob diari a les 06:00 UTC amb comanda explícita `command: ["subministres-etl"]`.

---

## 🐳 Pipeline DevSecOps (GitHub Actions)

El repositori disposa d'un pipeline complet i automatitzat de DevSecOps:

1. **Automated Semantic Versioning**: Increment automàtic de versió seguint *Conventional Commits* (`feat:` ➔ minor, `fix:` ➔ patch), etiquetatge Git (`vX.Y.Z`) i creació de GitHub Releases amb Changelog.
2. **Hardening Docker**: La imatge Docker s'executa com a usuari no-root `appuser` (UID 1000) i purga compiladors (`gcc`, `libffi-dev`, `libssl-dev`) després de la instal·lació.
3. **Publicació a GHCR**: Construcció multi-etapa amb Buildx i publicació directa a `ghcr.io/vdk84/subministres-etl`.
4. **Trivy Container Scan**: Escaneig de vulnerabilitats CVEs i publicació d'informes SARIF a GitHub Security.
5. **GitHub CodeQL (SAST)**: Anàlisi estàtica de seguretat de codi per a Python.
6. **SonarCloud**: Anàlisi continu de qualitat, mantenibilitat i cobertura.
7. **Dependabot**: Alertes i actualitzacions automàtiques de dependències.
