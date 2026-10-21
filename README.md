# subministres-etl 💧⚡🔥📱

[![Build, Scan & Publish Docker Image](https://github.com/VDK84/subministres-etl/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/VDK84/subministres-etl/actions/workflows/docker-publish.yml)
[![CodeQL Analysis](https://github.com/VDK84/subministres-etl/actions/workflows/codeql.yml/badge.svg)](https://github.com/VDK84/subministres-etl/actions/workflows/codeql.yml)
[![SonarCloud Analysis](https://github.com/VDK84/subministres-etl/actions/workflows/sonarcloud.yml/badge.svg)](https://github.com/VDK84/subministres-etl/actions/workflows/sonarcloud.yml)

ETL autònom i modular per a la recol·lecció, processament, emmagatzematge al NAS Samba i ingesta a **InfluxDB v2** de factures de subministraments de la llar (**Aigua, Gas, Llum i Telefonia**).

Dissenyat per executar-se com a contenidor i **CronJob** a Kubernetes (**K3s**) gestionat via GitOps amb **ArgoCD**, protegint els secrets mitjançant **OpenBao** i **External Secrets Operator (ESO)**.

---

## 🏛️ Arquitectura i Estratègia d'Extracció

L'aplicació implementa una estratègia híbrida intel·ligent que optimitza recursos i preserva la privacitat:

1. **Privacitat i bústia dedicada**:
   - Es crea una bústia de correu dedicada exclusivament a subministraments.
   - S'aplica una regla de reenviament (forwarding) des del correu personal cap a aquesta bústia només per als correus dels proveïdors. D'aquesta manera, l'aplicació no té mai accés al correu personal.

2. **Extracció directa per correu (IMAP amb PDF adjunt)**:
   - **Veolia (Aigua)**: La factura arriba directament com a fitxer adjunt PDF.
   - **Catgas Energia (Gas Natural)**: La factura arriba com a fitxer adjunt PDF.
   - *Flux*: El client IMAP detecta el correu del proveïdor, descarrega el fitxer adjunt, n'extreu el text i regex de consum ($m^3$, $kWh$), dates de període i imports, i l'envia al NAS i InfluxDB.

3. **Trigger per correu + Scraping web del portal de clients**:
   - **Bonpreu Esclat Energia (Electricitat)**: Bonpreu només notifica l'import i emissió per correu sense adjuntar el PDF.
   - **Digi Mobil (Telefonia / Fibra)**: Digi no envia la factura en PDF per correu, només avisa de l'import mensual.
   - *Flux*: La recepció del correu d'avís funciona com a disparador (trigger). El connector s'autentica al portal privat de clients (`https://clients.energia.bonpreu.cat/` o `https://mi.digimobil.es/`) mitjançant **Playwright** (Chromium headless), descarrega el PDF oficial, en parseja el detall (per exemple el desglossament P1/P2/P3 de llum o gigues de mòbil) i l'emmagatzema.

---

## 📦 Destinacions de les Dades

### 1. NAS Samba (`//192.168.1.2/hdd2404/factures`)
Els PDFs es guarden organitzats de forma estructurada i deduplicada (per nom i hash SHA256):

```text
/data/factures/
├── aigua/
│   └── veolia/
│       └── 2026/
│           └── 2026_09_veolia_B12345678.pdf
├── gas/
│   └── catgas/
│       └── 2026/
│           └── 2026_09_catgas_FG2026_1234.pdf
├── electricitat/
│   └── bonpreu/
│       └── 2026/
│           └── 2026_09_bonpreu_2026E1234.pdf
└── telefonia/
    └── digi/
        └── 2026/
            └── 2026_09_digi_FA2600123.pdf
```

### 2. InfluxDB v2 (Sèries Temporals per a Dashboards de Grafana)
Les dades s'envien al bucket `energy_data` (o configurable) amb el measurement `factures_subministres`:

- **Tags**:
  - `proveidor`: `veolia`, `catgas`, `bonpreu`, `digi`
  - `tipus`: `aigua`, `gas`, `electricitat`, `telefonia`
  - `numero_factura`: número identificatiu oficial
  - `any`: YYYY
  - `mes`: MM
  - `titular`: Nom del titular (opcional)
  - `referencia`: CUPS o número de contracte/pòlissa
- **Fields**:
  - `import_total`: Import total facturat en € (float)
  - `import_base`, `import_iva`: Desglossament monetari
  - `consum_m3`: Metres cúbics consumits (aigua i gas)
  - `consum_kwh`: Consum total elèctric o de gas
  - `consum_kwh_punta` (P1), `consum_kwh_pla` (P2), `consum_kwh_vall` (P3)
  - `potencia_contractada_kw`: Potència elèctrica contractada
  - `total_gb_consumits`: Dades mòbils consumides
  - `num_linies_mobils`: Nombre de línies mòbils facturades
  - `dies_facturats`: Nombre de dies del període de facturació

---

## 🔐 Configuració de Secrets a OpenBao

Els secrets es sincronitzen a Kubernetes de manera declarativa mitjançant **External Secrets Operator (ESO)** contra el `ClusterSecretStore` anomenat `openbao-cluster-secret-store`.

A la ruta `rusc/data/subministres` d'OpenBao s'han de definir les claus següents:

```json
{
  "imap_host": "imap.el-teu-proveidor.com",
  "imap_user": "subministres@el-teu-domini.cat",
  "imap_password": "contrasenya_o_app_password",
  "bonpreu_user": "usuari_o_dni_bonpreu",
  "bonpreu_password": "password_bonpreu",
  "digi_user": "telefon_o_email_digi",
  "digi_password": "password_digi",
  "veolia_user": "",
  "veolia_password": "",
  "catgas_user": "",
  "catgas_password": ""
}
```

*Nota: El token d'InfluxDB (`INFLUXDB_TOKEN`) s'extreu automàticament del secret existent `influxdb` (`admin_token`).*

---

## ☸️ Desplegament GitOps a K3s (ArgoCD)

Al repositori `argocd-bootstrap` es troba la declaració completa del servei:

- **`apps/42-subministres-etl.yaml`**: ArgoCD Application (syncWave: `3`, namespace: `subministres`).
- **`manifests/subministres-etl/`**:
  - `01-external-secret-samba.yaml`: Credencials SMB per muntar el recurs Samba.
  - `02-external-secret-app.yaml`: Credencials de l'aplicació des d'OpenBao.
  - `03-pv-samba-rw.yaml` & `04-pvc-samba-rw.yaml`: Volum persistent SMB (`//192.168.1.2/hdd2404/factures`).
  - `05-configmap.yaml`: Configuració no sensible de l'aplicació.
  - `06-cronjob.yaml`: CronJob diari a les 06:00 UTC (`0 6 * * *`).

### Execució Manual sota Demanda
```bash
kubectl create job --from=cronjob/subministres-etl subministres-etl-manual-01 -n subministres
kubectl logs -f job/subministres-etl-manual-01 -n subministres
```

---

## 🖥️ Interfície Web Operativa (Fase 2)

L'aplicació incorpora una interfície web ultralleugera (FastAPI + HTMX + Tailwind CSS) per a la gestió interactiva:

- **Sincronització sota demanda**: Botó "Sincronitzar Tot" o accions individuals per proveïdor (Veolia, Catgas, Bonpreu, Digi).
- **⚡ Descàrrega ràpida de l'Última Factura (KISS)**: Botó "Última" a cada proveïdor per baixar únicament la darrera factura si no és al NAS, evitant duplicats tant a la base de dades com a l'emmagatzematge.
- **📚 Explorador Històric & Descàrrega per Lots (Bulk Sync)**: Botó "Històric" que consulta totes les factures disponibles a la companyia (per bústia IMAP o portal web), les compara amb el NAS mostrant badges (`✅ Al NAS` vs `⏳ Pendent`) i ofereix el botó directe **"Descarregar Només Pendents"**.
- **🔍 Control d'Accessibilitat i Mida de Lletra (A- / A+)**: Selector de mida de font amb persistència a `localStorage` (100% fins a 150%) dissenyat per a una lectura òptima en monitors grans de 32", 4K o dispositius mòbils.
- **Pujada manual de factures (Dropzone / Drag & Drop)**: Permet arrossegar un PDF descarregat manualment; el sistema en detecta el proveïdor, n'extreu automàticament les dades, el guarda al NAS Samba i injecta les mètriques a InfluxDB.
- **Explorador de Factures al NAS**: Taula interactiva amb cercador, botó de descàrrega directa i **visor de PDF integrat en finestra modal** sense haver de muntar la unitat SMB al dispositiu.
- **Consola de Logs en directe**: Terminal integrada amb actualització automàtica cada 3 segons (polling HTMX) per seguir el progrés de l'scraping o detectar avisos de sessió.

### Endpoints de l'API Web

| Mètode | Endpoint | Descripció |
| :--- | :--- | :--- |
| `GET` | `/` | Pàgina principal del panell de control |
| `POST` | `/api/sync` | Dispara sincronització estàndard (`connector` opcional) |
| `POST` | `/api/sync/latest` | Cerca i descarrega exclusivament l'última factura |
| `GET` | `/api/history/{conn}` | Retorna el modal amb l'històric i comparativa amb el NAS |
| `GET` | `/api/logs` | Fragment HTML amb els darrers logs per a polling HTMX |
| `POST` | `/api/upload` | Pujada multipart d'un fitxer PDF i processament immediat |
| `GET` | `/api/pdf/view/{path}` | Serveix el fitxer PDF amb capçalera `inline` per al visor |
| `GET` | `/api/pdf/download/{path}` | Descarrega el fitxer PDF directament |

---

## 🚀 Execució Local i Desenvolupament

```bash
# 1. Crear entorn virtual i instal·lar requeriments
python -m venv .venv
source .venv/bin/activate  # o .venv\Scripts\activate a Windows
pip install -r requirements.txt

# 2. Instal·lar el navegador Chromium headless per a Playwright
playwright install chromium

# 3. Execució del Servidor Web (FastAPI + Uvicorn)
uvicorn src.web.app:app --host 127.0.0.1 --port 8000 --reload

# 4. Execució en mode CLI pur (per lots / dry-run)
python -m src.main --dry-run --days-back 30
python -m src.main --connector bonpreu --dry-run
```

---

## 🐳 Construcció i Pipeline DevSecOps

El projecte disposa d'un pipeline complet i automatitzat de **DevSecOps** mitjançant **GitHub Actions**:

1. **Docker Build & Push**: Construcció multi-etapa amb Buildx i publicació directa a GitHub Container Registry (`ghcr.io/vdk84/subministres-etl:latest`).
2. **Trivy Container Scanner**: Escaneig de vulnerabilitats (CVEs) a les capes de la imatge i publicació d'informes SARIF a la pestanya Security de GitHub.
3. **GitHub CodeQL (SAST)**: Anàlisi de seguretat de codi per a Python executat en cada commit i setmanalment.
4. **SonarCloud**: Anàlisi continu de qualitat, mantenibilitat i *code smells*.
5. **Dependabot**: Alertes i Pull Requests automàtics per a actualitzacions de dependències de Python i Actions.

```bash
# Construcció local manual (opcional)
docker build -t ghcr.io/vdk84/subministres-etl:latest .
```
