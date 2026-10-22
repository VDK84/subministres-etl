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
   - *Flux*: La recepció del correu d'avís funciona com a disparador (trigger). El connector utilitza **Playwright** (Chromium headless) per connectar als portals oficials (`https://www.bonpreuesclat.cat/energia` o `https://midigi.digimobil.es/`), descarregar el PDF oficial, parsejar-ne el detall (desglossament P1/P2/P3 o gigues de mòbil) i emmagatzemar-lo al NAS.

4. **Persistència de Sessió al NAS i Gestió de 2FA (Zero Bloquejos)**:
   - **Sessió persistent (`storage_state`)**: Les cookies i tokens de sessió dels portals es guarden al NAS a `/data/factures/.sessions/`. D'aquesta manera, les execucions periòdiques entren directament a l'àrea de factures sense tornar a fer login ni demanar 2FA durant setmanes o mesos.
   - **Detecció de 2FA**: Quan la sessió caduca i el portal sol·licita verificació en dos passos (SMS/Email), el sistema **no insisteix ni entra en bucle**. Desa una captura de seguretat a `.sessions/digi_2fa_prompt.png` i deixa el connector en estat `Pendent de 2FA`.
   - **Protecció amb Circuit Breaker**: Si es produeixen 3 intents fallits consecutius (per problemes de xarxa o canvi de portal), el connector activa automàticament una pausa preventiva de 60 minuts per evitar qualsevol bloqueig de compte a la companyia.
   - **Recuperació històrica per lots (Bulk)**: L'usuari pot accedir puntualment (ex: cada 3 o 6 mesos), validar la sessió amb el codi 2FA si ha caducat, i amb el botó "Històric" descarregar de cop totes les factures pendents acumulades.

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
  - `05-rbac.yaml`: `ServiceAccount`, `Role` i `RoleBinding` per a l'auto-anotació nativa del Pod en arrencar (`app.kubernetes.io/version`).
  - `06-deployment.yaml`: Deployment del servidor web FastAPI + Uvicorn.
  - `07-service.yaml` & `08-ingressroute.yaml`: Exposició interna i Ingress Traefik HTTPS a `https://subministres.rusc.local`.
  - `09-cronjob.yaml`: CronJob diari a les 06:00 UTC (`0 6 * * *`).

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
- **📚 Explorador Històric & Descàrrega per Lots (Bulk Sync)**: Botó "Històric" que consulta totes les factures disponibles a la companyia (per bústia IMAP o portal web), amb temporitzador visual en segons, mini-consola de logs en directe i botó directe **"Descarregar Només Pendents"**.
- **📦 Pujada per Lots Multi-format (PDF, EML, ZIP)**: La zona de pujada manual (Dropzone) admet fitxers individuals en `.pdf` o `.eml`, i arxius **`.zip` complets** (amb desenes de PDFs o EMLs en subcarpetes). El sistema desempaqueta a memòria, detecta automàticament el proveïdor de cada factura i les classifica a les carpetes corresponents del NAS.
- **🛡️ Filtre Estricte per Magic Bytes `%PDF`**: Protecció que descarta automàticament qualsevol imatge o logo inline (`.png`, `.jpg`, `.svg`) que les companyies adjunten als seus correus, garantint que només es processen documents de facturació reals.
- **🔍 Control d'Accessibilitat i Mida de Lletra (A- / A+)**: Selector de mida de font amb persistència a `localStorage` (100% fins a 150%) dissenyat per a una lectura òptima en monitors grans de 32", 4K o dispositius mòbils.
- **Explorador de Factures al NAS**: Taula interactiva amb cercador per text, **desplegable de filtratge per proveïdor**, **comptador dinàmic en viu** (*«Mostrant X de Y factures»*), descàrrega directa, visor de PDF integrat en finestra modal i **botó vermell 🗑️ Eliminar** (que suprimeix la factura del NAS Samba i n'elimina les mètriques d'InfluxDB de forma simultània).
- **Consola de Logs en directe**: Terminal integrada amb actualització automàtica cada 3 segons (polling HTMX silenciós filtrat d'Uvicorn) per seguir el progrés de l'scraping o detectar avisos de sessió.

### Endpoints de l'API Web

| Mètode | Endpoint | Descripció |
| :--- | :--- | :--- |
| `GET` | `/` | Pàgina principal del panell de control |
| `POST` | `/api/sync` | Dispara sincronització estàndard (`connector` opcional) |
| `POST` | `/api/sync/latest` | Cerca i descarrega exclusivament l'última factura |
| `GET` | `/api/history/{conn}` | Retorna el modal amb l'històric i comparativa amb el NAS |
| `GET` | `/api/logs` | Fragment HTML amb els darrers logs per a polling HTMX |
| `POST` | `/api/upload` | Pujada de fitxers (.pdf, .eml, .zip) amb detecció automàtica |
| `DELETE` | `/api/invoice/{path}` | Elimina el PDF del NAS Samba i suprimeix les mètriques d'InfluxDB |
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

1. **Semantic Versioning (SemVer) & GitHub Releases**: Increment automàtic de versió seguint *Conventional Commits* (`feat:` ➔ minor, `fix:` ➔ patch, `BREAKING CHANGE:` ➔ major), creació de Git Tags (`vX.Y.Z`) i publicació de Releases a GitHub amb Changelog generat.
2. **Docker Build & Push**: Construcció multi-etapa amb Buildx i publicació directa a GitHub Container Registry amb múltiples etiquetes (`latest`, `vX.Y.Z`, `X.Y.Z`, `sha-<curt>`).
3. **Trivy Container Scanner**: Escaneig de vulnerabilitats (CVEs) a les capes de la imatge i publicació d'informes SARIF a la pestanya Security de GitHub.
4. **GitHub CodeQL (SAST)**: Anàlisi de seguretat de codi per a Python executat en cada commit i setmanalment.
5. **SonarCloud**: Anàlisi continu de qualitat, mantenibilitat i *code smells*.
6. **Dependabot**: Alertes i Pull Requests automàtics per a actualitzacions de dependències agrupades per evitar conflictes.

```bash
# Construcció local manual (opcional)
docker build -t ghcr.io/vdk84/subministres-etl:latest .
```
