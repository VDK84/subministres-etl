# subministres-etl 💧⚡🔥📱

ETL automatitzat per a la recol·lecció, processament, emmagatzematge al NAS i ingesta a InfluxDB v2 de factures de subministraments de la llar.

---

## 🏛️ Arquitectura i Estratègia d'Extracció

L'aplicació implementa un enfocament híbrid segons el comportament de cada proveïdor de subministraments:

1. **Extracció directa per correu (IMAP amb PDF adjunt)**:
   - **Veolia (Aigua)**: La factura arriba en format PDF adjunt al correu.
   - **Catgas Energia (Gas Natural)**: La factura arriba en format PDF adjunt al correu.
   - *Procediment*: El connector llegeix la bústia dedicada, descarrega el fitxer PDF adjunt, parseja els consums ($m^3$, $kWh$), data, període i imports, i l'emmagatzema directament.

2. **Trigger per correu + Scraping web del portal de clients**:
   - **Bonpreu Esclat Energia (Electricitat)**: Bonpreu notifica l'import i l'emissió de la factura per correu sense adjuntar el PDF.
   - **Digi Mobil (Telefonia / Fibra)**: Digi només notifica l'import mensual per correu electrònic.
   - *Procediment*: La recepció del correu d'avís funciona com a disparador. El connector s'autentica de forma autònoma al portal de clients (`https://clients.energia.bonpreu.cat/` i `https://mi.digimobil.es/`) mitjançant Playwright (Chromium headless), descarrega el PDF oficial de la factura, extreu les mètriques i el desa.

---

## 📦 Destinacions de les Dades

### 1. NAS Samba (Fitxers PDF)
Els fitxers PDF es guarden organitzats en una estructura de carpetes neta al recurs compartit SMB:

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

### 2. InfluxDB v2 (Sèries Temporals per a Grafana)
Les dades s'envien al bucket configurat (ex: `energy_data`) amb el measurement `factures_subministres`:

- **Tags**: `proveidor`, `tipus` (`aigua`, `gas`, `electricitat`, `telefonia`), `numero_factura`, `any`, `mes`, `titular`, `referencia`
- **Fields**:
  - `import_total` (float)
  - `consum_m3` (aigua / gas)
  - `consum_kwh`, `consum_kwh_punta`, `consum_kwh_pla`, `consum_kwh_vall` (electricitat / gas)
  - `potencia_contractada_kw`
  - `total_gb_consumits`, `num_linies_mobils` (telefonia)
  - `dies_facturats`
  - `import_base`, `import_iva`

---

## ⚙️ Variables d'Entorn i Configuració

| Variable | Descripció | Per defecte |
| :--- | :--- | :--- |
| `LOG_LEVEL` | Nivell de log (`INFO`, `DEBUG`) | `INFO` |
| `DRY_RUN` | Mode simulació (no desa fitxers ni escriu a InfluxDB) | `false` |
| `DAYS_BACK` | Dies enrere a inspeccionar | `60` |
| `NAS_MOUNT_PATH` | Ruta de muntatge del share Samba al contenidor | `/data/factures` |
| `IMAP_HOST` | Host del servidor IMAP de la bústia dedicada | `""` |
| `IMAP_PORT` | Port IMAP SSL | `993` |
| `IMAP_USER` | Usuari/correu de la bústia de factures | `""` |
| `IMAP_PASSWORD` | Contrasenya o App Password del correu | `""` |
| `INFLUXDB_URL` | Endpoint d'InfluxDB v2 | `http://influxdb2.influxdb.svc.cluster.local:80` |
| `INFLUXDB_TOKEN` | Token d'escriptura d'InfluxDB | `""` |
| `INFLUXDB_ORG` | Organització a InfluxDB | `home_assistant` |
| `INFLUXDB_BUCKET` | Bucket a InfluxDB | `energy_data` |
| `BONPREU_USER` | Usuari/DNI/Email del portal de Bonpreu | `""` |
| `BONPREU_PASSWORD` | Contrasenya del portal de Bonpreu | `""` |
| `DIGI_USER` | Usuari/Telèfon/Email del portal Mi Digi | `""` |
| `DIGI_PASSWORD` | Contrasenya del portal Mi Digi | `""` |
| `VEOLIA_USER` | Usuari portal Veolia (opcional) | `""` |
| `VEOLIA_PASSWORD` | Contrasenya portal Veolia (opcional) | `""` |
| `CATGAS_USER` | Usuari portal Catgas (opcional) | `""` |
| `CATGAS_PASSWORD` | Contrasenya portal Catgas (opcional) | `""` |

---

## 🚀 Execució Local

```bash
# Instal·lar dependències
pip install -r requirements.txt
playwright install chromium

# Execució en mode prova (dry-run)
python -m src.main --dry-run --days-back 30

# Execució d'un connector específic
python -m src.main --connector bonpreu
```

---

## ☸️ Desplegament a Kubernetes (K3s + ArgoCD)

L'aplicació s'empaqueta com a CronJob de Kubernetes a través del Helm Chart disponible a `./helm/subministres-etl` i es sincronitza mitjançant ArgoCD des del repositori `argocd-bootstrap`.
