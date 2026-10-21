# pyright: reportMissingImports=false
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # General
    LOG_LEVEL: str = Field(default="INFO")
    DRY_RUN: bool = Field(default=False)
    DAYS_BACK: int = Field(default=60, description="Dies enrere per buscar correus o factures")

    # Storage NAS Samba (Mount point al contenidor K8s)
    NAS_MOUNT_PATH: str = Field(default="/data/factures", description="Punt de muntatge del share Samba al contenidor")

    # Email IMAP (Bústia dedicada per subministres)
    IMAP_ENABLED: bool = Field(default=True)
    IMAP_HOST: str = Field(default="")
    IMAP_PORT: int = Field(default=993)
    IMAP_USER: str = Field(default="")
    IMAP_PASSWORD: str = Field(default="")
    IMAP_FOLDER: str = Field(default="INBOX")
    IMAP_MARK_SEEN: bool = Field(default=False)

    # InfluxDB v2
    INFLUXDB_ENABLED: bool = Field(default=True)
    INFLUXDB_URL: str = Field(default="http://influxdb2.influxdb.svc.cluster.local:80")
    INFLUXDB_TOKEN: str = Field(default="")
    INFLUXDB_ORG: str = Field(default="home_assistant")
    INFLUXDB_BUCKET: str = Field(default="energy_data")

    # Proveïdors activats
    VEOLIA_ENABLED: bool = Field(default=True)
    VEOLIA_USER: str | None = Field(default=None)
    VEOLIA_PASSWORD: str | None = Field(default=None)

    CATGAS_ENABLED: bool = Field(default=True)
    CATGAS_USER: str | None = Field(default=None)
    CATGAS_PASSWORD: str | None = Field(default=None)

    BONPREU_ENABLED: bool = Field(default=True)
    BONPREU_USER: str | None = Field(default=None)
    BONPREU_PASSWORD: str | None = Field(default=None)

    DIGI_ENABLED: bool = Field(default=True)
    DIGI_USER: str | None = Field(default=None)
    DIGI_PASSWORD: str | None = Field(default=None)

    # Headless Scraper Settings & Protecció de Seguretat (Circuit Breaker)
    HEADLESS_BROWSER: bool = Field(default=True)
    SCRAPER_TIMEOUT_MS: int = Field(default=30000)
    MAX_SCRAPER_RETRIES: int = Field(default=3, description="Màxim d'intents abans de bloqueig preventiu")
    SCRAPER_COOLDOWN_MINUTES: int = Field(default=60, description="Minuts de cooldown per evitar bloquejos de compte")
    BONPREU_PORTAL_URL: str = Field(default="https://www.bonpreuesclat.cat/energia")
    DIGI_PORTAL_URL: str = Field(default="https://mi.digimobil.es/login")


settings = Settings()
