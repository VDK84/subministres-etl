# pyright: reportMissingImports=false
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import settings

logger = logging.getLogger("health_manager")

PROVIDERS = ("veolia", "catgas", "bonpreu", "digi")


def classify_incident(message: str) -> tuple[str, str, str]:
    """Classifica un missatge d'error en (codi, etiqueta_curta, suggeriment)."""
    msg_low = message.lower()
    if "2fa" in msg_low or "totp" in msg_low or "codi de verificac" in msg_low or "sms" in msg_low:
        return (
            "2FA_REQUIRED",
            "🔐 Verificació 2FA requerida",
            "El portal demana un codi 2FA que no s'ha pogut llegir automàticament per correu o requereix SMS.",
        )
    if "login" in msg_low or "credencial" in msg_low or "password" in msg_low or " secret" in msg_low:
        return (
            "AUTH_ERROR",
            "🔑 Error d'Autenticació / Credencials",
            "Revisa l'usuari i contrasenya al Secret o comprova si el portal demana un Captcha.",
        )
    if "parse" in msg_low or "mètriques" in msg_low or "import" in msg_low or "format" in msg_low:
        return (
            "PARSER_ERROR",
            "📄 Error de Format PDF (Parser)",
            "S'ha obtingut el PDF però no s'han pogut extreure totes les dades. Pot haver canviat el disseny de la factura.",
        )
    if "timeout" in msg_low or "temps d'espera" in msg_low:
        return (
            "TIMEOUT",
            "⏱️ Temps d'espera esgotat (Timeout)",
            "El portal web de la companyia ha trigat massa a respondre o un botó de descàrrega no ha reaccionat.",
        )
    if "influx" in msg_low:
        return (
            "INFLUX_ERROR",
            "🗄️ Error d'escriptura a InfluxDB",
            "La factura s'ha desat al NAS però ha fallat l'enviament de mètriques a InfluxDB.",
        )
    if "circuit breaker" in msg_low or "protecció de seguretat" in msg_low:
        return (
            "CIRCUIT_BREAKER",
            "⛔ Bloqueig Preventiu Actiu",
            "S'han aturat temporalment els intents per evitar bloquejar el compte del proveïdor.",
        )
    return (
        "GENERAL_ERROR",
        "⚠️ Incidència d'Extracció",
        "Revisa el detall tècnic de l'error per a més informació.",
    )


class HealthManager:
    """Gestiona l'estat de salut i l'històric d'incidències de cada connector persistit al NAS."""

    def __init__(self) -> None:
        self.status_dir = Path(settings.NAS_MOUNT_PATH) / ".status"
        self.status_file = self.status_dir / "health_state.json"
        self._state: dict[str, Any] = self._load()

    def _default_state(self) -> dict[str, Any]:
        return {
            prov: {
                "last_success_at": None,
                "last_success_source": None,
                "last_attempt_at": None,
                "last_status": "unknown",
                "incidents": [],
            }
            for prov in PROVIDERS
        }

    def _load(self) -> dict[str, Any]:
        state = self._default_state()
        try:
            if self.status_file.exists():
                raw = json.loads(self.status_file.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for prov in PROVIDERS:
                        if prov in raw and isinstance(raw[prov], dict):
                            state[prov].update(raw[prov])
        except (OSError, ValueError, TypeError) as err:
            logger.debug(f"No s'ha pogut llegir health_state.json: {err}")
        return state

    def _save(self) -> None:
        try:
            self.status_dir.mkdir(parents=True, exist_ok=True)
            self.status_file.write_text(json.dumps(self._state, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError as err:
            logger.debug(f"No s'ha pogut desar health_state.json: {err}")

    def reload(self) -> None:
        """Recarrega l'estat des del disc (útil quan un CronJob extern l'ha modificat)."""
        self._state = self._load()

    def record_success(self, proveidor: str, source: str = "auto", clear_incidents: bool = True) -> None:
        prov = proveidor.lower()
        if prov not in self._state:
            return
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        self._state[prov]["last_success_at"] = now_str
        self._state[prov]["last_success_source"] = source
        self._state[prov]["last_attempt_at"] = now_str
        self._state[prov]["last_status"] = "ok"
        if clear_incidents:
            self._state[prov]["incidents"] = []
        self._save()

    def record_incident(
        self,
        proveidor: str,
        message: str,
        severity: str = "error",
        source: str = "auto",
    ) -> None:
        prov = proveidor.lower()
        if prov not in self._state:
            return
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        code, title, hint = classify_incident(message)

        incidents: list[dict[str, Any]] = self._state[prov].get("incidents", [])
        # Evitar duplicar exactament el mateix error seguit
        if incidents and incidents[0].get("message") == message:
            incidents[0]["timestamp"] = now_str
            incidents[0]["count"] = incidents[0].get("count", 1) + 1
        else:
            ts_key = datetime.now().strftime("%Y%m%d%H%M%S")
            incidents.insert(
                0,
                {
                    "id": f"{prov}_{ts_key}_{len(incidents)}",
                    "proveidor": prov,
                    "timestamp": now_str,
                    "severity": severity,  # 'error' o 'warning'
                    "code": code,
                    "title": title,
                    "message": message,
                    "hint": hint,
                    "source": source,
                    "count": 1,
                },
            )

        # Mantenir com a màxim les últimes 15 incidències per proveïdor
        self._state[prov]["incidents"] = incidents[:15]
        self._state[prov]["last_attempt_at"] = now_str
        self._state[prov]["last_status"] = severity
        self._save()

    def clear_incidents(self, proveidor: str | None = None) -> None:
        if proveidor and proveidor.lower() in self._state:
            self._state[proveidor.lower()]["incidents"] = []
            if self._state[proveidor.lower()]["last_status"] in ("error", "warning"):
                self._state[proveidor.lower()]["last_status"] = "ok"
        else:
            for prov in PROVIDERS:
                self._state[prov]["incidents"] = []
                if self._state[prov]["last_status"] in ("error", "warning"):
                    self._state[prov]["last_status"] = "ok"
        self._save()

    def get_provider_health(self, proveidor: str, fallback_last_modified: str | None = None) -> dict[str, Any]:
        self.reload()
        prov = proveidor.lower()
        data = self._state.get(prov, {})
        incidents = data.get("incidents", [])
        last_ok = data.get("last_success_at") or fallback_last_modified or "Cap registre encara"
        return {
            "last_success_at": last_ok,
            "last_success_source": data.get("last_success_source") or "",
            "last_attempt_at": data.get("last_attempt_at"),
            "incident_count": len(incidents),
            "has_errors": any(i.get("severity") == "error" for i in incidents),
            "incidents": incidents,
        }

    def get_global_summary(self) -> dict[str, Any]:
        self.reload()
        all_incidents: list[dict[str, Any]] = []
        for prov in PROVIDERS:
            all_incidents.extend(self._state.get(prov, {}).get("incidents", []))
        all_incidents.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return {
            "total_incidents": len(all_incidents),
            "has_errors": any(i.get("severity") == "error" for i in all_incidents),
            "incidents": all_incidents,
        }


health_manager = HealthManager()
