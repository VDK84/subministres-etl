import hashlib
import logging
from pathlib import Path

from src.config import settings

logger = logging.getLogger("storage")


class StorageManager:
    """Gestiona l'emmagatzematge de factures PDF al volum NAS Samba."""

    def __init__(self, base_path: str | None = None) -> None:
        self.base_path = Path(base_path or settings.NAS_MOUNT_PATH)

    def get_destination_dir(self, tipus: str, proveidor: str, any_str: str) -> Path:
        """Retorna el directori organitzat: base_path / tipus / proveidor / any."""
        dest_dir = self.base_path / tipus.lower() / proveidor.lower() / str(any_str)
        dest_dir.mkdir(parents=True, exist_ok=True)
        return dest_dir

    def calculate_sha256(self, file_content: bytes) -> str:
        """Calcula el hash SHA256 d'un contingut per detectar duplicats exactes."""
        return hashlib.sha256(file_content).hexdigest()

    def exists(self, tipus: str, proveidor: str, any_str: str, filename: str) -> bool:
        """Comprova si el fitxer ja existeix al NAS."""
        target_file = self.get_destination_dir(tipus, proveidor, any_str) / filename
        return target_file.exists()

    def save_pdf(
        self,
        file_bytes: bytes,
        tipus: str,
        proveidor: str,
        any_str: str,
        filename: str,
    ) -> Path:
        """Desa un PDF al NAS i retorna el Path resultant."""
        dest_dir = self.get_destination_dir(tipus, proveidor, any_str)
        target_path = dest_dir / filename

        if settings.DRY_RUN:
            logger.info(f"[DRY-RUN] Es guardaria el fitxer a: {target_path} ({len(file_bytes)} bytes)")
            return target_path

        target_path.write_bytes(file_bytes)
        logger.info(f"Factura desada correctament a: {target_path}")
        return target_path

    def delete_pdf(self, rel_path: str) -> bool:
        """Elimina un fitxer del NAS Samba de forma segura."""
        target = (self.base_path / rel_path).resolve()
        if not str(target).startswith(str(self.base_path.resolve())) or not target.exists():
            return False
        if settings.DRY_RUN:
            logger.info(f"[DRY-RUN] S'eliminaria el fitxer a: {target}")
            return True
        target.unlink(missing_ok=True)
        logger.info(f"Fitxer eliminat correctament del NAS: {target}")
        return True


storage = StorageManager()
