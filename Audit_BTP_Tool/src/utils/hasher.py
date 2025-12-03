import hashlib
# On importe directement depuis le fichier python
from config.settings import CHUNK_SIZE

class FileManager:
    @staticmethod
    def get_file_hash(filepath: str) -> str:
        """Calcule le hash SHA-256 du fichier."""
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                while chunk := f.read(CHUNK_SIZE):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except (PermissionError, OSError):
            return "ACCESS_DENIED"

    @staticmethod
    def get_path_hash(filepath: str) -> str:
        """Hash MD5 rapide du chemin."""
        return hashlib.md5(filepath.encode('utf-8')).hexdigest()