import os
import time
from datetime import datetime
from src.utils.db_client import DatabaseManager
from src.cleaning.debris_filter import DebrisFilter
from src.utils.hasher import FileManager
from src.utils.metadata_engine import MetadataDispatcher
from config.settings import TARGET_EXTENSIONS
from src.utils.logger import get_logger

# Récupération du logger nommé "Loader"
logger = get_logger("Loader")


def scan_directory(root_path: str, db: DatabaseManager):
    """
    Parcourt récursivement le dossier cible en utilisant os.scandir pour optimiser les I/O.
    """
    print(f"[RUN] Audit Relationnel V2 (Optimisé) sur : {root_path}")
    start_time = time.time()
    file_count = 0

    # Pile pour le parcours itératif (évite la récursion profonde limitante)
    stack = [root_path]

    while stack:
        current_dir = stack.pop()
        
        try:
            # os.scandir est un itérateur (plus léger en RAM que os.listdir)
            # Context manager (with) assure la fermeture propre du descripteur
            with os.scandir(current_dir) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                        continue
                    
                    if not entry.is_file(follow_symlinks=False):
                        continue

                    # -- TRAITEMENT FICHIER --
                    name = entry.name
                    _, ext = os.path.splitext(name)
                    ext = ext.lower()

                    if TARGET_EXTENSIONS and ext not in TARGET_EXTENSIONS:
                        continue

                    try:
                        # 1. Analyse Déchets
                        risk_score, category = DebrisFilter.evaluate(name, ext)
                        if category == "PENDING":
                            category = "WORK_FILE"

                        # 2. Hachage
                        # Optimisation : On évite de hacher les fichiers système identifiés comme trash
                        file_path = entry.path
                        if category == "TRASH_SYS":
                            content_hash = "SKIPPED_TRASH"
                        else:
                            content_hash = FileManager.get_file_hash(file_path)

                        path_hash = FileManager.get_path_hash(file_path)

                        # 3. Métadonnées (Via entry.stat() qui est en cache depuis scandir)
                        stats = entry.stat()
                        
                        # Dispatch vers l'analyseur spécifique (PDF, CAD, etc.)
                        target_table, meta_payload = MetadataDispatcher.dispatch(file_path, ext)

                        main_data = {
                            'path_hash': path_hash,
                            'content_hash': content_hash,
                            'file_path': file_path,
                            'filename': name,
                            'visible_ext': ext,
                            'true_ext': ext,
                            'size_bytes': stats.st_size,
                            'dates_created': datetime.fromtimestamp(stats.st_ctime).isoformat(),
                            'dates_modified': datetime.fromtimestamp(stats.st_mtime).isoformat(),
                            'category': category,
                            'risk_score': risk_score,
                            'status': 'SCANNED'
                        }

                        # 4. Insertion BDD
                        db.insert_full_entry(main_data, target_table, meta_payload)

                        file_count += 1
                        if file_count % 500 == 0:
                            print(f"\r[PROGRESS] {file_count} fichiers traités...", end="")

                    except Exception as e:
                        # Log discret en cas d'erreur sur un fichier spécifique
                        # print(f"[WARN] Erreur sur {name}: {e}")
                        pass

        except PermissionError:
            print(f"\n[ACCÈS REFUSÉ] Impossible de lire le dossier : {current_dir}")
        except OSError as e:
            print(f"\n[ERREUR I/O] Sur {current_dir}: {e}")

    duration = time.time() - start_time
    print(f"\n[TERMINE] {file_count} fichiers inventoriés en {duration:.2f} secondes.")