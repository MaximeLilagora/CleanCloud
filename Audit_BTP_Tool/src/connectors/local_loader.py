import os
import time
from datetime import datetime
from src.utils.db_client import DatabaseManager
from src.cleaning.debris_filter import DebrisFilter
from src.utils.hasher import FileManager
# Nouvel import du moteur
from src.utils.metadata_engine import MetadataDispatcher
from config.settings import TARGET_EXTENSIONS

def scan_directory(root_path: str, db: DatabaseManager):
    print(f"[RUN] Audit Relationnel V2 (Mère/Filles) sur : {root_path}")
    start_time = time.time()
    file_count = 0
    
    for root, dirs, files in os.walk(root_path):
        for name in files:
            file_path = os.path.join(root, name)
            _, ext = os.path.splitext(name)
            ext = ext.lower()
            
            if TARGET_EXTENSIONS and ext not in TARGET_EXTENSIONS:
                continue

            try:
                # 1. Analyse Déchets
                risk_score, category = DebrisFilter.evaluate(name, ext)
                if category == "PENDING":
                    # Si ce n'est pas un déchet évident, on classifie 'WORK' par défaut
                    category = "WORK_FILE"
                
                # 2. Hachage Optimisé
                if category == "TRASH_SYS":
                    content_hash = "SKIPPED_TRASH"
                else:
                    content_hash = FileManager.get_file_hash(file_path)
                
                path_hash = FileManager.get_path_hash(file_path)
                stats = os.stat(file_path)
                
                # 3. MÉTADONNÉES AVANCÉES (Dispatcher)
                # C'est ici que la magie opère : on demande au moteur "Quelle table ? Quelles infos ?"
                target_table, meta_payload = MetadataDispatcher.dispatch(file_path, ext)

                # Préparation Données Table Mère
                main_data = {
                    'path_hash': path_hash,
                    'content_hash': content_hash,
                    'file_path': file_path,
                    'filename': name,
                    'visible_ext': ext,
                    'true_ext': ext, # A améliorer avec python-magic plus tard
                    'size_bytes': stats.st_size,
                    'dates_created': datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    'dates_modified': datetime.fromtimestamp(stats.st_mtime).isoformat(),
                    'category': category,
                    'risk_score': risk_score,
                    'status': 'SCANNED'
                }

                # 4. Insertion Relationnelle
                db.insert_full_entry(main_data, target_table, meta_payload)
                
                file_count += 1
                if file_count % 500 == 0:
                    print(f"\r[PROGRESS] {file_count} fichiers traités...", end="")
                    
            except Exception as e:
                # print(f"Err: {e}") # Debug only
                pass

    duration = time.time() - start_time
    print(f"\n[TERMINE] {file_count} fichiers inventoriés en {duration:.2f} secondes.")