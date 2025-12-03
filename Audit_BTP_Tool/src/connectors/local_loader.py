import os
import time
from datetime import datetime
from src.utils.db_client import DatabaseManager
from src.cleaning.debris_filter import DebrisFilter
from src.utils.hasher import FileManager
# Import direct simple
from config.settings import TARGET_EXTENSIONS

def scan_directory(root_path: str, db: DatabaseManager):
    print(f"[RUN] Démarrage du scan sur : {root_path}")
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
                # 1. Analyse rapide
                risk_score, status = DebrisFilter.evaluate(name, ext)
                
                # 2. Calcul Hash
                if status == "TRASH_SYS":
                    content_hash = "SKIPPED_TRASH"
                else:
                    content_hash = FileManager.get_file_hash(file_path)
                
                path_hash = FileManager.get_path_hash(file_path)
                stats = os.stat(file_path)
                
                file_data = {
                    'path_hash': path_hash,
                    'content_hash': content_hash,
                    'file_path': file_path,
                    'filename': name,
                    'extension': ext,
                    'size_bytes': stats.st_size,
                    'creation_date': datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    'modification_date': datetime.fromtimestamp(stats.st_mtime).isoformat(),
                    'risk_score': risk_score,
                    'processing_status': status
                }

                db.insert_file(file_data)
                
                file_count += 1
                if file_count % 500 == 0:
                    print(f"\r[PROGRESS] {file_count} fichiers traités...", end="")
                    
            except Exception:
                pass

    duration = time.time() - start_time
    print(f"\n[TERMINE] {file_count} fichiers traités en {duration:.2f} secondes.")