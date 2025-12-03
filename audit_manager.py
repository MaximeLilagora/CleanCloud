import os
import sqlite3
import hashlib
import time
import json
from datetime import datetime
from typing import Tuple, List, Optional

# --- CONFIGURATION ---
DB_NAME = "inventory.sqlite"
CHUNK_SIZE = 65536  # Lecture par blocs de 64KB (Standard)
TARGET_EXTENSIONS = None 

class DatabaseManager:
    """
    Gère les interactions avec la base de données SQLite.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.connect()
        self.init_schema()

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            print(f"[ERREUR] Connexion BDD impossible : {e}")

    def init_schema(self):
        """Structure incluant les métadonnées techniques et les scores de risque."""
        schema = """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path_hash TEXT,
            content_hash TEXT,
            file_path TEXT UNIQUE,
            filename TEXT,
            extension TEXT,
            size_bytes INTEGER,
            creation_date DATETIME,
            modification_date DATETIME,
            category TEXT,
            processing_status TEXT DEFAULT 'PENDING',
            ai_data JSON,
            tech_data JSON,
            risk_score INTEGER DEFAULT 0
        );
        
        CREATE INDEX IF NOT EXISTS idx_content_hash ON files(content_hash);
        CREATE INDEX IF NOT EXISTS idx_path_hash ON files(path_hash);
        """
        try:
            cursor = self.conn.cursor()
            cursor.executescript(schema)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"[ERREUR] Création schéma : {e}")

    def insert_file(self, file_data: dict):
        """Insère le fichier avec son score de risque et statut."""
        query = """
        INSERT INTO files (
            path_hash, content_hash, file_path, filename, extension, 
            size_bytes, creation_date, modification_date, category,
            risk_score, processing_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET
            content_hash=excluded.content_hash,
            modification_date=excluded.modification_date,
            size_bytes=excluded.size_bytes,
            risk_score=excluded.risk_score,
            processing_status=excluded.processing_status;
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (
                file_data['path_hash'],
                file_data['content_hash'],
                file_data['file_path'],
                file_data['filename'],
                file_data['extension'],
                file_data['size_bytes'],
                file_data['creation_date'],
                file_data['modification_date'],
                "UNKNOWN",
                file_data['risk_score'],
                file_data['processing_status']
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"[ERREUR] Insertion {file_data['filename']} : {e}")

    def get_duplicates(self) -> List[sqlite3.Row]:
        query = """
        SELECT content_hash, COUNT(*) as count, GROUP_CONCAT(file_path, ' || ') as paths, SUM(size_bytes) as total_wasted
        FROM files
        WHERE processing_status != 'TRASH_EXT' 
        GROUP BY content_hash
        HAVING count > 1
        ORDER BY total_wasted DESC
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
        
    def get_trash_stats(self) -> dict:
        """Récupère les stats des fichiers déchets détectés."""
        query = "SELECT COUNT(*) as count, SUM(size_bytes) as size FROM files WHERE risk_score >= 90"
        cursor = self.conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        return {'count': row['count'], 'size': row['size'] if row['size'] else 0}

    def close(self):
        if self.conn:
            self.conn.close()

class DebrisFilter:
    """
    Identifie les fichiers temporaires ou inutiles spécifiques au BTP.
    """
    TRASH_EXTENSIONS = {
        '.bak', '.sv$', '.tmp', '.log', '.ds_store', 
        '.plot.log', '.err', '.dmp', '.old'
    }
    
    TRASH_FILENAMES = {
        'thumbs.db', 'desktop.ini', '.bridgecache'
    }

    @staticmethod
    def evaluate(filename: str, extension: str) -> Tuple[int, str]:
        if extension in DebrisFilter.TRASH_EXTENSIONS:
            return 100, "TRASH_EXT"
            
        if filename.lower() in DebrisFilter.TRASH_FILENAMES:
            return 100, "TRASH_SYS"
            
        if "conflit" in filename.lower() and "copie" in filename.lower():
            return 90, "CONFLICT_COPY"
            
        return 0, "PENDING"

class FileManager:
    @staticmethod
    def get_file_hash(filepath: str) -> str:
        """
        Calcule le hash SHA-256 du fichier.
        Standard robuste, fonctionne nativement sans dépendance externe.
        """
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
        return hashlib.md5(filepath.encode('utf-8')).hexdigest()

def scan_directory(root_path: str, db: DatabaseManager):
    print(f"[RUN] Démarrage du scan optimisé (SHA-256 + DebrisFilter) sur : {root_path}")
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
                # 1. Analyse rapide (Nom & Extension)
                risk_score, status = DebrisFilter.evaluate(name, ext)
                
                # 2. Calcul Hash
                # Optimisation : On ne hashe pas le contenu si c'est un fichier système verrouillé ou connu
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
                    
            except Exception as e:
                # Gestion silencieuse des erreurs courantes (fichiers verrouillés)
                pass

    duration = time.time() - start_time
    print(f"\n[TERMINE] {file_count} fichiers traités en {duration:.2f} secondes.")

def generate_report(db: DatabaseManager):
    duplicates = db.get_duplicates()
    trash_stats = db.get_trash_stats()
    
    print("\n" + "="*50)
    print("RAPPORT TECHNIQUE (PHASE 2)")
    print("="*50)
    
    # Section Déchets
    print(f"DÉCHETS IDENTIFIÉS (Filtre Extensions/Système)")
    print(f"Nombre : {trash_stats['count']} fichiers")
    print(f"Volume : {trash_stats['size']/1024/1024:.2f} MB")
    print("-" * 50)

    # Section Doublons
    print(f"DOUBLONS STRICTS (Hors déchets)")
    if not duplicates:
        print("Aucun doublon strict détecté.")
    else:
        total_wasted = 0
        print(f"{'Hash (SHA-256)':<15} | {'Copies':<8} | {'Perte':<10} | {'Exemple'}")
        print("-" * 50)
        
        for row in duplicates:
            wasted = row['total_wasted'] - (row['total_wasted'] / row['count'])
            total_wasted += wasted
            paths = row['paths'].split(' || ')
            print(f"{row['content_hash'][:12]:<15} | {row['count']:<8} | {wasted/1024/1024:.1f} MB  | {paths[0][:30]}...")

        print("-" * 50)
        print(f"GAIN POTENTIEL TOTAL : {(total_wasted + trash_stats['size'])/1024/1024:.2f} MB")

def main():
    print("=== OUTIL D'AUDIT BTP - PHASE 2 (SHA-256) ===")
    
    db = DatabaseManager(DB_NAME)
    target_dir = input("Dossier à auditer : ").strip()
    
    if os.path.exists(target_dir):
        scan_directory(target_dir, db)
        generate_report(db)
    else:
        print("[ERREUR] Dossier introuvable.")
    
    db.close()

if __name__ == "__main__":
    main()