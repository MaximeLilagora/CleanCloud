import os
import sqlite3
import hashlib
import time
import json
from datetime import datetime
from typing import Tuple, List, Optional

# --- CONFIGURATION ---
DB_NAME = "inventory.sqlite"
CHUNK_SIZE = 65536  # Lecture par blocs de 64KB pour ne pas saturer la RAM
TARGET_EXTENSIONS = None  # Mettre une liste ['.dwg', '.ifc'] pour filtrer, None pour tout prendre

class DatabaseManager:
    """
    Gère les interactions avec la base de données SQLite.
    Responsable de la création des tables et de l'insertion des données.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.connect()
        self.init_schema()

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            # Permet d'accéder aux colonnes par nom
            self.conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            print(f"[ERREUR] Connexion BDD impossible : {e}")

    def init_schema(self):
        """Crée la structure de la table définie dans les spécifications."""
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
            print("[INFO] Schéma base de données initialisé/vérifié.")
        except sqlite3.Error as e:
            print(f"[ERREUR] Création schéma : {e}")

    def insert_file(self, file_data: dict):
        """Insère ou met à jour un fichier dans l'inventaire."""
        query = """
        INSERT INTO files (
            path_hash, content_hash, file_path, filename, extension, 
            size_bytes, creation_date, modification_date, category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET
            content_hash=excluded.content_hash,
            modification_date=excluded.modification_date,
            size_bytes=excluded.size_bytes;
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
                "UNKNOWN" # Catégorie par défaut avant analyse IA
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"[ERREUR] Insertion fichier {file_data['filename']} : {e}")

    def get_duplicates(self) -> List[sqlite3.Row]:
        """Récupère les groupes de fichiers ayant le même contenu (hash)."""
        query = """
        SELECT content_hash, COUNT(*) as count, GROUP_CONCAT(file_path, ' || ') as paths, SUM(size_bytes) as total_wasted
        FROM files
        GROUP BY content_hash
        HAVING count > 1
        ORDER BY total_wasted DESC
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def close(self):
        if self.conn:
            self.conn.close()

class FileManager:
    """
    Responsable des opérations sur les fichiers : Hachage et Normalisation.
    """
    @staticmethod
    def get_file_hash(filepath: str) -> str:
        """
        Calcule le hash SHA-256 du fichier.
        Lit le fichier par morceaux (chunks) pour éviter de charger 
        des fichiers de 10Go en RAM.
        Note: Pour la prod, utiliser xxHash pour plus de rapidité.
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
        """Hash simple du chemin pour indexation rapide."""
        return hashlib.md5(filepath.encode('utf-8')).hexdigest()

    @staticmethod
    def get_metadata(entry: os.DirEntry) -> dict:
        """Extrait les métadonnées système de base."""
        stats = entry.stat()
        return {
            'filename': entry.name,
            'extension': os.path.splitext(entry.name)[1].lower(),
            'size_bytes': stats.st_size,
            'creation_date': datetime.fromtimestamp(stats.st_ctime).isoformat(),
            'modification_date': datetime.fromtimestamp(stats.st_mtime).isoformat()
        }

def scan_directory(root_path: str, db: DatabaseManager):
    """
    Le 'Crawler' principal. Utilise os.scandir pour la performance.
    """
    print(f"[RUN] Démarrage du scan sur : {root_path}")
    start_time = time.time()
    file_count = 0
    
    # os.walk est simple, mais os.scandir est plus performant pour les gros volumes
    # Ici j'utilise os.walk pour simplifier la récursivité dans ce prototype
    for root, dirs, files in os.walk(root_path):
        for name in files:
            file_path = os.path.join(root, name)
            
            # Filtre extension (Optionnel)
            _, ext = os.path.splitext(name)
            if TARGET_EXTENSIONS and ext.lower() not in TARGET_EXTENSIONS:
                continue

            # Traitement
            try:
                # 1. Calcul des Hashs
                content_hash = FileManager.get_file_hash(file_path)
                path_hash = FileManager.get_path_hash(file_path)
                
                # 2. Métadonnées (on réutilise os.stat via os.path pour ce prototype)
                stats = os.stat(file_path)
                
                file_data = {
                    'path_hash': path_hash,
                    'content_hash': content_hash,
                    'file_path': file_path,
                    'filename': name,
                    'extension': ext.lower(),
                    'size_bytes': stats.st_size,
                    'creation_date': datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    'modification_date': datetime.fromtimestamp(stats.st_mtime).isoformat()
                }

                # 3. Enregistrement DB
                db.insert_file(file_data)
                
                file_count += 1
                if file_count % 100 == 0:
                    print(f"\r[PROGRESS] {file_count} fichiers traités...", end="")
                    
            except Exception as e:
                print(f"\n[SKIP] Erreur sur {name}: {e}")

    duration = time.time() - start_time
    print(f"\n[TERMINE] {file_count} fichiers traités en {duration:.2f} secondes.")

def generate_report(db: DatabaseManager):
    """Génère un rapport texte simple sur les doublons."""
    duplicates = db.get_duplicates()
    
    print("\n" + "="*40)
    print("RAPPORT PRÉLIMINAIRE DE DÉDOUBLONNAGE")
    print("="*40)
    
    if not duplicates:
        print("Aucun doublon strict détecté.")
        return

    total_wasted = 0
    print(f"{'Hash (Début)':<15} | {'Nb Copies':<10} | {'Espace Perdu':<15} | {'Fichiers'}")
    print("-" * 80)
    
    for row in duplicates:
        # On soustrait la taille d'une copie car il faut en garder une !
        wasted = row['total_wasted'] - (row['total_wasted'] / row['count'])
        total_wasted += wasted
        
        paths = row['paths'].split(' || ')
        display_paths = paths[0] + f" (+ {len(paths)-1} autres)"
        
        print(f"{row['content_hash'][:12]:<15} | {row['count']:<10} | {wasted/1024/1024:.2f} MB      | {display_paths}")

    print("-" * 80)
    print(f"TOTAL ESPACE RÉCUPÉRABLE (ESTIMÉ) : {total_wasted/1024/1024:.2f} MB")

def main():
    print("=== OUTIL D'AUDIT BTP - PHASE 1 : SQUELETTE ===")
    
    # 1. Setup DB
    db = DatabaseManager(DB_NAME)
    
    # 2. Input utilisateur
    target_dir = input("Entrez le chemin complet du dossier à auditer : ").strip()
    
    if os.path.exists(target_dir):
        # 3. Scan
        scan_directory(target_dir, db)
        
        # 4. Rapport
        generate_report(db)
    else:
        print("[ERREUR] Le dossier n'existe pas.")
    
    db.close()

if __name__ == "__main__":
    main()