import sqlite3
import json
from typing import Optional, Dict

class DatabaseManager:
    """
    Gère l'architecture 'Class Table Inheritance'.
    1 Table Mère (files_inventory) + 5 Tables Filles (meta_*)
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
            # Activation des Foreign Keys (vital pour SQLite)
            self.conn.execute("PRAGMA foreign_keys = ON;")
        except sqlite3.Error as e:
            print(f"[ERREUR CRITIQUE] Connexion BDD : {e}")

    def init_schema(self):
        """Création des tables Mère et Filles."""
        cursor = self.conn.cursor()
        
        # 1. TABLE MÈRE : Inventaire Global
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS files_inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path_hash TEXT,
            content_hash TEXT,
            file_path TEXT UNIQUE,
            filename TEXT,
            visible_ext TEXT,
            true_ext TEXT,
            size_bytes INTEGER,
            dates_created DATETIME,
            dates_modified DATETIME,
            category TEXT,          -- ex: 'PLAN', 'TRASH'
            status TEXT DEFAULT 'PENDING',
            risk_score INTEGER DEFAULT 0
        );
        """)
        
        # Index pour la rapidité
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_content ON files_inventory(content_hash);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON files_inventory(path_hash);")

        # 2. TABLES SATELLITES (Spécialisation)
        
        # A. CAD / CAO (.dwg, .rvt)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta_cad (
            file_id INTEGER PRIMARY KEY,
            software_version TEXT,  -- ex: 'AutoCAD 2018'
            has_xrefs BOOLEAN,      -- Références externes détectées ?
            scale TEXT,             -- Échelle potentielle
            FOREIGN KEY(file_id) REFERENCES files_inventory(id) ON DELETE CASCADE
        );
        """)

        # B. DOCUMENTS (.pdf, .docx)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta_document (
            file_id INTEGER PRIMARY KEY,
            page_count INTEGER,
            author TEXT,
            is_encrypted BOOLEAN,
            producer_tool TEXT,     -- ex: 'Revit PDF Writer'
            FOREIGN KEY(file_id) REFERENCES files_inventory(id) ON DELETE CASCADE
        );
        """)

        # C. VISUAL / IMAGES (.jpg, .png)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta_visual (
            file_id INTEGER PRIMARY KEY,
            width INTEGER,
            height INTEGER,
            color_space TEXT,       -- RGB / CMYK
            geo_lat REAL,           -- GPS Latitude
            geo_long REAL,          -- GPS Longitude
            FOREIGN KEY(file_id) REFERENCES files_inventory(id) ON DELETE CASCADE
        );
        """)

        # D. SPREADSHEET (.xlsx, .csv)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta_spreadsheet (
            file_id INTEGER PRIMARY KEY,
            sheet_count INTEGER,
            row_limit INTEGER,      -- Pour estimer la densité
            has_macros BOOLEAN,     -- Sécurité (.xlsm)
            FOREIGN KEY(file_id) REFERENCES files_inventory(id) ON DELETE CASCADE
        );
        """)

        # E. ARCHIVE (.zip, .rar)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta_archive (
            file_id INTEGER PRIMARY KEY,
            file_count INTEGER,     -- Combien de fichiers dedans ?
            compression_ratio REAL,
            is_encrypted BOOLEAN,
            FOREIGN KEY(file_id) REFERENCES files_inventory(id) ON DELETE CASCADE
        );
        """)

        self.conn.commit()

    def insert_full_entry(self, main_data: dict, meta_table: str = None, meta_data: dict = None):
        """
        Transaction atomique : Insère dans Inventaire -> Récupère ID -> Insère dans Meta.
        """
        cursor = self.conn.cursor()
        try:
            # 1. Insertion/Update Table Mère
            # On utilise une syntaxe UPSERT compatible SQLite moderne
            query_main = """
            INSERT INTO files_inventory (
                path_hash, content_hash, file_path, filename, 
                visible_ext, true_ext, size_bytes, 
                dates_created, dates_modified, category, risk_score, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_path) DO UPDATE SET
                content_hash=excluded.content_hash,
                dates_modified=excluded.dates_modified,
                status=excluded.status,
                risk_score=excluded.risk_score;
            """
            
            cursor.execute(query_main, (
                main_data['path_hash'], main_data['content_hash'], main_data['file_path'], 
                main_data['filename'], main_data['visible_ext'], main_data['true_ext'],
                main_data['size_bytes'], main_data['dates_created'], main_data['dates_modified'],
                main_data['category'], main_data['risk_score'], main_data['status']
            ))

            # Récupération de l'ID (soit le nouvel ID, soit l'existant)
            # Rowid fonctionne, mais pour être sûr en cas d'update sans changement d'ID :
            cursor.execute("SELECT id FROM files_inventory WHERE file_path = ?", (main_data['file_path'],))
            row = cursor.fetchone()
            if not row:
                raise Exception("ID non retrouvé après insertion.")
            
            file_id = row['id']

            # 2. Insertion Table Fille (si métadonnées présentes)
            if meta_table and meta_data:
                # Construction dynamique de la requête SQL pour la table fille
                columns = list(meta_data.keys())
                placeholders = ', '.join(['?'] * len(columns))
                col_names = ', '.join(columns)
                
                # On ajoute file_id manuellement
                sql_meta = f"INSERT OR REPLACE INTO {meta_table} (file_id, {col_names}) VALUES (?, {placeholders})"
                
                values = [file_id] + list(meta_data.values())
                cursor.execute(sql_meta, values)

            self.conn.commit()

        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"[SQL ERROR] Sur {main_data['filename']}: {e}")

    def get_duplicates(self):
        """
        Version adaptée à la nouvelle structure.
        CORRECTIF : Ajout de GROUP_CONCAT pour récupérer la liste 'paths'
        """
        query = """
        SELECT 
            content_hash, 
            COUNT(*) as count, 
            GROUP_CONCAT(file_path, ' || ') as paths,
            SUM(size_bytes) as total_wasted
        FROM files_inventory
        WHERE category != 'TRASH_EXT' 
        GROUP BY content_hash
        HAVING count > 1
        ORDER BY total_wasted DESC
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
        
    def get_trash_stats(self) -> dict:
        query = "SELECT COUNT(*) as count, SUM(size_bytes) as size FROM files_inventory WHERE risk_score >= 90"
        cursor = self.conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        return {'count': row['count'], 'size': row['size'] if row['size'] else 0}

    def close(self):
        if self.conn:
            self.conn.close()