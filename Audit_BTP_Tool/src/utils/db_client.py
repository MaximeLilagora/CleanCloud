import sqlite3
import json
from typing import List

class DatabaseManager:
    """
    Gère les interactions avec la base de données SQLite.
    Responsable du schéma et des transactions.
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
        """Insère ou met à jour le fichier avec son score de risque."""
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
        """Récupère les doublons stricts (hors fichiers déchets)."""
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