import csv
import os
from datetime import datetime
from src.utils.db_client import DatabaseManager

class AuditReporter:
    """
    Gère la génération des rapports (CSV pour compatibilité Excel maximale).
    Simple, sans dépendance lourde (pandas), rapide.
    """
    
    def __init__(self, db: DatabaseManager, output_dir: str = "reports"):
        self.db = db
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def generate_full_audit(self):
        """Génère un CSV complet de tout l'inventaire avec métadonnées."""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.output_dir, f"Audit_Complet_{timestamp}.csv")
        
        print(f"[REPORT] Génération du rapport global : {filename}")
        
        rows = self.db.get_full_inventory()
        
        if not rows:
            print("[REPORT] Aucune donnée à exporter.")
            return

        try:
            # Récupération dynamique des noms de colonnes depuis sqlite3.Row
            headers = rows[0].keys()
            
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                # 'utf-8-sig' est important pour qu'Excel ouvre le fichier sans bugs d'accents
                writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=';')
                
                writer.writeheader()
                for row in rows:
                    writer.writerow(dict(row))
                    
            print(f"[SUCCÈS] Rapport généré ({len(rows)} lignes).")
            
        except IOError as e:
            print(f"[ERREUR] Écriture du rapport impossible : {e}")

    def generate_trash_report(self):
        """Génère un petit rapport ciblé sur les fichiers à supprimer."""
        # À implémenter si besoin spécifique
        pass