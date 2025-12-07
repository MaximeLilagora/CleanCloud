import os
import sys
from src.utils.logger import setup_logging, get_logger

# On appelle la fonction logger
setup_logging() 
logger = get_logger(__name__)

# Ajout du dossier courant au path pour garantir les imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

try: 
    from config.settings import DB_NAME
except ImportError as e:
    print(f"[ERREUR CRITIQUE] Impossible d'importer la configuration.")
    print(f"Détail : {e}")
    print("Vérifiez que le fichier 'config/settings.py' existe bien.")
    sys.exit(1)

from src.utils.db_client import DatabaseManager
from src.connectors.local_loader import scan_directory
from src.utils.reporter import AuditReporter  # Import du nouveau reporter

def display_summary(db: DatabaseManager):
    """Affiche un résumé rapide dans la console avant l'export."""
    duplicates = db.get_duplicates()
    trash_stats = db.get_trash_stats()
    
    print("\n" + "="*50)
    print("RÉSUMÉ RAPIDE")
    print("="*50)
    print(f"DÉCHETS DÉTECTÉS : {trash_stats['count']} fichiers ({trash_stats['size']/1024/1024:.2f} MB)")
    
    if duplicates:
        print(f"DOUBLONS STRICTS : {len(duplicates)} groupes identifiés.")
    else:
        print("Aucun doublon strict détecté.")
    print("-" * 50)

def main():
    print("=== AUDIT BTP TOOL ===")
    
    # Initialisation
    db = DatabaseManager(DB_NAME)
    
    # 1. Scan
    target_dir = input("Dossier à auditer : ").strip()
    
    if os.path.exists(target_dir):
        # Lancement du scan
        scan_directory(target_dir, db)
        
        # 2. Résumé Console
        display_summary(db)
        
        # 3. Génération Rapport
        print("\n[REPORTING] Exportation des données...")
        reporter = AuditReporter(db)
        reporter.generate_full_audit()
        
    else:
        print("[ERREUR] Dossier introuvable.")
    
    db.close()

if __name__ == "__main__":
    main()