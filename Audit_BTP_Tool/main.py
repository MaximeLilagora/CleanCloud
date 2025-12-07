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

def generate_report(db: DatabaseManager):
    duplicates = db.get_duplicates()
    trash_stats = db.get_trash_stats()
    
    print("\n" + "="*50)
    print("RAPPORT D'AUDIT (SIMPLIFIÉ)")
    print("="*50)
    
    print(f"DÉCHETS : {trash_stats['count']} fichiers ({trash_stats['size']/1024/1024:.2f} MB)")
    print("-" * 50)

    if not duplicates:
        print("Aucun doublon strict détecté.")
    else:
        total_wasted = 0
        print(f"{'Hash':<15} | {'Copies':<8} | {'Perte':<10} | {'Exemple'}")
        print("-" * 50)
        
        for row in duplicates:
            wasted = row['total_wasted'] - (row['total_wasted'] / row['count'])
            total_wasted += wasted
            paths = row['paths'].split(' || ')
            display_hash = row['content_hash'][:12] if row['content_hash'] else "N/A"
            print(f"{display_hash:<15} | {row['count']:<8} | {wasted/1024/1024:.1f} MB  | {paths[0][:30]}...")

        print("-" * 50)
        print(f"GAIN POTENTIEL : {(total_wasted + trash_stats['size'])/1024/1024:.2f} MB")

def main():
    print("=== AUDIT BTP TOOL ===")
    
    # Initialisation
    db = DatabaseManager(DB_NAME)
    
    # Scan
    target_dir = input("Dossier à auditer : ").strip()
    
    if os.path.exists(target_dir):
        scan_directory(target_dir, db)
        generate_report(db)
    else:
        print("[ERREUR] Dossier introuvable.")
    
    db.close()

if __name__ == "__main__":
    main()