from typing import Tuple

class DebrisFilter:
    """
    Identifie les fichiers temporaires ou inutiles spécifiques au BTP.
    """
    # Extensions à supprimer sans hésitation
    TRASH_EXTENSIONS = {
        '.bak', '.sv$', '.tmp', '.log', '.ds_store', 
        '.plot.log', '.err', '.dmp', '.old'
    }
    
    # Fichiers système ou cache inutiles
    TRASH_FILENAMES = {
        'thumbs.db', 'desktop.ini', '.bridgecache'
    }

    @staticmethod
    def evaluate(filename: str, extension: str) -> Tuple[int, str]:
        """
        Retourne (Score de Risque, Statut).
        Score 100 = Déchet certain.
        """
        if extension in DebrisFilter.TRASH_EXTENSIONS:
            return 100, "TRASH_EXT"
            
        if filename.lower() in DebrisFilter.TRASH_FILENAMES:
            return 100, "TRASH_SYS"
            
        if "conflit" in filename.lower() and "copie" in filename.lower():
            return 90, "CONFLICT_COPY"
            
        return 0, "PENDING"