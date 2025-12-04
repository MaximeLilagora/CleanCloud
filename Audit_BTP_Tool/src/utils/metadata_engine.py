import os
from typing import Tuple, Dict, Any, Optional

# Dépendances optionnelles (Try/Except pour ne pas casser si module manquant)
try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

try:
    from PIL import Image
except ImportError:
    Image = None

class MetadataDispatcher:
    """
    Analyse l'extension et route vers l'extracteur spécifique.
    Retourne : (Nom_Table_SQL, Dictionnaire_Données)
    """

    @staticmethod
    def dispatch(file_path: str, extension: str) -> Tuple[Optional[str], Dict[str, Any]]:
        
        # Normalisation
        ext = extension.lower()
        
        # 1. Dispatch PDF / DOCS
        if ext == '.pdf':
            return 'meta_document', PdfExtractor.extract(file_path)
            
        # 2. Dispatch IMAGES
        elif ext in ['.jpg', '.jpeg', '.png', '.tiff', '.webp', '.bmp']:
            return 'meta_visual', ImageExtractor.extract(file_path)
            
        # 3. Dispatch CAD (DWG, RVT)
        elif ext in ['.dwg', '.dxf', '.rvt']:
            return 'meta_cad', CadExtractor.extract(file_path)

        # 4. Dispatch EXCEL
        elif ext in ['.xlsx', '.xls', '.csv']:
            return 'meta_spreadsheet', SpreadsheetExtractor.extract(file_path)

        # Pas de table spécifique
        return None, {}

class PdfExtractor:
    @staticmethod
    def extract(path: str) -> dict:
        if not PdfReader: return {}
        data = {'page_count': 0, 'is_encrypted': False, 'producer_tool': None, 'author': None}
        try:
            reader = PdfReader(path)
            data['is_encrypted'] = reader.is_encrypted
            
            if reader.is_encrypted:
                try: 
                    reader.decrypt("")
                except: 
                    return data # Stop si crypté dur
            
            data['page_count'] = len(reader.pages)
            
            if reader.metadata:
                meta = reader.metadata
                data['producer_tool'] = meta.get('/Producer', '') or meta.get('/Creator', '')
                data['author'] = meta.get('/Author', '')
                
        except Exception:
            pass
        return data

class ImageExtractor:
    @staticmethod
    def extract(path: str) -> dict:
        data = {'width': 0, 'height': 0, 'color_space': 'UNKNOWN'}
        if not Image: return data
        try:
            with Image.open(path) as img:
                data['width'], data['height'] = img.size
                data['color_space'] = img.mode # RGB, CMYK, etc.
        except Exception:
            pass
        return data

class CadExtractor:
    @staticmethod
    def extract(path: str) -> dict:
        # L'extraction DWG réelle demande des libs lourdes.
        # Ici on simule une détection basique ou on laisse vide pour futur usage.
        return {
            'software_version': 'Unknown (Requires deeper scan)', 
            'has_xrefs': False,
            'scale': '1:1'
        }

class SpreadsheetExtractor:
    @staticmethod
    def extract(path: str) -> dict:
        # Extraction basique possible via openpyxl si installé
        return {'sheet_count': 1, 'has_macros': path.endswith('.xlsm')}