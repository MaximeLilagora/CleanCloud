import os
import re
from typing import Tuple, Dict, Any, Optional

# --- GESTION DES DÉPENDANCES OPTIONNELLES ---
# Grâce au pip install, ces imports réussiront
try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

try:
    from PIL import Image
except ImportError:
    Image = None

try:
    import olefile
except ImportError:
    olefile = None

try:
    import ezdxf
    from ezdxf.lldxf.const import DXFStructureError
except ImportError:
    ezdxf = None

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
            
        # 3. Dispatch CAD (DWG, RVT, IFC)
        elif ext in ['.dwg', '.dxf', '.rvt', '.ifc']:
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
                # Tentative de décryptage à vide
                try: 
                    reader.decrypt("")
                except: 
                    return data 
            
            try:
                data['page_count'] = len(reader.pages)
            except:
                pass
            
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
    """
    Expertise technique BTP : DWG, RVT, IFC.
    """
    
    ACAD_VERSIONS = {
        b'AC1032': 'AutoCAD 2018/2023',
        b'AC1027': 'AutoCAD 2013/2017',
        b'AC1024': 'AutoCAD 2010/2012',
        b'AC1021': 'AutoCAD 2007/2009',
        b'AC1018': 'AutoCAD 2004/2006',
        b'AC1015': 'AutoCAD 2000/2002',
        b'AC1014': 'AutoCAD R14',
        b'AC1009': 'AutoCAD R11/R12'
    }

    @staticmethod
    def extract(path: str) -> dict:
        ext = os.path.splitext(path)[1].lower()
        
        if ext == '.dwg':
            return CadExtractor._analyze_dwg_binary(path)
        elif ext == '.dxf':
            return CadExtractor._analyze_dxf(path)
        elif ext == '.rvt':
            return CadExtractor._analyze_rvt(path)
        elif ext == '.ifc':
            return CadExtractor._analyze_ifc(path)
            
        return {'software_version': 'Unknown Format', 'has_xrefs': False, 'scale': 'N/A'}

    @staticmethod
    def _analyze_dwg_binary(path: str) -> dict:
        """Lecture binaire optimisée pour les DWG (Performance)"""
        data = {'software_version': 'Unknown', 'has_xrefs': False, 'scale': '1:1'}
        try:
            with open(path, 'rb') as f:
                header = f.read(6)
                if header in CadExtractor.ACAD_VERSIONS:
                    data['software_version'] = CadExtractor.ACAD_VERSIONS[header]
                else:
                    data['software_version'] = f"Legacy/New ({header.decode('utf-8', errors='ignore')})"
        except Exception as e:
            data['software_version'] = f"Error: {str(e)}"
        return data

    @staticmethod
    def _analyze_dxf(path: str) -> dict:
        """Lecture via ezdxf pour les DXF (Précision)"""
        data = {'software_version': 'DXF Unknown', 'has_xrefs': False, 'scale': '1:1'}
        
        # Fallback si ezdxf n'est pas installé malgré tout
        if not ezdxf:
            return CadExtractor._analyze_dwg_binary(path)

        try:
            # dxf_info est très rapide car il ne charge pas la géométrie
            info = ezdxf.dxf_info(path)
            data['software_version'] = f"DXF {info.release}"
            
            # Pour l'encodage et la version, c'est fiable. 
            # Pour les XREFs, il faudrait charger le fichier (lent), on évite pour le moment.
        except (IOError, DXFStructureError):
            pass
            
        return data

    @staticmethod
    def _analyze_rvt(path: str) -> dict:
        data = {'software_version': 'Revit Unknown', 'has_xrefs': False, 'scale': 'N/A'}
        
        if not olefile:
            return data
            
        try:
            if olefile.isOleFile(path):
                with olefile.OleFileIO(path) as ole:
                    # Revit stocke les infos dans 'BasicFileInfo'
                    if 'BasicFileInfo' in ole.listdir():
                        content = ole.openstream('BasicFileInfo').read()
                        # Décodage latin-1 ou utf-16 brute force pour trouver l'année
                        content_str = content.decode('utf-16', errors='ignore')
                        
                        match = re.search(r'Revit (\d{4})', content_str)
                        if match:
                            data['software_version'] = f"Revit {match.group(1)}"
                                
                        if "Worksharing" in content_str or "Central" in content_str:
                            data['has_xrefs'] = True 
        except Exception:
            pass
        return data

    @staticmethod
    def _analyze_ifc(path: str) -> dict:
        data = {'software_version': 'IFC Unknown', 'has_xrefs': False, 'scale': '1:1'}
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                header = f.read(1000)
                if "IFC2X3" in header:
                    data['software_version'] = "IFC 2x3"
                elif "IFC4" in header:
                    data['software_version'] = "IFC 4"
                
                if "IFCSITE" in header.upper():
                    data['scale'] = "Georef"
        except Exception:
            pass
        return data

class SpreadsheetExtractor:
    @staticmethod
    def extract(path: str) -> dict:
        return {'sheet_count': 1, 'has_macros': path.endswith('.xlsm')}