# Paramètres globaux de l'application

DB_NAME = "inventory_v2.sqlite"

# Taille des blocs de lecture (64KB) pour le hachage
CHUNK_SIZE = 65536  

# Extensions à scanner (ex: ['.pdf', '.dwg']). 
# Mettre None pour scanner tous les fichiers.
TARGET_EXTENSIONS = None

# --- DÉFINITION DES FAMILLES TECHNIQUES (BTP & IT) ---

EXT_CAD = {'.dwg', '.dxf', '.rvt', '.ifc', '.nwd', '.pln'}
EXT_DOC = {'.pdf', '.docx', '.doc', '.odt', '.rtf', '.txt', '.md'}
EXT_IMG = {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.webp', '.bmp'}
EXT_XLS = {'.xlsx', '.xls', '.csv', '.ods', '.xml'}
EXT_ARC = {'.zip', '.rar', '.7z', '.tar', '.gz'}

# Mapping inverse pour le Dispatcher
# Permet de savoir instantanément : ".dwg" -> "meta_cad"
EXTENSION_MAP = {}
for ext in EXT_CAD: EXTENSION_MAP[ext] = 'meta_cad'
for ext in EXT_DOC: EXTENSION_MAP[ext] = 'meta_document'
for ext in EXT_IMG: EXTENSION_MAP[ext] = 'meta_visual'
for ext in EXT_XLS: EXTENSION_MAP[ext] = 'meta_spreadsheet'
for ext in EXT_ARC: EXTENSION_MAP[ext] = 'meta_archive'