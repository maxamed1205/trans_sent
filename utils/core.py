def check_data_source_exists(config):
    """Vérifie la présence d'au moins un fichier source (.txt ou .csv) dans data/raw/"""
    from utils.config_loader import get_abs_path_from_config
    data_dir = get_abs_path_from_config(config, 'raw_data_dir')
    check_dir_exists(data_dir, f"Le dossier source des données n'existe pas : {data_dir}")
    files = [f for f in os.listdir(data_dir) if f.endswith('.txt') or f.endswith('.csv')]
    if not files:
        log_error(f"Aucun fichier source (.txt ou .csv) trouvé dans {data_dir}")
        raise FileNotFoundError(f"Aucun fichier source (.txt ou .csv) trouvé dans {data_dir}")
    return files
import os
import logging
from datetime import datetime

# --- Gestion des chemins ---
def get_abs_path(root, rel_path):
    """Retourne le chemin absolu à partir de la racine et d'un chemin relatif."""
    return os.path.abspath(os.path.join(root, rel_path))

def ensure_dir_exists(path):
    """Crée le dossier s'il n'existe pas."""
    if not os.path.exists(path):
        os.makedirs(path)

def safe_open(path, mode='r', encoding=None):
    """Ouvre un fichier en s'assurant que le dossier existe."""
    ensure_dir_exists(os.path.dirname(path))
    return open(path, mode, encoding=encoding)

# --- Gestion des erreurs/logs ---
def setup_logger(log_dir, log_name='pipeline.log'):
    ensure_dir_exists(log_dir)
    log_path = os.path.join(log_dir, log_name)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger()

def log_error(msg, exc=None):
    logging.error(msg)
    if exc:
        logging.error(str(exc))
    print(f"[ERREUR] {msg}")
    if exc:
        print(f"[EXCEPTION] {exc}")

def log_info(msg):
    logging.info(msg)
    print(f"[INFO] {msg}")

# --- Vérifications génériques ---
def check_file_exists(path, msg=None):
    if not os.path.isfile(path):
        log_error(msg or f"Fichier introuvable : {path}")
        raise FileNotFoundError(msg or f"Fichier introuvable : {path}")

def check_dir_exists(path, msg=None):
    if not os.path.isdir(path):
        log_error(msg or f"Dossier introuvable : {path}")
        raise FileNotFoundError(msg or f"Dossier introuvable : {path}")

def check_not_empty(lst, msg=None):
    if not lst:
        log_error(msg or "Liste vide alors qu'au moins un élément est attendu.")
        raise ValueError(msg or "Liste vide alors qu'au moins un élément est attendu.")

# --- Utilitaires divers ---
def timestamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
