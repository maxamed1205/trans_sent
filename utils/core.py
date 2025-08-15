# --- Utilitaire de mesure de temps d'exécution ---
import functools
import time

def log_execution_time(operation_name=None):
    """
    Décorateur pour mesurer et logger le temps d'exécution d'une fonction.
    Utilisation :
        @log_execution_time()
        def ma_fonction(...): ...
    ou
        @log_execution_time('nom personnalisé')
        def ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            logger = logging.getLogger()
            start = time.perf_counter()
            logger.info(f"[TIMER] Début de '{name}'")
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            logger.info(f"[TIMER] Fin de '{name}' : {elapsed:.3f} secondes")
            return result
        return wrapper
    return decorator

import os
import logging
import shutil
from datetime import datetime
from logging import StreamHandler, FileHandler, Formatter, getLogger, INFO, DEBUG, ERROR

# --- Gestion du device (CPU/GPU) ---
def get_best_device(device_cfg):
    """
    Retourne 'cuda' si demandé et disponible, sinon 'cpu'.
    Log l'information sur le choix du device.
    """
    import torch
    if device_cfg == 'cuda':
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        if device == 'cpu':
            log_info('GPU non disponible, passage automatique sur CPU.')
        else:
            log_info('GPU CUDA détecté et utilisé.')
        return device
    return device_cfg

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
    # Niveau de log configurable via config.yaml ou variable d'env
    import yaml
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
    log_level = INFO
    if os.path.isfile(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            try:
                config = yaml.safe_load(f)
                level_str = str(config.get('log_level', 'INFO')).upper()
                if level_str == 'DEBUG':
                    log_level = DEBUG
                elif level_str == 'ERROR':
                    log_level = ERROR
            except Exception:
                pass
    # Fichier log avec timestamp
    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_name_ts = f"pipeline_{now}.log"
    log_path_ts = os.path.join(log_dir, log_name_ts)
    logger = getLogger()
    logger.setLevel(log_level)
    if logger.hasHandlers():
        logger.handlers.clear()
    file_handler = FileHandler(log_path_ts, encoding='utf-8')
    file_handler.setFormatter(Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(file_handler)
    stream_handler = StreamHandler()
    stream_handler.setFormatter(Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(stream_handler)
    # Copie le dernier log en pipeline.log (pour accès rapide)
    log_path = os.path.join(log_dir, 'pipeline.log')
    try:
        shutil.copyfile(log_path_ts, log_path)
    except Exception:
        pass
    return logger

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

def ensure_dirs_exist(paths):
    """
    Crée tous les dossiers de la liste s'ils n'existent pas.
    """
    for path in paths:
        if not os.path.isdir(path):
            os.makedirs(path, exist_ok=True)
            log_info(f"Dossier créé automatiquement : {path}")

def check_not_empty(lst, msg=None):
    if not lst:
        log_error(msg or "Liste vide alors qu'au moins un élément est attendu.")
        raise ValueError(msg or "Liste vide alors qu'au moins un élément est attendu.")

# --- Utilitaires divers ---
def timestamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
