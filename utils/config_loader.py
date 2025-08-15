import os
import yaml
from utils.core import log_error, check_dir_exists

CONFIG_REQUIRED_KEYS = [
    'raw_data_dir', 'processed_data_dir', 'batches_dir', 'meta_dir', 'logs_dir', 'models_dir', 'models'
]

# Charge la configuration YAML et valide les clés principales

def load_config(config_path='config.yaml', root=None):
    if root is None:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_full_path = os.path.join(root, config_path) if not os.path.isabs(config_path) else config_path
    if not os.path.isfile(config_full_path):
        log_error(f"Fichier de configuration introuvable : {config_full_path}")
        raise FileNotFoundError(f"Fichier de configuration introuvable : {config_full_path}")
    with open(config_full_path, 'r') as f:
        config = yaml.safe_load(f)
    validate_config(config, root)
    return config

def validate_config(config, root):
    for key in CONFIG_REQUIRED_KEYS:
        if key not in config:
            log_error(f'Clé manquante dans la config : {key}')
            raise KeyError(f'Clé manquante dans la config : {key}')
    # Crée les dossiers principaux s'ils n'existent pas
    from utils.core import ensure_dirs_exist
    dir_keys = ['raw_data_dir', 'processed_data_dir', 'batches_dir', 'meta_dir', 'logs_dir', 'models_dir']
    abs_paths = [os.path.join(root, config[key]) for key in dir_keys]
    ensure_dirs_exist(abs_paths)
    # Vérifie que les dossiers existent bien
    for abs_path in abs_paths:
        check_dir_exists(abs_path, f'Dossier manquant : {abs_path}')

# Utilitaire pour obtenir un chemin absolu à partir de la config

def get_abs_path_from_config(config, key, root=None):
    if root is None:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    rel_path = config[key]
    return os.path.abspath(os.path.join(root, rel_path))
