"""
Module de post-traitement pour la pipeline de traduction.
Utilise la configuration centralisée.
"""
import os
from utils.core import log_info
from utils.config_loader import load_config, get_abs_path_from_config

config = load_config()

def postprocess_batch(batch_name):
    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    batch_path = os.path.join(batches_dir, batch_name)
    log_info(f"Post-traitement du lot : {batch_path}")
    # ... logique de post-traitement à implémenter ...
    return True
