"""
Module de prétraitement pour la pipeline de traduction.
Utilise la configuration centralisée.
"""

import os
from utils.core import log_info, log_execution_time
from utils.config_loader import load_config, get_abs_path_from_config

config = load_config()

@log_execution_time('Prétraitement')
def preprocess_batch(batch_name):
    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    batch_path = os.path.join(batches_dir, batch_name)
    log_info(f"Prétraitement du lot : {batch_path}")

    import pandas as pd
    # Lecture du lot
    df = pd.read_parquet(batch_path)

    # Prétraitement minimal : strip et vérification non vide
    if 'en' not in df.columns:
        raise ValueError(f"Colonne 'en' absente dans le lot {batch_name}")
    df['en'] = df['en'].astype(str).str.strip()
    if df['en'].isnull().any() or (df['en'] == '').any():
        raise ValueError(f"Des phrases vides ou nulles détectées dans le lot {batch_name}")

    # Sauvegarde (ici on écrase le lot, sinon choisis un autre dossier)
    df.to_parquet(batch_path, index=False)
    log_info(f"Prétraitement terminé pour {batch_name}")
    return True
