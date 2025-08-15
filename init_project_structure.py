
import os
import pandas as pd
from datetime import datetime
from utils.config_loader import load_config
from utils.core import ensure_dir_exists, log_info

config = load_config()
folders = config["project_structure"]["folders"]
for folder in folders:
    ensure_dir_exists(folder)

# Exemple de DataFrame pour un batch de traduction
batch_columns = [
    'id_phrase', 'batch_id', 'en_phrase', 'fr_phrase',
    'nb_tokens_en', 'nb_tokens_fr',
    'nb_negations_en', 'nb_negations_fr',
    'detected_negation_en', 'detected_negation_fr',
    'score_qualite', 'date_traduction',
    'status', 'model_version', 'commentaire'
]

batch_df = pd.DataFrame(columns=batch_columns)
batch_path = os.path.join(config["output"]["batches_dir"], 'batch_0001.parquet')
batch_df.to_parquet(batch_path, index=False)

# Exemple de DataFrame pour la meta des lots
meta_columns = [
    'batch_id', 'source', 'parametres', 'model_version',
    'date_traitement', 'nb_phrases', 'commentaire'
]

meta_df = pd.DataFrame(columns=meta_columns)
meta_path = os.path.join(config["output"]["meta_dir"], 'batch_info.parquet')
meta_df.to_parquet(meta_path, index=False)

log_info('Structure de dossiers et fichiers initialisée avec succès.')
