

import os
import pandas as pd
from datetime import datetime
import json
from utils.config_loader import load_config
from utils.core import (
    ensure_dir_exists,
    check_dir_exists,
    log_error
)

config = load_config()
snapshots_dir = config["model"]["snapshots_dir"]
output_batches_dir = config["output"]["batches_dir"]
output_meta_dir = config["output"]["meta_dir"]

try:
    check_dir_exists(snapshots_dir, "Le dossier de snapshots du modèle est introuvable.")
    ensure_dir_exists(output_batches_dir)
    ensure_dir_exists(output_meta_dir)
except Exception as e:
    log_error(str(e))
    raise

# Recherche du premier snapshot valide
model_dir = None
config_path = None
for subdir in os.listdir(snapshots_dir):
    subdir_path = os.path.join(snapshots_dir, subdir)
    if os.path.isdir(subdir_path):
        candidate_config = os.path.join(subdir_path, "config.json")
        if os.path.isfile(candidate_config):
            model_dir = subdir_path
            config_path = candidate_config
            break
if not model_dir or not config_path:
    log_error("Aucun snapshot valide avec config.json trouvé dans le dossier de snapshots.")
    raise FileNotFoundError("Aucun snapshot valide avec config.json trouvé dans le dossier de snapshots.")

# Lecture automatique de la version du modèle
with open(config_path, encoding="utf-8") as f:
    model_config = json.load(f)
    model_version = model_config.get("_name_or_path") or model_config.get("model_type")
if not model_version:
    log_error("Impossible de déterminer la version du modèle à partir du config.json.")
    raise ValueError("Impossible de déterminer la version du modèle à partir du config.json.")

# Exemple de données enrichies pour une phrase traduite
batch_data = [{
    'id_phrase': 1,
    'batch_id': 'batch_0001',
    'en_phrase': "The patient does not have any signs of infection.",
    'fr_phrase': "Le patient n'a aucun signe d'infection.",
    'nb_tokens_en': 10,  # nombre de tokens anglais (exemple)
    'nb_tokens_fr': 8,   # nombre de tokens français (exemple)
    'nb_negations_en': 1,  # nombre de négations détectées en anglais
    'nb_negations_fr': 1,  # nombre de négations détectées en français
    'detected_negation_en': ["not"],  # liste des mots de négation détectés en anglais
    'detected_negation_fr': ["aucun"],  # liste des mots de négation détectés en français
    'score_qualite': None,  # à remplir si un score de qualité est calculé
    'date_traduction': datetime.now().isoformat(),
    'status': 'auto',  # statut de la traduction (auto, validé, à corriger...)
    'model_version': model_version,  # version du modèle détectée automatiquement
    'commentaire': ''  # champ libre pour remarques
}]



batch_df = pd.DataFrame(batch_data)
batch_path = os.path.join(output_batches_dir, 'batch_0001.parquet')
batch_df.to_parquet(batch_path, index=False)

# Création du fichier meta batch_info.parquet
meta_data = [{
    'batch_id': 'batch_0001',
    'source': 'negation_medical.txt',
    'parametres': json.dumps({'lang_pair': 'en-fr', 'preprocessing': 'default'}),
    'model_version': model_version,
    'date_traitement': datetime.now().isoformat(),
    'nb_phrases': len(batch_df),
    'commentaire': 'Exemple de lot de traduction enrichi.'
}]

meta_df = pd.DataFrame(meta_data)
meta_path = os.path.join(output_meta_dir, 'batch_info.parquet')
meta_df.to_parquet(meta_path, index=False)

# Affichage des en-têtes et d'un exemple de ligne pour batch
print('Colonnes du batch :')
print(batch_df.columns.tolist())
print('\nExemple de contenu :')
print(batch_df.head(1).to_markdown(index=False))

# Affichage des en-têtes et d'un exemple de ligne pour meta
print('\nColonnes du meta :')
print(meta_df.columns.tolist())
print('\nExemple de contenu meta :')
print(meta_df.head(1).to_markdown(index=False))
