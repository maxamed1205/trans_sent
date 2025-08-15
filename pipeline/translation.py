"""
Module de traduction pour la pipeline de traduction.
Utilise la configuration centralisée.
"""
import os
import pandas as pd
from transformers import MarianMTModel, MarianTokenizer
from utils.core import log_info, log_error, ensure_dir_exists
from utils.config_loader import load_config, get_abs_path_from_config

config = load_config()

def translate_batch(batch_name):
    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    batch_path = os.path.join(batches_dir, batch_name)
    models = config['models']
    model_info = models[0]  # Utilise le premier modèle de la liste
    model_dir = get_abs_path_from_config(config, 'models_dir')
    model_path = os.path.join(model_dir, model_info['path'])
    device = config.get('device', 'cpu')
    log_info(f"Traduction du lot : {batch_path} avec le modèle {model_info['name']}")

    # Charger le batch
    try:
        df = pd.read_parquet(batch_path)
    except Exception as e:
        log_error(f"Erreur lors du chargement du batch : {batch_path}", exc=e)
        raise

    if 'en' not in df.columns:
        log_error(f"Colonne 'en' absente dans le batch : {batch_path}")
        raise ValueError(f"Colonne 'en' absente dans le batch : {batch_path}")

    # Vérifier la présence des fichiers essentiels du modèle
    required_files = [
        'config.json',
        'pytorch_model.bin',
        'tokenizer_config.json',
        'vocab.json',
        'source.spm',
        'target.spm'
    ]
    missing = [f for f in required_files if not os.path.isfile(os.path.join(model_path, f))]
    if missing:
        log_error(f"Fichiers manquants dans le dossier du modèle ({model_path}) : {', '.join(missing)}")
        raise FileNotFoundError(f"Fichiers manquants dans le dossier du modèle ({model_path}) : {', '.join(missing)}")

    # Charger le modèle MarianMT
    try:
        tokenizer = MarianTokenizer.from_pretrained(model_path)
        model = MarianMTModel.from_pretrained(model_path)
        model.to(device)
    except Exception as e:
        log_error(f"Erreur lors du chargement du modèle MarianMT : {model_path}", exc=e)
        raise

    # Traduire chaque phrase
    translations = []
    for sent in df['en']:
        try:
            inputs = tokenizer([sent], return_tensors="pt", padding=True)
            inputs = {k: v.to(device) for k, v in inputs.items()}
            translated = model.generate(**inputs)
            fr_text = tokenizer.batch_decode(translated, skip_special_tokens=True)[0]
            translations.append(fr_text)
        except Exception as e:
            log_error(f"Erreur de traduction pour la phrase : {sent}", exc=e)
            translations.append("")

    df['fr'] = translations

    # Générer le batch fr et le meta associé
    from pipeline.batch_generation_fr import generate_fr_batch
    en_batch_path = batch_path
    fr_sentences = df['fr'].tolist()
    generate_fr_batch(en_batch_path, fr_sentences)
