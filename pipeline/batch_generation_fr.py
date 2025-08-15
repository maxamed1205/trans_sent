"""
Génération des lots pour la traduction (fichiers fr_batch_XXXX.parquet et meta fr_batch_info.parquet)
"""
import os
import pandas as pd
from utils.core import log_info, ensure_dir_exists
from utils.config_loader import load_config, get_abs_path_from_config

def generate_fr_batch(en_batch_path, fr_sentences):
    """
    Crée un fichier batch de traduction à partir d'un batch source et d'une liste de phrases traduites.
    - en_batch_path : chemin du batch source (en_batch_XXXX.parquet)
    - fr_sentences : liste de phrases traduites (même ordre)
    """
    config = load_config()
    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    meta_dir = get_abs_path_from_config(config, 'meta_dir')
    ensure_dir_exists(batches_dir)
    ensure_dir_exists(meta_dir)

    # Charger le batch source
    df = pd.read_parquet(en_batch_path)
    if len(df) != len(fr_sentences):
        raise ValueError("Le nombre de traductions ne correspond pas au nombre de phrases du batch source.")

    # Colonnes à conserver et ordre : id_phrase, fr, nb_words, source_file, line_number
    cols = []
    if 'id_phrase' in df.columns:
        cols.append('id_phrase')
    df_out = df[cols].copy()
    df_out['fr'] = fr_sentences
    if 'nb_words' in df.columns:
        df_out['nb_words'] = df['nb_words']
    if 'source_file' in df.columns:
        df_out['source_file'] = df['source_file']
    if 'line_number' in df.columns:
        df_out['line_number'] = df['line_number']
    # Réordonner les colonnes
    ordered_cols = []
    if 'id_phrase' in df_out.columns:
        ordered_cols.append('id_phrase')
    ordered_cols.append('fr')
    if 'nb_words' in df_out.columns:
        ordered_cols.append('nb_words')
    if 'source_file' in df_out.columns:
        ordered_cols.append('source_file')
    if 'line_number' in df_out.columns:
        ordered_cols.append('line_number')
    df_out = df_out[ordered_cols]

    # Générer le nom du batch traduit
    en_batch_name = os.path.basename(en_batch_path)
    if en_batch_name.startswith('en_batch_') and en_batch_name.endswith('.parquet'):
        fr_batch_name = 'fr_' + en_batch_name[len('en_'):]
    else:
        fr_batch_name = 'fr_' + en_batch_name
    fr_batch_path = os.path.join(batches_dir, fr_batch_name)
    df_out.to_parquet(fr_batch_path, index=False)
    log_info(f"Lot de traduction généré : {fr_batch_path} ({len(df_out)} phrases)")

    # Mettre à jour le meta fr_batch_info.parquet
    meta_path = os.path.join(meta_dir, 'fr_batch_info.parquet')
    meta_record = {
        'batch_id': fr_batch_name.replace('.parquet',''),
        'fr_batch_path': fr_batch_path,
        'date_creation': pd.Timestamp.now().isoformat(),
        'nb_phrases': len(df),
        'status': 'en_attente',
        'commentaire': ''
    }
    if os.path.exists(meta_path):
        meta_df = pd.read_parquet(meta_path)
        meta_df = pd.concat([meta_df, pd.DataFrame([meta_record])], ignore_index=True)
    else:
        meta_df = pd.DataFrame([meta_record])
    meta_df.to_parquet(meta_path, index=False)
    log_info(f"Meta fr_batch_info.parquet mis à jour : {meta_path}")
    return fr_batch_path
