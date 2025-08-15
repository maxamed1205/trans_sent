"""
Module de gestion du statut des lots pour la pipeline.
"""
import pandas as pd
import os
from utils.core import log_info, log_error, log_execution_time

def update_batch_status(meta_path, batch_id, new_status):
@log_execution_time('Maj statut lot')
def update_batch_status(meta_path, batch_id, new_status):
    """Met à jour le statut d'un lot dans le fichier meta."""
    if not os.path.isfile(meta_path):
        log_error(f"Fichier meta introuvable : {meta_path}")
        return False
    meta_df = pd.read_parquet(meta_path)
    if batch_id not in meta_df['batch_id'].values:
        log_error(f"Batch ID {batch_id} non trouvé dans le meta.")
        return False
    meta_df.loc[meta_df['batch_id'] == batch_id, 'status'] = new_status
    meta_df.to_parquet(meta_path, index=False)
    log_info(f"Statut du lot {batch_id} mis à jour : {new_status}")
    return True
