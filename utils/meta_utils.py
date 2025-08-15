import pandas as pd

def get_batches_to_process(meta_path):
    """Retourne la liste des batch_id à traiter (statut != 'termine' et != 'erreur')."""
    df = pd.read_parquet(meta_path)
    return df.loc[~df['status'].isin(['termine', 'erreur']), 'batch_id'].tolist()

def update_batch_status(meta_path, batch_id, new_status):
    """Met à jour le statut d'un lot dans le meta."""
    df = pd.read_parquet(meta_path)
    df.loc[df['batch_id'] == batch_id, 'status'] = new_status
    df.to_parquet(meta_path, index=False)
