"""
Module d'archivage automatique des lots traités.
"""
import os
import shutil
from utils.core import log_info, log_error

def archive_batch(batch_path, archive_dir):
    """Déplace un lot traité dans le dossier d'archive."""
    if not os.path.isfile(batch_path):
        log_error(f"Fichier de lot introuvable : {batch_path}")
        return False
    os.makedirs(archive_dir, exist_ok=True)
    dest_path = os.path.join(archive_dir, os.path.basename(batch_path))
    shutil.move(batch_path, dest_path)
    log_info(f"Lot archivé : {dest_path}")
    return True
