"""
Module de reporting et visualisation automatique.
"""
import pandas as pd
import matplotlib.pyplot as plt
import os
from utils.core import log_info

def generate_report(meta_path, output_dir):
    """Génère un rapport simple sur les lots traités et sauvegarde une visualisation."""
    if not os.path.isfile(meta_path):
        log_info(f"Fichier meta introuvable : {meta_path}")
        return False
    meta_df = pd.read_parquet(meta_path)
    status_counts = meta_df['status'].value_counts()
    plt.figure(figsize=(6,4))
    status_counts.plot(kind='bar', color='skyblue')
    plt.title('Statut des lots')
    plt.xlabel('Statut')
    plt.ylabel('Nombre de lots')
    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    plot_path = os.path.join(output_dir, 'batch_status_report.png')
    plt.savefig(plot_path)
    log_info(f"Rapport de statut généré : {plot_path}")
    return True
