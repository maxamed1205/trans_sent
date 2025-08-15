"""
Module de reporting et visualisation automatique.
"""
import pandas as pd
import matplotlib.pyplot as plt
import os
from utils.core import log_info

import seaborn as sns
from datetime import datetime

RESULTS_DIR = os.path.join("analysis", "results")
os.makedirs(RESULTS_DIR, exist_ok=True)
META_DIR = os.path.join("translations", "meta")
BATCH_META = os.path.join(META_DIR, "batch_info.parquet")
FR_BATCH_META = os.path.join(META_DIR, "fr_batch_info.parquet")

def generate_report():
    # Lire les fichiers meta
    if not os.path.exists(BATCH_META) or not os.path.exists(FR_BATCH_META):
        log_info("Fichiers meta manquants, rapport non généré.")
        return None
    batch_info = pd.read_parquet(BATCH_META)
    fr_batch_info = pd.read_parquet(FR_BATCH_META)

    # Statistiques globales
    n_batches = len(batch_info)
    n_fr_batches = len(fr_batch_info)
    n_phrases = batch_info['num_phrases'].sum() if 'num_phrases' in batch_info else None
    n_fr_phrases = fr_batch_info['num_phrases'].sum() if 'num_phrases' in fr_batch_info else None
    # Statut des lots
    status_counts = fr_batch_info['status'].value_counts() if 'status' in fr_batch_info else None

    # Temps moyen par lot (si dispo)
    avg_time = fr_batch_info['duration'].mean() if 'duration' in fr_batch_info else None

    # Générer un histogramme du statut des lots
    plt.figure(figsize=(6,4))
    if status_counts is not None:
        sns.barplot(x=status_counts.index, y=status_counts.values)
        plt.title('Statut des lots FR')
        plt.ylabel('Nombre de lots')
        plt.xlabel('Statut')
        plt.tight_layout()
        img_path = os.path.join(RESULTS_DIR, f"batch_status_report_{datetime.now().strftime('%Y%m%d-%H%M%S')}.png")
        plt.savefig(img_path)
        plt.close()
    else:
        img_path = None

    # Sauvegarder un CSV synthétique
    report_csv = os.path.join(RESULTS_DIR, f"report_{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv")
    fr_batch_info.to_csv(report_csv, index=False)

    # Logger le rapport
    log_info(f"Rapport batchs : {n_fr_batches} lots FR, {n_batches} lots EN, {n_fr_phrases} phrases traduites.")
    if avg_time:
        log_info(f"Temps moyen par lot FR : {avg_time:.2f}s")
    if img_path:
        log_info(f"Histogramme sauvegardé : {img_path}")
    log_info(f"CSV synthétique : {report_csv}")
    return report_csv, img_path

if __name__ == "__main__":
    generate_report()
