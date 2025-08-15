
"""
Script principal d'automatisation de la pipeline de traduction
Pipeline modulaire : prétraitement, traduction, post-traitement, analyse, gestion des statuts, logs, archivage, reporting, parallélisation.
"""

import argparse
import glob
import os
from utils.config_loader import load_config, get_abs_path_from_config
from utils.core import log_info, log_error
# ...imports pipeline (étapes) commentés pour l'instant...

def get_all_batches(batches_dir):
    return glob.glob(os.path.join(batches_dir, '*.parquet'))

def main(parallel=False, max_workers=2, force_rebuild=False, batch_size=100):
    config = load_config()
    # Vérification de la présence des données sources
    from utils.core import check_data_source_exists
    files = check_data_source_exists(config)
    print(f"Fichiers sources trouvés : {files}")

    # Étape 2 : Génération des lots
    from pipeline.batch_generation_en import generate_batches
    generate_batches(batch_size=batch_size, force_rebuild=force_rebuild)
    print("Lots générés (ou déjà présents).")

    # Étape 3 : Validation du format des lots
    from tests.test_batch_format import test_batch_format
    test_batch_format()
    print("Format des lots validé.")

    # --- Étape 5 : Pipeline automatisée, sélection des lots à traiter selon le statut ---
    from utils.meta_utils import get_batches_to_process
    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    meta_dir = get_abs_path_from_config(config, 'meta_dir')
    meta_path = os.path.join(meta_dir, 'batch_info.parquet')
    batch_paths = get_all_batches(batches_dir)
    # Correction : extraire batch_id depuis le nom de fichier en_batch_XXXX.parquet
    batch_id_to_path = {}
    for p in batch_paths:
        fname = os.path.basename(p)
        if fname.startswith('en_batch_') and fname.endswith('.parquet'):
            batch_id = fname[len('en_'):-len('.parquet')]
            batch_id_to_path[batch_id] = p
    batches_to_process = get_batches_to_process(meta_path)
    if batches_to_process:
        log_info(f"[ÉTAPE 5] {len(batches_to_process)} lot(s) à traiter : {batches_to_process}")
    else:
        log_info("[ÉTAPE 5] Aucun lot à traiter : tous les lots sont terminés ou en erreur.")

    # --- Étapes suivantes à valider plus tard ---
    from utils.meta_utils import update_batch_status

    def process_one_batch(batch_id):
        batch_name = os.path.basename(batch_id_to_path[batch_id])
        try:
            update_batch_status(meta_path, batch_id, 'en_cours')
            # Étape 1 : prétraitement
            from pipeline.preprocessing import preprocess_batch
            preprocess_batch(batch_name)
            # Étape 2 : traduction
            from pipeline.translation import translate_batch
            translate_batch(batch_name)
            # Les étapes suivantes seront ajoutées après validation de la traduction
            update_batch_status(meta_path, batch_id, 'termine')
            log_info(f"Lot {batch_id} prétraité et traduit.")
        except Exception as e:
            update_batch_status(meta_path, batch_id, 'erreur')
            log_error(f"Erreur sur le lot {batch_id}", exc=e)
        return True

    log_info("--- Démarrage de la pipeline automatisée ---")
    for batch_id in batches_to_process:
        process_one_batch(batch_id)
    log_info("--- Pipeline terminée avec succès ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de traduction modulaire")
    parser.add_argument('--parallel', action='store_true', help='Activer le traitement parallèle des lots')
    parser.add_argument('--max-workers', type=int, default=2, help='Nombre de workers pour le mode parallèle')
    parser.add_argument('--force-rebuild', action='store_true', help='Forcer la régénération des lots')
    parser.add_argument('--batch-size', type=int, default=100, help='Taille des lots à générer')
    args = parser.parse_args()
    try:
        main(parallel=args.parallel, max_workers=args.max_workers, force_rebuild=args.force_rebuild, batch_size=args.batch_size)
    except Exception as e:
        log_error("Erreur critique dans la pipeline", exc=e)
        raise
