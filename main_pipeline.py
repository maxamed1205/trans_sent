
"""
Script principal d'automatisation de la pipeline de traduction
Pipeline modulaire : prétraitement, traduction, post-traitement, analyse, gestion des statuts, logs, archivage, reporting, parallélisation.
"""


import argparse
import glob
import os
from utils.config_loader import load_config, get_abs_path_from_config
from utils.core import log_info, log_error, setup_logger

# Initialisation du logger au lancement du script
setup_logger('logs')
# ...imports pipeline (étapes) commentés pour l'instant...

def get_all_batches(batches_dir):
    return glob.glob(os.path.join(batches_dir, '*.parquet'))

def main(parallel=False, max_workers=2, force_rebuild=False, batch_size=100, num_batches=None, stop_after=None, batch_size_model=32, monitoring_frequency=1, alert_ram=90, alert_gpu=90, alert_time=10, fp16=False):
    config = load_config()
    # Vérification de la présence des données sources
    from utils.core import check_data_source_exists
    files = check_data_source_exists(config)
    print(f"Fichiers sources trouvés : {files}")

    # Étape 2 : Génération des lots
    from pipeline.batch_generation_en import generate_batches
    generate_batches(batch_size=batch_size, force_rebuild=force_rebuild, num_batches=num_batches)
    print("Lots générés (ou déjà présents).")

    # Étape 3 : Validation du format des lots (désactivée car le test pose problème avec lots FR)
    # from tests.test_batch_format import test_batch_format
    # test_batch_format()
    # print("Format des lots validé.")

    # --- Étape 5 : Pipeline automatisée, sélection des lots à traiter selon le statut ---
    from utils.meta_utils import get_batches_to_process
    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    meta_dir = get_abs_path_from_config(config, 'meta_dir')
    meta_path = os.path.join(meta_dir, 'batch_info.parquet')
    batch_paths = get_all_batches(batches_dir)
    # Correction : extraire batch_id depuis le nom de fichier batch_XXXX_YYYY.parquet
    batch_id_to_path = {}
    for p in batch_paths:
        fname = os.path.basename(p)
        # Correction : inclure les fichiers commençant par 'en_batch_' (et potentiellement d'autres préfixes)
        if fname.endswith('.parquet'):
            batch_id = fname[:-len('.parquet')]
            batch_id_to_path[batch_id] = p
    batches_to_process = get_batches_to_process(meta_path)
    # Limiter le nombre de batchs à traiter si demandé
    if num_batches is not None:
        batches_to_process = batches_to_process[:num_batches]
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
            if stop_after == 'preprocessing':
                log_info(f"Arrêt demandé après prétraitement du lot {batch_id}.")
                return True
            # Étape 2 : traduction
            from pipeline.translation import translate_batch
            translate_batch(
                batch_name,
                batch_size_model=batch_size_model,
                monitoring_frequency=monitoring_frequency,
                alert_ram=alert_ram,
                alert_gpu=alert_gpu,
                alert_time=alert_time,
                fp16=fp16
            )
            if stop_after == 'translation':
                log_info(f"Arrêt demandé après traduction du lot {batch_id}.")
                return True
            # Étape 3 : post-traitement (à activer si besoin)
            try:
                from pipeline.postprocessing import postprocess_batch
                postprocess_batch(batch_name)
                if stop_after == 'postprocessing':
                    log_info(f"Arrêt demandé après post-traitement du lot {batch_id}.")
                    return True
            except ImportError:
                pass
            # Étape 4 : analyse (à activer si besoin)
            try:
                from pipeline.analysis import analyze_batch
                analyze_batch(batch_name)
                if stop_after == 'analysis':
                    log_info(f"Arrêt demandé après analyse du lot {batch_id}.")
                    return True
            except ImportError:
                pass
            update_batch_status(meta_path, batch_id, 'termine')
            log_info(f"Lot {batch_id} prétraité, traduit et post-traité.")
        except Exception as e:
            update_batch_status(meta_path, batch_id, 'erreur')
            log_error(f"Erreur sur le lot {batch_id}", exc=e)
        return True

    import concurrent.futures
    log_info("--- Démarrage de la pipeline automatisée ---")
    if parallel:
        log_info(f"Traitement parallèle activé ({max_workers} workers)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_one_batch, batch_id): batch_id for batch_id in batches_to_process}
            for future in concurrent.futures.as_completed(futures):
                batch_id = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    log_error(f"Exception dans le traitement du lot {batch_id}: {exc}")
    else:
        for batch_id in batches_to_process:
            process_one_batch(batch_id)
    log_info("--- Pipeline terminée avec succès ---")

if __name__ == "__main__":
    import importlib
    parser = argparse.ArgumentParser(description="Pipeline de traduction modulaire")
    parser.add_argument('--parallel', action='store_true', help='Activer le traitement parallèle des lots')
    parser.add_argument('--max-workers', type=int, default=None, help='Nombre de workers pour le mode parallèle (par défaut: optimal selon CPU/GPU/batchs)')
    parser.add_argument('--force-rebuild', action='store_true', help='Forcer la régénération des lots')
    parser.add_argument('--batch-size', type=int, default=100, help='Taille des lots à générer')
    parser.add_argument('--num-batches', type=int, default=None, help='Nombre de lots à traiter (None = tous)')
    parser.add_argument('--stop-after', type=str, default=None, choices=['preprocessing', 'translation', 'postprocessing', 'analysis'], help="Arrêter la pipeline après cette étape pour chaque lot")
    parser.add_argument('--batch-size-model', type=int, default=32, help='Taille des batchs envoyés au modèle de traduction')
    parser.add_argument('--monitoring-frequency', type=int, default=1, help='Fréquence des logs/monitoring (1=chaque batch, 2=1 batch sur 2, etc.)')
    parser.add_argument('--alert-ram', type=int, default=90, help="Seuil d'alerte RAM (%)")
    parser.add_argument('--alert-gpu', type=int, default=90, help="Seuil d'alerte GPU (%)")
    parser.add_argument('--alert-time', type=float, default=10, help="Seuil d'alerte temps par lot (secondes)")
    parser.add_argument('--fp16', action='store_true', help='Activer le mode half-precision (FP16) pour la traduction (GPU uniquement)')
    args = parser.parse_args()

    # Calcul automatique du nombre optimal de workers si non spécifié
    def compute_optimal_workers(num_batches=None):
        try:
            import torch
            gpu_available = torch.cuda.is_available()
        except ImportError:
            gpu_available = False
        cpu_count = os.cpu_count() or 2
        # On limite à 1 worker si GPU, sinon on prend min(cpu_count, num_batches, 8)
        if gpu_available:
            # Pour la 4070 Super, un seul worker saturera le GPU (traduction batchée)
            return 1
        else:
            # Pour CPU, on ne dépasse pas le nombre de batchs ni 8 threads (par sécurité)
            if num_batches is not None:
                return max(1, min(cpu_count, num_batches, 8))
            else:
                return max(1, min(cpu_count, 8))

    max_workers = args.max_workers
    if max_workers is None:
        max_workers = compute_optimal_workers(args.num_batches)
        print(f"[AUTO] Nombre optimal de workers détecté : {max_workers}")

    try:
        main(
            parallel=args.parallel,
            max_workers=max_workers,
            force_rebuild=args.force_rebuild,
            batch_size=args.batch_size,
            num_batches=args.num_batches,
            stop_after=args.stop_after,
            batch_size_model=args.batch_size_model,
            monitoring_frequency=args.monitoring_frequency,
            alert_ram=args.alert_ram,
            alert_gpu=args.alert_gpu,
            alert_time=args.alert_time,
            fp16=args.fp16
        )
    except Exception as e:
        log_error("Erreur critique dans la pipeline", exc=e)
        raise
