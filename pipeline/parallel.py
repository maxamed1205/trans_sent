"""
Module de parallélisation du traitement des lots.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.core import log_info

def process_batches_in_parallel(batch_paths, process_func, max_workers=2):
    """Traite plusieurs lots en parallèle avec la fonction donnée."""
    log_info(f"Traitement parallèle de {len(batch_paths)} lots avec {max_workers} workers.")
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_func, bp): bp for bp in batch_paths}
        for future in as_completed(future_to_batch):
            batch_path = future_to_batch[future]
            try:
                result = future.result()
                results.append((batch_path, result))
            except Exception as e:
                log_info(f"Erreur lors du traitement du lot {batch_path} : {e}")
                results.append((batch_path, False))
    return results
