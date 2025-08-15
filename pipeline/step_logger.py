"""
Module de logging structuré par étape et par lot.
"""
import logging
import os
from utils.core import ensure_dir_exists

def get_step_logger(logs_dir, step_name, batch_id=None):
    """Crée un logger structuré pour une étape et un lot donné."""
    ensure_dir_exists(logs_dir)
    log_file = f"{step_name}"
    if batch_id:
        log_file += f"_batch_{batch_id}"
    log_file += ".log"
    log_path = os.path.join(logs_dir, log_file)
    logger = logging.getLogger(f"{step_name}_{batch_id}")
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
