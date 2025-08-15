"""
Test de synchronisation entre la config centralisée et la structure réelle du projet.
Ce script vérifie que tous les dossiers/fichiers déclarés dans la config existent bien sur le disque.
Il doit être lancé après toute modification de la config ou de la structure.
"""

import os
from utils.config_loader import load_config
from utils.core import log_info, log_error, ensure_dir_exists

def check_path_exists(path, is_dir=True):
    if is_dir:
        if not os.path.isdir(path):
            log_error(f"Dossier manquant : {path}")
            return False
    else:
        if not os.path.isfile(path):
            log_error(f"Fichier manquant : {path}")
            return False
    return True

def validate_config_structure():
    config = load_config()
    all_ok = True

    # Vérifie les dossiers principaux
    if "project_structure" in config and "folders" in config["project_structure"]:
        for folder in config["project_structure"]["folders"]:
            if not check_path_exists(folder, is_dir=True):
                all_ok = False

    # Vérifie les dossiers/fichiers de sortie
    if "output" in config:
        for key, val in config["output"].items():
            if isinstance(val, str):
                # Heuristique : si le nom contient 'dir', on attend un dossier
                if "dir" in key:
                    if not check_path_exists(val, is_dir=True):
                        all_ok = False
                else:
                    # sinon fichier
                    if not check_path_exists(val, is_dir=False):
                        all_ok = False
    # Vérifie les chemins de données et modèles
    for section in ("data", "model"):
        if section in config:
            for key, val in config[section].items():
                if isinstance(val, str):
                    # Heuristique : si le nom contient 'dir', on attend un dossier
                    if "dir" in key:
                        if not check_path_exists(val, is_dir=True):
                            all_ok = False
                    else:
                        if not check_path_exists(val, is_dir=False):
                            all_ok = False
    if all_ok:
        log_info("La structure du projet est synchronisée avec la config.")
    else:
        log_error("Des incohérences ont été détectées entre la config et la structure réelle.")
    return all_ok

if __name__ == "__main__":
    validate_config_structure()
