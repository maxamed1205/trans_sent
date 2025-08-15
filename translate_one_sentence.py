import os
import time
from transformers import MarianMTModel, MarianTokenizer
from utils.config_loader import load_config
from utils.core import (
    ensure_dir_exists,
    check_file_exists,
    check_dir_exists,
    log_error
)

def get_unique_filename(base_name, n_sent, out_dir):
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    return os.path.join(out_dir, f"{base_name}_translated_{n_sent}_{timestamp}.txt")

if __name__ == "__main__":
    # Charger la configuration centralisée
    config = load_config()
    input_path = config["data"]["negation_medical"]
    out_dir = config["output"]["translation_dir"]
    model_snapshots_dir = config["model"]["snapshots_dir"]

    try:
        ensure_dir_exists(out_dir)
        check_file_exists(input_path, "Fichier d'entrée introuvable")
        check_dir_exists(model_snapshots_dir, "Dossier de snapshots du modèle introuvable")
    except Exception as e:
        log_error(str(e))
        raise

    # Recherche du snapshot valide
    valid_snapshots = []
    for d in os.listdir(model_snapshots_dir):
        subdir = os.path.join(model_snapshots_dir, d)
        if os.path.isdir(subdir):
            files = os.listdir(subdir)
            has_config = "config.json" in files
            has_model = any(f in files for f in ["pytorch_model.bin", "model.safetensors"])
            if has_config and has_model:
                valid_snapshots.append(subdir)
    if not valid_snapshots:
        log_error(f"Aucun snapshot complet n’a été trouvé dans : {model_snapshots_dir}")
        raise FileNotFoundError(f"Aucun snapshot complet n’a été trouvé dans : {model_snapshots_dir}")

    # Charger la première phrase
    with open(input_path, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        log_error("Le fichier d'entrée est vide.")
        raise ValueError("Le fichier d'entrée est vide.")
    first_sentence = lines[0]

    # Sélectionner le bon snapshot du modèle local
    model_dir = valid_snapshots[0]

    tokenizer = MarianTokenizer.from_pretrained(model_dir)
    model = MarianMTModel.from_pretrained(model_dir)

    # Traduire
    inputs = tokenizer([first_sentence], return_tensors="pt", padding=True)
    translated = model.generate(**inputs)
    fr_text = tokenizer.batch_decode(translated, skip_special_tokens=True)[0]

    # Générer un nom de fichier unique
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    out_path = get_unique_filename(base_name, 1, out_dir)

    # Sauvegarder le résultat
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"EN: {first_sentence}\nFR: {fr_text}\n")
    print(f"Traduction sauvegardée dans : {out_path}")
