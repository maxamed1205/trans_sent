"""
Module de traduction pour la pipeline de traduction.
Utilise la configuration centralisée.
"""
import os
import pandas as pd
from transformers import MarianMTModel, MarianTokenizer
from utils.core import log_info, log_error, ensure_dir_exists, log_execution_time
from utils.config_loader import load_config, get_abs_path_from_config
import psutil
import traceback
import time
try:
    import torch
except ImportError:
    torch = None

config = load_config()

@log_execution_time('Traduction')
def translate_batch(batch_name, batch_size_model=32, monitoring_frequency=1, alert_ram=90, alert_gpu=90, alert_time=10, fp16=False):
    monitoring_stats = {
        'ram_used_mb': [],
        'ram_total_mb': [],
        'gpu_used_mb': [],
        'gpu_total_mb': [],
        'batch_times': [],
        'num_batches': 0
    }

    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    batch_path = os.path.join(batches_dir, batch_name)
    models = config['models']
    model_info = models[0]  # Utilise le premier modèle de la liste
    model_dir = get_abs_path_from_config(config, 'models_dir')
    model_path = os.path.join(model_dir, model_info['path'])
    from utils.core import get_best_device
    device_cfg = config.get('device', 'cpu')
    device = get_best_device(device_cfg)
    log_info(f"Traduction du lot : {batch_path} avec le modèle {model_info['name']}")

    # Charger le batch
    try:
        df = pd.read_parquet(batch_path)
    except Exception as e:
        log_error(f"Erreur lors du chargement du batch : {batch_path}", exc=e)
        raise

    if 'en' not in df.columns:
        log_error(f"Colonne 'en' absente dans le batch : {batch_path}")
        raise ValueError(f"Colonne 'en' absente dans le batch : {batch_path}")

    # Vérifier la présence des fichiers essentiels du modèle
    import glob
    # Recherche récursive du modèle (pytorch_model.bin ou model.safetensors)
    def find_model_file(path):
        for ext in ("pytorch_model.bin", "model.safetensors"):
            found = [f for f in glob.glob(os.path.join(path, "**", ext), recursive=True)]
            if found:
                return found[0]
        return None

    required_files = [
        'config.json',
        'tokenizer_config.json',
        'vocab.json',
        'source.spm',
        'target.spm'
    ]
    missing = [f for f in required_files if not os.path.isfile(os.path.join(model_path, f))]
    model_file = find_model_file(model_path)
    if missing or not model_file:
        missing_files = missing.copy()
        if not model_file:
            missing_files.append('pytorch_model.bin/model.safetensors')
        log_error(f"Fichiers manquants dans le dossier du modèle ({model_path}) : {', '.join(missing_files)}")
        # Crée le dossier modèle si besoin
        from utils.core import ensure_dir_exists
        ensure_dir_exists(model_path)
        # Si le dossier est vide, tente de télécharger, sinon lève une exception
        if not os.listdir(model_path):
            try:
                tokenizer = MarianTokenizer.from_pretrained(model_info['name'], cache_dir=model_path)
                model = MarianMTModel.from_pretrained(model_info['name'], cache_dir=model_path)
                model.save_pretrained(model_path)
                tokenizer.save_pretrained(model_path)
                log_info(f"Modèle téléchargé et sauvegardé dans {model_path}")
            except Exception as e:
                log_error(f"Erreur lors du téléchargement du modèle MarianMT : {model_info['name']}", exc=e)
                raise FileNotFoundError(f"Impossible de télécharger le modèle {model_info['name']} : {e}")
        else:
            raise FileNotFoundError(f"Fichiers manquants dans le dossier du modèle local ({model_path}) : {', '.join(missing_files)}. Aucun téléchargement car le dossier n'est pas vide.")
    # Charger le modèle MarianMT
    try:
        tokenizer = MarianTokenizer.from_pretrained(model_path)
        model = MarianMTModel.from_pretrained(model_path)
        if fp16 and torch and torch.cuda.is_available():
            model = model.half()
            log_info("Modèle chargé en mode FP16 (half-precision)")
        model.to(device)
    except Exception as e:
        log_error(f"Erreur lors du chargement du modèle MarianMT : {model_path}", exc=e)
        raise

    # Traduction par batchs (tout le lot ou sous-batchs)
    # batch_size_model est maintenant passé en argument (CLI/config)
    en_sentences = df['en'].tolist()
    translations = []
    log_info(f"Traduction par batchs de taille {batch_size_model} (total: {len(en_sentences)} phrases)")

    for i in range(0, len(en_sentences), batch_size_model):
        batch_sents = en_sentences[i:i+batch_size_model]
        t0 = time.time()
        try:
            inputs = tokenizer(batch_sents, return_tensors="pt", padding=True, truncation=True)
            inputs = {k: v.to(device) for k, v in inputs.items()}
            translated = model.generate(**inputs)
            fr_texts = tokenizer.batch_decode(translated, skip_special_tokens=True)
            translations.extend(fr_texts)
            if (monitoring_stats['num_batches'] % monitoring_frequency) == 0:
                log_info(f"Batch traduit : {len(batch_sents)} phrases.")
        except RuntimeError as e:
            if 'out of memory' in str(e).lower():
                log_error(f"OOM GPU/CPU sur le batch {i//batch_size_model+1}", exc=e)
            else:
                log_error(f"Erreur critique sur le batch {i//batch_size_model+1}", exc=e)
            log_error(traceback.format_exc())
            translations.extend([""] * len(batch_sents))
        except Exception as e:
            log_error(f"Erreur de traduction pour le batch {i//batch_size_model+1}", exc=e)
            log_error(traceback.format_exc())
            translations.extend([""] * len(batch_sents))
        t1 = time.time()
        batch_time = t1 - t0
        monitoring_stats['batch_times'].append(batch_time)
        monitoring_stats['num_batches'] += 1
        # Monitoring/logs moins fréquents
        if (monitoring_stats['num_batches'] % monitoring_frequency) == 0:
            ram = psutil.virtual_memory()
            ram_used = ram.used // 1024 // 1024
            ram_total = ram.total // 1024 // 1024
            ram_pct = 100 * ram_used / ram_total
            monitoring_stats['ram_used_mb'].append(ram_used)
            monitoring_stats['ram_total_mb'].append(ram_total)
            if ram_pct > alert_ram:
                log_error(f"ALERTE: RAM utilisée {ram_pct:.1f}% > seuil {alert_ram}%")
            if torch and torch.cuda.is_available():
                try:
                    gpu_mem = torch.cuda.memory_allocated() // 1024 // 1024
                    gpu_total = torch.cuda.get_device_properties(0).total_memory // 1024 // 1024
                    gpu_pct = 100 * gpu_mem / gpu_total
                    monitoring_stats['gpu_used_mb'].append(gpu_mem)
                    monitoring_stats['gpu_total_mb'].append(gpu_total)
                    log_info(f"GPU: {gpu_mem}MB / {gpu_total}MB")
                    if gpu_pct > alert_gpu:
                        log_error(f"ALERTE: GPU utilisé {gpu_pct:.1f}% > seuil {alert_gpu}%")
                except Exception as e:
                    log_error("Erreur monitoring GPU", exc=e)
            log_info(f"RAM: {ram_used}MB / {ram_total}MB")
            log_info(f"Lots traités: {monitoring_stats['num_batches']} / {((len(en_sentences)-1)//batch_size_model)+1}")
            if len(monitoring_stats['batch_times']) >= 2:
                avg_time = sum(monitoring_stats['batch_times'])/len(monitoring_stats['batch_times'])
                log_info(f"Temps moyen par lot: {avg_time:.2f}s")
            if batch_time > alert_time:
                log_error(f"ALERTE: Temps par lot {batch_time:.2f}s > seuil {alert_time}s")

    # Vérifier que l'ordre est conservé et la taille correcte
    assert len(translations) == len(df), "Le nombre de traductions ne correspond pas au nombre de phrases."
    df['fr'] = translations

    # Mini-rapport monitoring à la fin
    try:
        log_info("--- Rapport monitoring ---")
        log_info(f"RAM max utilisée: {max(monitoring_stats['ram_used_mb'])}MB / {monitoring_stats['ram_total_mb'][0]}MB")
        if monitoring_stats['gpu_used_mb']:
            log_info(f"GPU max utilisé: {max(monitoring_stats['gpu_used_mb'])}MB / {monitoring_stats['gpu_total_mb'][0]}MB")
        log_info(f"Temps moyen par lot: {sum(monitoring_stats['batch_times'])/len(monitoring_stats['batch_times']):.2f}s")
        log_info(f"Nombre de lots: {monitoring_stats['num_batches']}")
    except Exception as e:
        log_error("Erreur lors du rapport monitoring", exc=e)

    # Générer le batch fr et le meta associé
    from pipeline.batch_generation_fr import generate_fr_batch
    en_batch_path = batch_path
    fr_sentences = df['fr'].tolist()
    generate_fr_batch(en_batch_path, fr_sentences)
