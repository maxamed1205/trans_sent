"""
compare_fp16_fp32.py : Compare la traduction d'un batch en FP32 et FP16.
Affiche et exporte les différences (si présentes).
"""
import os
import pandas as pd
from transformers import MarianMTModel, MarianTokenizer
import torch
from utils.config_loader import load_config, get_abs_path_from_config
from utils.core import log_info

def translate_sentences(sentences, model_path, device, fp16=False):
    tokenizer = MarianTokenizer.from_pretrained(model_path)
    model = MarianMTModel.from_pretrained(model_path)
    if fp16 and torch.cuda.is_available():
        model = model.half()
    model.to(device)
    inputs = tokenizer(sentences, return_tensors="pt", padding=True, truncation=True)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        translated = model.generate(**inputs)
    return tokenizer.batch_decode(translated, skip_special_tokens=True)

def compare_batch(batch_name, batch_dir=None):
    config = load_config()
    if batch_dir is None:
        batch_dir = get_abs_path_from_config(config, 'batches_dir')
    batch_path = os.path.join(batch_dir, batch_name)
    df = pd.read_parquet(batch_path)
    sentences = df['en'].astype(str).tolist()
    model_info = config['models'][0]
    model_dir = get_abs_path_from_config(config, 'models_dir')
    model_path = os.path.join(model_dir, model_info['path'])
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    log_info(f"Comparaison FP32 vs FP16 sur {len(sentences)} phrases, modèle {model_info['name']}")
    fr_fp32 = translate_sentences(sentences, model_path, device, fp16=False)
    fr_fp16 = translate_sentences(sentences, model_path, device, fp16=True)
    # Analyse des différences
    diffs = []
    for i, (s, t32, t16) in enumerate(zip(sentences, fr_fp32, fr_fp16)):
        if t32 != t16:
            diffs.append({'idx': i, 'en': s, 'fr_fp32': t32, 'fr_fp16': t16})
    if diffs:
        diff_df = pd.DataFrame(diffs)
        out_path = os.path.join(batch_dir, f'diff_fp32_fp16_{batch_name}.csv')
        diff_df.to_csv(out_path, index=False)
        print(f"Différences trouvées : {len(diffs)}. Export : {out_path}")
    else:
        print("Aucune différence entre FP32 et FP16 sur ce batch.")
    return diffs

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Comparer la traduction FP32 vs FP16 sur un batch.")
    parser.add_argument('batch_name', type=str, help='Nom du batch (ex: en_batch_0001.parquet)')
    parser.add_argument('--batch-dir', type=str, default=None, help='Répertoire des batches (optionnel)')
    args = parser.parse_args()
    compare_batch(args.batch_name, args.batch_dir)
