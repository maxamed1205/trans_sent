
import os
import pandas as pd
from utils.config_loader import load_config, get_abs_path_from_config
from utils.core import ensure_dir_exists, log_info, log_error, log_execution_time

@log_execution_time('Génération lots EN')
def generate_batches(batch_size=100, force_rebuild=False, num_batches=None):
	# Détection de la portée de la négation (scope) : tout ce qui suit le premier mot de négation trouvé
	def extract_scope(phrase, neg_words):
		if not neg_words:
			return ""
		phrase_l = phrase.lower()
		for neg in neg_words:
			idx = phrase_l.find(neg)
			if idx != -1:
				# Prend tout ce qui suit le mot de négation (après l'espace suivant)
				after = phrase[idx+len(neg):].lstrip()
				return after if after else ""
		return ""
	"""
	Génère les lots à partir des données sources (txt/csv) dans data/raw/.
	Chaque lot contient uniquement : id_phrase, batch_id, en
	Ne crée les lots que si aucun n'existe, sauf si force_rebuild=True.
	"""
	config = load_config()
	data_dir = get_abs_path_from_config(config, 'raw_data_dir')
	batches_dir = get_abs_path_from_config(config, 'batches_dir')
	meta_dir = get_abs_path_from_config(config, 'meta_dir')
	ensure_dir_exists(batches_dir)
	ensure_dir_exists(meta_dir)

	# Vérifier s'il existe déjà des lots
	existing_batches = [f for f in os.listdir(batches_dir) if f.endswith('.parquet')]
	if existing_batches and not force_rebuild:
		log_info(f"Lots déjà présents dans {batches_dir}, génération ignorée.")
		return
	if force_rebuild:
		for f in existing_batches:
			os.remove(os.path.join(batches_dir, f))
		log_info("Lots existants supprimés (force_rebuild=True).")

	# Charger les phrases sources (on prend le premier fichier .txt trouvé)
	files = [f for f in os.listdir(data_dir) if f.endswith('.txt') or f.endswith('.csv')]
	if not files:
		log_error(f"Aucun fichier source trouvé dans {data_dir}")
		raise FileNotFoundError(f"Aucun fichier source trouvé dans {data_dir}")
	src_path = os.path.join(data_dir, files[0])
	if src_path.endswith('.txt'):
		# On génère id_phrase, line_number, source_file
		with open(src_path, encoding='utf-8') as f:
			lines = [line.strip() for line in f if line.strip()]
		df = pd.DataFrame({
			'id_phrase': range(1, len(lines)+1),
			'en': lines,
			'line_number': range(1, len(lines)+1),
			'source_file': os.path.basename(src_path)
		})
	elif src_path.endswith('.csv'):
		df = pd.read_csv(src_path)
		if 'en' not in df.columns:
			log_error("Le CSV source doit contenir une colonne 'en'.")
			raise ValueError("Le CSV source doit contenir une colonne 'en'.")
		# On conserve toutes les colonnes d'origine
	else:
		log_error("Format de fichier source non supporté.")
		raise ValueError("Format de fichier source non supporté.")

	# Génération des lots
	n = len(df)
	meta_records = []
	negation_keywords = [
		'not', "n't", 'no', 'never', 'none', 'neither', 'nor', 'cannot', 'without', 'denies', 'deny', 'denied', 'refute', 'refutes', 'refuted', 'absence', 'lacks', 'lack', 'negative', 'negatives', 'negation', 'exclude', 'excluded', 'excludes', 'excluding'
	]
	global_start = 1
	batch_ranges = list(range(0, n, batch_size))
	if num_batches is not None:
		batch_ranges = batch_ranges[:num_batches]
	for idx, start in enumerate(batch_ranges):
		end = min(start + batch_size, n)
		batch = df.iloc[start:end].copy()
		batch['start_idx'] = global_start
		batch['end_idx'] = global_start + len(batch) - 1
		batch['nb_words'] = batch['en'].apply(lambda x: len(str(x).split()))
		batch['nb_chars'] = batch['en'].apply(lambda x: len(str(x)))
		# Détection négation simple
		def detect_negation(phrase):
			words = str(phrase).lower().split()
			found = [w for w in negation_keywords if w in words]
			return found
		import numpy as np
		def to_pylist(val):
			if isinstance(val, list):
				return val
			if isinstance(val, np.ndarray):
				return val.tolist()
			return list(val)
		batch['negation_words'] = batch['en'].apply(lambda x: to_pylist(detect_negation(x)))
		batch['has_negation'] = batch['negation_words'].apply(lambda neg_list: len(neg_list) > 0)
		def extract_scope_indices(phrase, neg_words):
			if not neg_words:
				return (None, None, "")
			phrase_l = phrase.lower()
			for neg in neg_words:
				idx = phrase_l.find(neg)
				if idx != -1:
					start = idx + len(neg)
					after = phrase[start:].lstrip()
					nb_strip = len(phrase[start:]) - len(after)
					scope_start = start + nb_strip
					scope_end = len(phrase)
					return (scope_start, scope_end, after if after else "")
			return (None, None, "")
		scope_info = [extract_scope_indices(phrase, negs) for phrase, negs in zip(batch['en'], batch['negation_words'])]
		batch['negation_scope_start'] = [s[0] for s in scope_info]
		batch['negation_scope_end'] = [s[1] for s in scope_info]
		batch['negation_scope'] = [s[2] for s in scope_info]
		# Colonnes finales : id_phrase, en, line_number, has_negation, puis le reste
		keep_cols = [c for c in df.columns if c not in ('source_file', 'start_idx', 'end_idx')]
		extra_cols = ['has_negation', 'nb_words', 'nb_chars', 'negation_words', 'negation_scope', 'negation_scope_start', 'negation_scope_end']
		extra_cols = [c for c in extra_cols if c not in keep_cols]
		ordered_cols = []
		for col in ['id_phrase', 'en', 'line_number', 'has_negation']:
			if col in keep_cols:
				ordered_cols.append(col)
		ordered_cols += [c for c in extra_cols if c not in ordered_cols]
		ordered_cols += [c for c in keep_cols if c not in ordered_cols]
		batch = batch[ordered_cols]
		# Ajoute le nom du fichier source dans le nom du batch
		src_file = os.path.splitext(os.path.basename(src_path))[0]
		batch_filename = f"en_batch_{src_file}_{global_start:05d}_{global_start+len(batch)-1:05d}.parquet"
		batch_path = os.path.join(batches_dir, batch_filename)
		batch.to_parquet(batch_path, index=False)
		log_info(f"Lot généré : {batch_path} ({len(batch)} phrases)")
		meta_records.append({
			'batch_id': batch_filename.replace('.parquet',''),
			'batch_file': batch_filename,
			'date_creation': pd.Timestamp.now().isoformat(),
			'nb_phrases': len(batch),
			'start_idx': global_start,
			'end_idx': global_start+len(batch)-1,
			'status': 'en_attente',
			'commentaire': ''
		})
		global_start += len(batch)
	# Sauvegarde du meta
	meta_path = os.path.join(meta_dir, 'batch_info.parquet')
	meta_df = pd.DataFrame(meta_records)
	meta_df.to_parquet(meta_path, index=False)
	log_info(f"Meta batch_info.parquet généré : {meta_path}")
