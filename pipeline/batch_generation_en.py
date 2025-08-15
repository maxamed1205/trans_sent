
import os
import pandas as pd
from utils.config_loader import load_config, get_abs_path_from_config
from utils.core import ensure_dir_exists, log_info, log_error

def generate_batches(batch_size=100, force_rebuild=False):
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
		with open(src_path, encoding='utf-8') as f:
			lines = [line.strip() for line in f if line.strip()]
		df = pd.DataFrame({'en': lines})
	elif src_path.endswith('.csv'):
		df = pd.read_csv(src_path)
		if 'en' not in df.columns:
			log_error("Le CSV source doit contenir une colonne 'en'.")
			raise ValueError("Le CSV source doit contenir une colonne 'en'.")
		df = df[['en']]
	else:
		log_error("Format de fichier source non supporté.")
		raise ValueError("Format de fichier source non supporté.")

	# Génération des lots
	n = len(df)
	batches = [df.iloc[i:i+batch_size] for i in range(0, n, batch_size)]
	meta_records = []
	# Liste simple de mots de négation (à adapter selon besoin)
	negation_keywords = [
		'not', "n't", 'no', 'never', 'none', 'neither', 'nor', 'cannot', 'without', 'denies', 'deny', 'denied', 'refute', 'refutes', 'refuted', 'absence', 'lacks', 'lack', 'negative', 'negatives', 'negation', 'exclude', 'excluded', 'excludes', 'excluding'
	]
	for idx, batch in enumerate(batches, 1):
		batch_id = f"batch_{idx:04d}"
		batch_filename = f"en_{batch_id}.parquet"
		batch = batch.copy()
		batch['id_phrase'] = range(1, len(batch)+1)
		batch['line_number'] = batch['id_phrase'] # correspond à la ligne dans le lot (1-based)
		batch['nb_words'] = batch['en'].apply(lambda x: len(str(x).split()))
		batch['nb_chars'] = batch['en'].apply(lambda x: len(str(x)))
		# Détection négation simple
		def detect_negation(phrase):
			words = str(phrase).lower().split()
			found = [w for w in negation_keywords if w in words]
			return found
		import numpy as np
		def to_pylist(val):
			# Si c'est déjà une liste, retourne-la
			if isinstance(val, list):
				return val
			# Si c'est un numpy array, convertit en liste
			if isinstance(val, np.ndarray):
				return val.tolist()
			# Sinon, force la conversion
			return list(val)
		batch['negation_words'] = batch['en'].apply(lambda x: to_pylist(detect_negation(x)))
		batch['has_negation'] = batch['negation_words'].apply(lambda neg_list: len(neg_list) > 0)
		batch['source_file'] = os.path.basename(src_path)
		# Ajout des indices de début et fin de la portée de négation
		def extract_scope_indices(phrase, neg_words):
			if not neg_words:
				return (None, None, "")
			phrase_l = phrase.lower()
			for neg in neg_words:
				idx = phrase_l.find(neg)
				if idx != -1:
					start = idx + len(neg)
					after = phrase[start:].lstrip()
					# Calcul du vrai start après suppression des espaces
					nb_strip = len(phrase[start:]) - len(after)
					scope_start = start + nb_strip
					scope_end = len(phrase)
					return (scope_start, scope_end, after if after else "")
			return (None, None, "")
		scope_info = [extract_scope_indices(phrase, negs) for phrase, negs in zip(batch['en'], batch['negation_words'])]
		batch['negation_scope_start'] = [s[0] for s in scope_info]
		batch['negation_scope_end'] = [s[1] for s in scope_info]
		batch['negation_scope'] = [s[2] for s in scope_info]
		# Réordonner les colonnes
		batch = batch[['id_phrase', 'en', 'nb_words', 'nb_chars', 'has_negation', 'negation_words', 'negation_scope', 'negation_scope_start', 'negation_scope_end', 'source_file', 'line_number']]
		batch_path = os.path.join(batches_dir, batch_filename)
		batch.to_parquet(batch_path, index=False)
		log_info(f"Lot généré : {batch_path} ({len(batch)} phrases)")
		meta_records.append({
			'batch_id': batch_id,
			'source': os.path.basename(src_path),
			'date_creation': pd.Timestamp.now().isoformat(),
			'nb_phrases': len(batch),
			'parametres': f"batch_size={batch_size}",
			'status': 'en_attente',
			'commentaire': ''
		})
	# Sauvegarde du meta
	meta_path = os.path.join(meta_dir, 'batch_info.parquet')
	meta_df = pd.DataFrame(meta_records)
	meta_df.to_parquet(meta_path, index=False)
	log_info(f"Meta batch_info.parquet généré : {meta_path}")
