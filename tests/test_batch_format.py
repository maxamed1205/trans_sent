
import os
import pandas as pd
import numpy as np
from utils.config_loader import load_config, get_abs_path_from_config

def test_batch_format():
    """
    Vérifie que chaque lot dans batches_dir respecte le format attendu :
    - Colonnes : id_phrase (int), batch_id (str), en (str)
    - id_phrase commence à 1 et est consécutif
    - batch_id est identique pour toutes les lignes du lot
    - Pas de colonne superflue
    """
    config = load_config()
    batches_dir = get_abs_path_from_config(config, 'batches_dir')
    batch_files = [f for f in os.listdir(batches_dir) if f.endswith('.parquet')]
    assert batch_files, f"Aucun lot trouvé dans {batches_dir}"
    expected_cols = ['id_phrase', 'en', 'nb_words', 'nb_chars', 'has_negation', 'negation_words', 'negation_scope', 'negation_scope_start', 'negation_scope_end', 'source_file', 'line_number']
    # DEBUG: Vérifier qu'aucune vérification sur df n'est hors de la boucle
    try:
        print('[DEBUG] df existe hors boucle ?', 'df' in locals())
        print('[DEBUG] batch_files:', batch_files)
    except Exception as e:
        print('[DEBUG] Exception lors du print debug hors boucle:', e)
    for batch_file in batch_files:
        path = os.path.join(batches_dir, batch_file)
        df = pd.read_parquet(path)
        # Vérification des colonnes de portée de négation (indices)
        assert df['negation_scope_start'].apply(lambda x: (x is None) or isinstance(x, int)).all(), f"Colonne 'negation_scope_start' doit être int ou None dans {batch_file}"
        assert df['negation_scope_end'].apply(lambda x: (x is None) or isinstance(x, int)).all(), f"Colonne 'negation_scope_end' doit être int ou None dans {batch_file}"
    assert df['negation_scope_start'].apply(lambda x: (x is None) or isinstance(x, int)).all(), f"Colonne 'negation_scope_start' doit être int ou None dans {batch_file}"
    assert df['negation_scope_end'].apply(lambda x: (x is None) or isinstance(x, int)).all(), f"Colonne 'negation_scope_end' doit être int ou None dans {batch_file}"
    for batch_file in batch_files:
        path = os.path.join(batches_dir, batch_file)
        df = pd.read_parquet(path)
        # Colonnes attendues
        assert list(df.columns) == expected_cols, f"Colonnes inattendues dans {batch_file}: {df.columns}"
        # id_phrase
        assert df['id_phrase'].iloc[0] == 1, f"id_phrase doit commencer à 1 dans {batch_file}"
        assert (df['id_phrase'] == range(1, len(df)+1)).all(), f"id_phrase non consécutif dans {batch_file}"
        # en
        assert df['en'].apply(lambda x: isinstance(x, str)).all(), f"Colonne 'en' doit être str dans {batch_file}"
        # nb_words
        assert df['nb_words'].apply(lambda x: isinstance(x, int)).all(), f"Colonne 'nb_words' doit être int dans {batch_file}"
        # nb_chars
        assert df['nb_chars'].apply(lambda x: isinstance(x, int)).all(), f"Colonne 'nb_chars' doit être int dans {batch_file}"
        # has_negation
        assert df['has_negation'].apply(lambda x: isinstance(x, (bool, np.bool_))).all(), f"Colonne 'has_negation' doit être bool dans {batch_file}"
        # negation_words (robuste à la désérialisation Parquet)
        import ast
        def is_list_or_liststr(x):
            import numpy as np
            if isinstance(x, list):
                return True
            if isinstance(x, np.ndarray):
                return True
            if isinstance(x, str):
                try:
                    val = ast.literal_eval(x)
                    return isinstance(val, list)
                except Exception:
                    return False
            return False
        assert df['negation_words'].apply(is_list_or_liststr).all(), f"Colonne 'negation_words' doit être list ou list-string dans {batch_file}"
        # negation_scope
        if not df['negation_scope'].apply(lambda x: isinstance(x, str)).all():
            print(f"[DEBUG] negation_scope types dans {batch_file}: {df['negation_scope'].apply(type).value_counts()}")
            print(f"[DEBUG] negation_scope exemples: {df['negation_scope'].head(3).tolist()}")
        assert df['negation_scope'].apply(lambda x: isinstance(x, str)).all(), f"Colonne 'negation_scope' doit être str dans {batch_file}"
        # source_file
        assert df['source_file'].apply(lambda x: isinstance(x, str)).all(), f"Colonne 'source_file' doit être str dans {batch_file}"
        # line_number
        assert (df['line_number'] == range(1, len(df)+1)).all(), f"line_number non consécutif dans {batch_file}"
    print(f"Tous les lots ({len(batch_files)}) sont valides.")

if __name__ == "__main__":
    test_batch_format()
