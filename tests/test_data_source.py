
from utils.config_loader import load_config
from utils.core import check_data_source_exists

def test_data_source():
    config = load_config()
    files = check_data_source_exists(config)
    print(f"Fichiers sources trouvés : {files}")

if __name__ == "__main__":
    test_data_source()
    print("Test de présence des données source : OK")
