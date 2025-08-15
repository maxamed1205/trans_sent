import unittest
import os
from utils.config_loader import load_config
from utils.core import check_dir_exists, check_file_exists

class TestConfigAndStructure(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		cls.config = load_config()

	def test_required_keys(self):
		required_keys = [
			'raw_data_dir', 'processed_data_dir', 'batches_dir', 'meta_dir', 'logs_dir', 'models_dir', 'models'
		]
		for key in required_keys:
			self.assertIn(key, self.config, f"Clé manquante dans la config : {key}")

	def test_paths_exist(self):
		# Vérifie que les dossiers principaux existent
		for key in ['raw_data_dir', 'processed_data_dir', 'batches_dir', 'meta_dir', 'logs_dir', 'models_dir']:
			path = self.config[key]
			self.assertTrue(os.path.isdir(path), f"Dossier manquant : {path}")

	def test_models_section(self):
		self.assertIsInstance(self.config['models'], list)
		self.assertGreater(len(self.config['models']), 0, "La section 'models' doit contenir au moins un modèle.")
		for model in self.config['models']:
			self.assertIn('name', model)
			self.assertIn('path', model)
			self.assertIn('type', model)

	def test_utils_import(self):
		# Vérifie que les utilitaires sont importables et fonctionnels
		check_dir_exists(self.config['logs_dir'])
		check_file_exists('config.yaml')

if __name__ == "__main__":
	unittest.main()
