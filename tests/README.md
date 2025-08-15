
# Dossier `tests/`

Ce dossier contient les tests automatiques pour valider la configuration, la structure et le bon fonctionnement des utilitaires de la pipeline.

## Fichiers présents
- `test_config_and_structure.py` :
	- Vérifie la présence des clés dans la config
	- Vérifie l'existence des dossiers principaux
	- Vérifie la validité de la section modèles
	- Vérifie le bon chargement des utilitaires

## Exécution des tests

Depuis la racine du projet ou le dossier `trans_sent/` :

```bash
python -m unittest tests/test_config_and_structure.py
```

Tous les nouveaux tests liés à la robustesse de la pipeline doivent être ajoutés ici.
