# SunSéjour Datamart: Automated ETL Pipeline & Tourism Business Analytics

> **Cadre institutionnel** : Projet Industriel / Stage — SunSéjour  
> **Formation** : ENSIIE — Ingénierie Logicielle & Systèmes Décisionnels  
> **Auteur** : Mohamed Nourou Dine CISSE  
> **Domaine** : `Data Engineering, ETL & Business Intelligence`  
> **Stack Technique** : `Python` `Pandas` `SQL` `Docker` `Docker Compose` `Batch Scripting` `Excel Ingestion`

---

## 📌 Présentation Réelle du Projet (Honest Overview)

Pour l'agence de voyage et d'excursions **SunSéjour**, les données d'activité (réservations d'excursions, transferts de passagers, paiements) étaient historiquement gérées via des fichiers Excel hétérogènes générant des erreurs de saisie et des retards dans le calcul du Chiffre d'Affaires (CA).

Ce projet a consisté à concevoir et déployer un **pipeline ETL automatisé et conteneurisé** qui :
1. Valide la conformité des fichiers Excel sources (contrôle de formats, détection des valeurs manquantes et doublons).
2. Transforme et normalise les données selon les règles métier de l'agence.
3. Alimente un Datamart relationnel pour le reporting financier et opérationnel.
4. Fournit un guide d'utilisation opérationnel et un script d'exécution en un clic pour les équipes métier sous Windows.

---

## 🏗️ Structure du Répertoire

```text
03_SunSejour_ETL_Datamart/
├── README.md                      # Documentation technique
├── data_samples/                  # Échantillons de données anonymisées pour test
│   ├── excursion_data/
│   │   └── Excursions.xls         # Données brutes d'excursions
│   └── transfert_data/
│       └── 2024-01.xlsx           # Données brutes de transferts mensuels
├── reports/
│   ├── Rapport_Datamart_SunSejour.pdf     # Rapport technique d'architecture décisionnelle
│   └── Guide_Utilisation_ETL_SunSejour.pdf # Guide pas-à-pas pour les utilisateurs métier
└── src/
    ├── config.py                  # Paramètres et chaînes de connexion
    ├── validate.py                # Règles de validation et contrôle qualité des données
    ├── transform.py               # Moteur de transformation et nettoyage
    ├── load.py                    # Chargement SQL dans le datamart
    ├── pipeline.py                # Script d'orchestration global
    ├── CA_script.py               # Calcul automatisé du Chiffre d'Affaires et des marges
    ├── requirements.txt           # Dépendances Python
    ├── Dockerfile                 # Conteneurisation du job ETL
    ├── docker-compose.yml         # Déploiement multi-services
    ├── entrypoint.sh              # Point d'entrée conteneur Linux
    └── lancer_pipeline.bat        # Lanceur double-clic pour postes Windows de l'agence
```

---

## ⚙️ Détail des Étapes ETL

1. **Validation (`validate.py`)** :
   - Vérification des colonnes requises, typage des dates, plages de valeurs monétaires valides.
   - Génération d'un rapport de rejets traçable en cas d'incohérence.
2. **Transformation (`transform.py`)** :
   - Réconciliation des codes prestataires, standardisation des intitulés d'excursions et calcul des commissions nettes.
3. **Chargement & Reporting (`load.py`, `CA_script.py`)** :
   - Insertion transactionnelle dans les tables de faits et dimensions du Datamart.
   - Agrégation mensuelle et trimestrielle du Chiffre d'Affaires par catégorie de service.

---

## 🚀 Guide d'Exécution

### Option 1 : Exécution locale avec Python
```bash
cd src
pip install -r requirements.txt
python pipeline.py
```

### Option 2 : Exécution via Docker Compose
```bash
cd src
docker compose up --build
```

### Option 3 : Poste utilisateur Windows
Double-cliquer sur `src/lancer_pipeline.bat` pour déclencher l'ingestion sans passer par un terminal.

---

## 🔍 Preuves & Documents Inclus
- 📄 **Rapport Datamart** : [`reports/Rapport_Datamart_SunSejour.pdf`](reports/Rapport_Datamart_SunSejour.pdf)
- 📄 **Guide Utilisateur** : [`reports/Guide_Utilisation_ETL_SunSejour.pdf`](reports/Guide_Utilisation_ETL_SunSejour.pdf)

---

## 📬 Profil de l'Auteur
- **Mohamed Nourou Dine CISSE**
- **ENSIIE** & **Université Paris-Saclay** — Double diplôme, Master 2 Intelligence Artificielle (M2IA)
- Échange académique : **McMaster University** (Canada)
- **GitHub** : [@nourou-dine-cisse](https://github.com/nourou-dine-cisse)
