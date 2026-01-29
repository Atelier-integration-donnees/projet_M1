# Repo projet_M1 : Intégration des données

**Formateur** : Benoît Gerald

**Membres du groupe** : 

* Anas AMAZOUZ
* Abdelmounaim EL HOUZI
* Dunvael LE ROUX

*Lien Edensia pour joindre livrables* : <https://flow.edensia.com/login>

---

## Objectifs du projet

Ce projet a pour objectif de mettre en place une chaîne complète d’intégration de données afin de construire un datamart analytique à partir des données publiques OpenFoodFacts.

L’ETL est implémenté à l’aide d’Apache Spark (PySpark) pour le traitement distribué des données, tandis que MySQL est utilisé comme base relationnelle cible pour l’analyse décisionnelle.

Le pipeline suit une architecture Bronze / Silver / Gold, permettant de séparer clairement :

* l’ingestion des données brutes,
* le nettoyage et la normalisation,
* la modélisation analytique et le chargement final.

---

## Architecture générale

```
OpenFoodFacts (CSV)
        ↓
      Bronze
        ↓
      Silver
        ↓
      Gold
        ↓
   Datamart MySQL
        ↓
 SQL Analytics / Dashboard
```

Chaque couche correspond à une étape fonctionnelle du pipeline et produit des jeux de données ainsi que des métriques de qualité associées.

---

### Prérequis

**Outils**

* Apache Spark (PySpark)
* Python
* MySQL 8
* Docker Desktop
* GitHub
* IDE pour cloner le dépôt et exécuter les scripts

**Sources de données**

* Export OpenFoodFacts (bulk CSV) : <https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz>
* Page officielle OpenFoodFacts : <https://fr.openfoodfacts.org/data>

**Dépendances Python**

Les dépendances sont listées dans le fichier *requirements.txt* et peuvent être installées automatiquement.

---

### Structure du dépôt

```
projet_M1/
│
├── conf/
│   └── settings.py
│       # Paramètres globaux du projet (DB, chemins, constantes)
│
├── docs/
│   ├── 1_Bronze.md
│   │   # Documentation fonctionnelle de la couche Bronze
│   │
│   ├── 2_Silver.md
│   │   # Documentation fonctionnelle de la couche Silver
│   │
│   ├── data-dictionary.md
│   │   # Dictionnaire de données du datamart (tables, colonnes, types, règles)
│   │
│   └── schema-etoile.md
│       # Schéma du modèle décisionnel en étoile (fact & dimensions)
│
├── etl/
│   ├── 1_Bronze.py
│   │   # Ingestion des données OpenFoodFacts (couche Bronze)
│   │
│   ├── 2_silver.py
│   │   # Nettoyage, normalisation et déduplication (couche Silver)
│   │
│   ├── 3_gold.py
│   │   # Modélisation, création du datamart et chargement MySQL (couche Gold)
│   │
│   ├── check_silver_schema.py
│   │   # Vérification de la conformité du schéma Silver
│   │
│   └── clean_db.py
│       # Nettoyage / réinitialisation des tables MySQL
│
├── jars/
│   └── mysql-connector-j-8.2.0.jar
│       # Driver JDBC MySQL utilisé par Spark
│
├── logs/
│   ├── metrics_silver.json
│   │   # Métriques de qualité calculées après l’étape Silver
│   │
│   ├── gold_metrics.json
│   │   # Métriques de qualité finales après l’étape Gold
│
├── sql/
│   ├── ddl.sql
│   │   # Création du schéma du datamart (dimensions, faits, index)
│   │
│   └── queries_analysis.sql
│       # Requêtes analytiques SQL
│
├── tests/
│   ├── 1_test_bronze.py
│   ├── 2_test_silver.py
│   └── 3_test_gold.py
│       # Tests unitaires des différentes couches
│
├── dashboard.py
│   # Script de visualisation connecté au datamart MySQL
│
├── docker-compose.yml
│   # Déploiement de l’environnement MySQL via Docker
│
├── requirements.txt
│   # Dépendances Python du projet
│
├── reset_db.py
│   # Réinitialisation complète de la base MySQL
│
├── README.md
│   # Documentation principale du projet
│
└── Projet_Atelier_Intégration_des_Données_25-26.pdf
    # Sujet et consignes du projet
```

---

### Choix techniques

* Apache Spark (PySpark) pour le traitement distribué de grands volumes de données
* Architecture Bronze / Silver / Gold pour structurer les étapes de transformation
* MySQL comme datamart relationnel pour l’analyse SQL
* Docker pour assurer la reproductibilité de l’environnement
* JDBC pour le chargement des données Spark vers MySQL

---

### Architecture & contraintes fonctionnelles

* Bronze : ingestion des données CSV OpenFoodFacts et extraction des champs principaux
* Silver : normalisation des types et unités, flatten des structures, déduplication par code-barres et gestion des langues (priorité fr > en > fallback)
* Gold : modélisation en étoile (dimensions + table(s) de faits) et chargement dans MySQL
* Qualité : calcul de métriques de complétude, unicité et cohérence, avec génération de rapports JSON

Les métriques de qualité permettent d’évaluer l’impact des transformations entre les différentes couches et d’identifier les anomalies potentielles dans les données nutritionnelles.

---

### Organisation des fichiers

**/conf**
Contient les fichiers de configuration globaux.
* **settings.py** : paramètres MySQL, chemins d’entrée/sortie, constantes partagées

**/etl**
Scripts ETL Spark correspondant aux différentes couches.
* **1_Bronze.py** : ingestion des données OpenFoodFacts
* **2_silver.py** : nettoyage, normalisation et calcul des métriques Silver
* **check_silver_schema.py** : validation du schéma Silver
* **3_gold.py** : modélisation analytique et chargement MySQL
* **clean_db.py** : nettoyage des tables MySQL

**/sql**
Scripts SQL exécutés sur le datamart.
* **ddl.sql** : création du schéma relationnel
* **queries_analysis.sql** : requêtes analytiques finales

**/logs**
Sorties de qualité produites par le pipeline (Silver et Gold).
* **metrics_silver.json**
  Rapport de qualité après l’étape Silver :
  * complétude
  * unicité
  * cohérence
* **gold_metrics.json**
  Rapport de qualité final après chargement Gold / MySQL

**/docs**
Documentation fonctionnelle et décisionnelle du projet.
* **1_Bronze.md** : description de l’étape Bronze (ingestion, choix techniques, règles de qualité)
* **2_Silver.md** : description de l’étape Silver (nettoyage, normalisation, déduplication)
* **data-dictionary.md** : dictionnaire de données du datamart (tables, colonnes, types, clés)
* **schema-etoile.md** : schéma du modèle décisionnel en étoile (table de faits, dimensions, relations)

**/tests**
Tests unitaires pour chaque couche (Bronze, Silver, Gold).
* **1_test_bronze.py** : tests sur l’ingestion Bronze
* **2_test_silver.py** : tests sur la conformation Silver
* **3_test_gold.py** : tests sur la modélisation et le chargement Gold

**Autres fichiers à la racine**

* **dashboard.py**
  Script de visualisation / tableau de bord :
  * connexion à MySQL
  * affichage des indicateurs analytiques (bonus dashboard)
* **docker-compose.yml**
  Déploiement de l’environnement MySQL via Docker.
* **requirements.txt**
  Liste des dépendances Python nécessaires au projet.
* **reset_db.py**
  Script de réinitialisation complète de la base MySQL.

---

## Ordre d’exécution du projet

1. Démarrage de l’environnement
* Lancer MySQL via Docker
* Installer les dépendances Python

2. Initialisation de la base
* Exécuter le script `ddl.sql`
* Optionnel : exécuter `reset_db.py`

3. Pipeline ETL Spark
* `1_Bronze.py`
* `2_silver.py`
* `check_silver_schema.py`
* `3_gold.py`

4. Analytique SQL
* Exécuter `queries_analysis.sql`

5. Dashboard (optionnel)
* Lancer `dashboard.py`

---

## How to run

**Installation des dépendances**

```Bash
pip install -r requirements.txt
```

**Lancer MySQL**

```Bash
docker-compose up -d
```

**Création du schéma** *(commande optionnelle, le script le fait déjà)*

```Bash
mysql -u <user> -p <database> < sql/ddl.sql
```

**Exécution du pipeline Spark**

```Bash
spark-submit etl/1_Bronze.py
spark-submit etl/2_silver.py
spark-submit etl/3_gold.py
```

**Analytique SQL**

```Bash
mysql -u <user> -p <database> < sql/queries_analysis.sql
```

---

## Correspondance avec les étapes du projet

| Étape pédagogique         | Scripts / livrables associés                                    |
| ------------------------- | --------------------------------------------------------------- |
| Collecte                  | `1_Bronze.py`                                                   |
| Nettoyage / normalisation | `2_silver.py`                                                   |
| Qualité des données       | `metrics_silver.json`, `gold_metrics.json`                      |
| Modélisation              | `3_gold.py`, `ddl.sql`                                          |
| Chargement MySQL          | `3_gold.py`                                                     |
| Analytique SQL            | `queries_analysis.sql`                                          |
| Reporting                 | `dashboard.py`                                                  |
| Documentation             | `/docs`, `README.md`, `data-dictionary.md`, `schema-etoile.md`  |

*Les éléments de modélisation et de documentation détaillée (dictionnaire de données et schéma décisionnel) sont disponibles dans le dossier `/docs`.*

Ce README fournit l’ensemble des informations nécessaires à la compréhension, à l’exécution et à l’évaluation du projet.

---
