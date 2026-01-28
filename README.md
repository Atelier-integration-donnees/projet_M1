# Repo projet_M1 : Intégration des données

*Formateur* : Benoît Gerald

*Membres du groupe* : 

* Anas AMAZOUZ
* Abdelmounaim EL HOUZI
* Dunvael LE ROUX

*Lien Edensia pour joindre livrables* : <https://flow.edensia.com/login>

---

## Structure du projet

### Prérequis

#### Outils

* ETL Apache Spark
* Python
* MySQL
* Docker Desktop
* Export de données : https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz
* Lien de la page : https://fr.openfoodfacts.org/data
* GitHub
* IDE pour clôner le répertoire et exécuter le code/script

#### Dépendances Python

* Installation avec le fichier *requirements.txt*

### Architecture du projet

Le projet consiste à créer une chaîne d’intégration de données pour construire un datamart analytique à partir des données OpenFoodFacts, en exploitant Apache Spark (Java ou PySpark) pour l’ETL et MySQL pour le datamart relationnel.

#### Typologie

#### Architecture & contraintes

* Ingestion (Bronze) : lire JSON (ou CSV). Extractions des champs clefs (code, noms,
nutriments 100g, tags…).
* Conformation (Silver) : normaliser types/units, flatten des structures, dédoublonnage
par code-barres et choix de la langue (fr > en > fallback).
* Modélisation (Gold) : tables dimensionnelles + fact table(s)=> Cible : MySQL 8 via Spark JDBC.
* Qualité : produire des métriques (complétude, unicité, cohérence, référentiels) et rapports (CSV/JSON + tableau de bord SQL).

#### Fichiers du projet

---

### Étapes principales du projet

1. Compréhension du contexte et cadrage



2. Collecte et préparation des données



3. Nettoyage et normalisation



4. Modélisation des données



5. Chargement dans le datamart


6. Qualité et reporting



7. Analytique SQL



8. Documentation et livrables

* Préparer le README du projet, data-dictionary, schémas de données, documentation d’exécution et prompts.
* Déposer dans un repo Git structuré.

---

## Livrables attendus

* Repo (Git) structuré => docs (README, data-dictionary, schémas), /etl (code Spark), /sql
(DDL/DML), /tests, /conf.
* Pipeline Spark reproductible : initial load (export complet). Log des métriques de
qualité.
* Datamart MySQL (étoile ou flocon contrôlé) + scripts DDL/DML.
* Cahier de qualité : règles, coverage, anomalies, before/after.
* Jeu de requêtes analytiques (SQL) répondant à des questions métiers.
* Note d’architecture : choix techniques, schémas, stratégie d’upsert.

---

## Critères d'évaluations (100 points)

* Collecte & incrémental (20) : bulk, idempotence.
* Qualité & métriques (20) : règles solides, reporting clair, anomalies expliquées.
* Modèles Datamarts (20) : étoiles cohérentes, clés, index.
* ETL Spark (25) : code clair/testé, perfs (partitionnement, broadcast), upserts
maîtrisés.
* Analytique SQL (10) : requêtes pertinentes, résultats commentés.
* Docs & reproductibilité (5) : README, schémas, how-to run, journal des prompts.

### Bonus

* Conformité multilingue : résolution des noms produit/catégorie par priorité de langue
* Détection d’anomalies (exemple : IQR) sur nutriments
* **Petit dashboard (connecté à MySQL)**
* Historisation

---

## Commandes utiles

### How to run

Donner les droits d'exécution au script de déploiement automatique :

`chmod +x [NomScript]`

Lancer le script de déploiement automatique :

`. [NomScript].sh`

### Vérifications/Tests



---
