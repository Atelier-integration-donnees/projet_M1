# Repo projet_M1 : Intégration des données

*Formateur* : Benoît Gerald

*Membres du groupe* : 

* Anas AMAZOUZ
* Abdelmounaim EL HOUZI
* Dunvael LE ROUX

*Lien du kanban* : <https://trello.com/invite/b/6914676875effe805916fb75/ATTI787a959b552291903e206cdf8a69b3aa22FDC4AC/kanban-equipe>

---

## Structure du projet

### Prérequis

#### Outils

Outils nécessaires au brief

* ETL Apache Spark
* Python
* MySQL
* Export de données : https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz
* Lien de la page : https://fr.openfoodfacts.org/data
* GitHub

#### Choix techniques

Justification des technbologies et outils utilisés
Trello Kanban

### Architecture du projet

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

### Étapes

---

## Livrables attendus

* Repo (Git) structuré => docs (README, data-dictionary, schémas), /etl (code Spark), /sql
(DDL/DML), /tests, /conf.
* *ipeline Spark reproductible : initial load (export complet). Log des métriques de
qualité.
* Datamart MySQL (étoile ou flocon contrôlé) + scripts DDL/DML.
* Cahier de qualité : règles, coverage, anomalies, before/after.
* Jeu de requêtes analytiques (SQL) répondant à des questions métiers.
* Note d’architecture : choix techniques, schémas, stratégie d’upsert.

---

## Commandes utiles

---