# Data Dictionary & Architecture — OpenFoodFacts Datamart
1. Introduction et contexte métier

Ce projet vise à mettre en pratique les concepts d’ingénierie des données massives (Big Data).

Il s’appuie sur la base de données collaborative OpenFoodFacts (OFF), une ressource mondiale référençant plusieurs millions de produits alimentaires.
L’objectif métier est de valoriser ces données brutes afin de construire une solution décisionnelle capable de répondre à des problématiques de santé publique, telles que :
* la répartition des Nutri-Scores par marque,
* l’analyse des teneurs en sucres ou en additifs par catégorie de produits.

La complexité du projet réside dans :
* le volume important des données (exports CSV volumineux),
* leur hétérogénéité,
ce qui rend les approches de traitement traditionnelles inadaptées et justifie l’utilisation d’une architecture distribuée.

---

2. Objectifs pédagogiques et techniques

Conformément au cahier des charges, ce projet a pour objectif la mise en place d’une chaîne d’intégration de données (Pipeline ETL).

Les principaux défis techniques sont :
* Ingestion de données massives
  Utilisation d’Apache Spark (via PySpark) pour traiter efficacement les exports complets OpenFoodFacts.
* Qualité des données (Data Quality)
  Implémentation de règles de nettoyage strictes (détection d’anomalies, harmonisation des unités) et calcul d’indicateurs de complétude.
* Modélisation décisionnelle
  Conception et alimentation d’un datamart en schéma étoile dans MySQL 8.
* Historisation (SCD Type 2)
  Gestion des dimensions à évolution lente afin de conserver l’historique des modifications produits.

---

3. Analyse et sélection des données (Scoping)

Le fichier source brut contient plus de 150 colonnes.
Une intégration complète serait :
* coûteuse techniquement,
* inutile fonctionnellement pour les KPI ciblés.
Une sélection stricte des attributs pertinents a donc été réalisée.

---

4. Données d’identification et de classification (Dimensions)

Ces champs alimentent les tables de dimensions (dim_product, dim_brand, dim_category).

| Champ source (CSV) | Rôle dans le datamart | Justification                             |
| ------------------ | --------------------- | ----------------------------------------- |
| `code`             | Clé fonctionnelle     | Identifiant unique du produit (EAN-13)    |
| `product_name`     | Attribut descriptif   | Nom principal du produit                  |
| `brands`           | Dimension             | Agrégation par marque                     |
| `categories_tags`  | Dimension             | Classification produit                    |
| `countries_tags`   | Filtre géographique   | Filtrage des produits vendus en France    |
| `main_category`    | Hiérarchie            | Catégorie principale en cas de multi-tags |

---

5. Données temporelles (SCD2)

| Champ source (CSV) | Rôle technique | Justification                              |
| ------------------ | -------------- | ------------------------------------------ |
| `last_modified_t`  | Versionning    | Timestamp UNIX de la dernière modification |
| `created_t`        | Information    | Date de création de la fiche produit       |

---

6. Métriques et scores (Table de faits)

| Champ source (CSV)   | Colonne cible (SQL)  | Type    | Usage analytique         |
| -------------------- | -------------------- | ------- | ------------------------ |
| `nutriscore_grade`   | `nutriscore_grade`   | CHAR(1) | KPI principal            |
| `nova_group`         | `nova_group`         | INT     | Niveau de transformation |
| `energy-kcal_100g`   | `energy_kcal_100g`   | DECIMAL | Valeur énergétique       |
| `sugars_100g`        | `sugars_100g`        | DECIMAL | KPI santé                |
| `fat_100g`           | `fat_100g`           | DECIMAL | Matières grasses         |
| `saturated-fat_100g` | `saturated_fat_100g` | DECIMAL | Acides gras saturés      |
| `salt_100g`          | `salt_100g`          | DECIMAL | Sel                      |
| `proteins_100g`      | `proteins_100g`      | DECIMAL | Apport protéique         |
| `completeness`       | `completeness_score` | FLOAT   | Score qualité OFF        |

---

7. Exclusions volontaires

Afin d’optimiser les performances Spark et MySQL, les colonnes suivantes ont été exclues :

**Micro-nutriments rares**
* `butyric-acid_100g`
* `caproic-acid_100g`
* `vitamin-pp_100g`
* `water-hardness_100g`
Justification : taux de remplissage trop faible.

**Champs textuels lourds**
* `ingredients_text`
* `packaging_text`
Justification : non structurés, peu utiles pour l’analytique SQL.

**URLs d’images**
* `image_small_url`
* `image_ingredients_url`
Justification : inutiles pour un datamart chiffré.

---

8. Architecture et flux de données

Le projet repose sur une architecture Médaillon :

```
Raw (CSV)
   ↓
Bronze
   ↓
Silver
   ↓
Gold (MySQL)
```

---

9. Pipeline ETL Spark (PySpark)

9.1 Couche Bronze — Ingestion

* Lecture du CSV brut avec schéma explicite (StructType)
* Projection immédiate (column pruning)
* Limitation volumétrique (limit(50000)) en environnement local
* Conversion vers Apache Parquet

