# README – Couche SILVER (OpenFoodFacts)

## 1. Introduction

La couche Silver est la seconde étape du pipeline ETL Spark (Bronze → Silver → Gold) appliqué aux données OpenFoodFacts (OFF).
Elle transforme les données brutes (Bronze) en un dataset conforme, normalisé, nettoyé et auditables, prêt à alimenter le datamart (Gold / MySQL).

Objectifs de la couche Silver:
 - Conformation des données (types, unités, formats)
 - Déduplication par code-barres (EAN-13)
 - Normalisation multilingue (FR > EN > fallback)
 - Contrôle qualité (bornes, complétude, validité)
 - Auditabilité (flagging + logs)
 - Préparation SCD2 via hash diff et métadonnées
 - Métriques de qualité exportées à chaque exécution

## -----------------------------------------------------------------------------------------------------------------------------------------

## 2. Données d’entrée et de sortie

### Entrée
La couche Silver consomme les données issues de la couche Bronze :

- Format : **Parquet**
- Chemin : `data/bronze/`

Ces données contiennent les informations brutes extraites depuis OpenFoodFacts (OFF), avec des champs tels que :

- `code` (EAN-13)
- `product_name_fr`, `product_name_en`, `product_name`
- `brands`
- `nutriscore_grade`
- `nutriments.*_100g`
- `last_modified_t`

### Sortie
Le résultat de la couche Silver est un dataset Parquet :

- Format : **Parquet**
- Chemin : `data/silver/`

Ce dataset contient des colonnes nettoyées, normalisées, et enrichies avec des métadonnées de qualité.

## -----------------------------------------------------------------------------------------------------------------------------------------

## 3. Description du pipeline Silver

### 3.1 Lecture des données Bronze
Le script lit les fichiers Parquet du dossier `data/bronze/`.  
Il compte le nombre de lignes en entrée (`input_rows`) pour générer des métriques de qualité.

### 3.2 Déduplication (Business Key = code-barres)
La couche Silver garantit l’unicité des produits :

- Les données sont partitionnées par `code` (EAN-13).
- Elles sont triées par `last_modified_t` (date de dernière modification).
- Seul l’enregistrement le plus récent est conservé.

Cela permet d’éviter la duplication de produits et de ne conserver que la version la plus à jour.

### 3.3 Choix du nom du produit (multilingue)
Pour assurer une cohérence linguistique, le script applique une logique de priorité :

1. `product_name_fr` (priorité maximale)
2. `product_name_en`
3. `product_name`
4. si aucun nom disponible → `produit inconnu`

Ensuite, le nom du produit est normalisé (minuscules + suppression des espaces inutiles).

La marque (`brands`) subit le même nettoyage (trim + lowercase).

### 3.4 Harmonisation des unités

#### Conversion kJ → kcal
Les données OFF peuvent contenir l’énergie en kJ ou en kcal.  
La couche Silver convertit automatiquement :

- si `energy_kcal_100g` est null
- alors `energy_kj_100g / 4.184`

#### Conversion sodium → sel
Si le champ `salt_100g` est absent mais `sodium_100g` existe, la couche Silver calcule :
salt_100g = sodium_100g * 2.5


### 3.5 Correction des valeurs hors bornes (qualité)
Les valeurs nutritionnelles sont contrôlées :

- `energy_kcal_100g` : bornes 0 à 1000
- autres nutriments : bornes 0 à 100

Si une valeur est négative ou supérieure à la borne, elle est remplacée par `null`.

Cette règle permet de supprimer les anomalies (ex : données mal saisies, erreurs d’unité).

### 3.6 Score de complétude
Un score de complétude est calculé sur les champs clés :

- `product_name`
- `brands`
- `nutriscore_grade`
- nutriments clés (6 champs)
                |----------------------------------------------------------------------|
                |     completeness_score = nb_champs_non_null / nb_champs_totaux       |
                |----------------------------------------------------------------------|

Le score est un ratio (0 à 1) représentant le pourcentage de champs renseignés.

### 3.7 Audit qualité (JSON)
La couche Silver génère un champ `quality_issues_json` contenant un JSON d’anomalies :

- Exemple : `energy_kcal_100g_error: true` si null

Ce champ permet de tracer les problèmes par produit et d’alimenter un rapport de qualité.

### 3.8 Flag de validité
Un produit est considéré **valide** si tous les nutriments clés sont renseignés.  
Le champ `is_valid` vaut :

- `True` si tous les nutriments sont non null
- `False` sinon

### 3.9 Métadonnées SCD2
La couche Silver prépare l’historisation :

- `effective_from` : timestamp du run
- `is_current` : True (version courante)

### 3.10 Hash Diff
Un hash MD5 est calculé sur les champs essentiels :

- code
- nom produit
- marque
- nutriscore
- nutriments

Ce hash permet de détecter si un produit a changé et facilite la gestion SCD2 dans une étape ultérieure.

### 3.11 Écriture du dataset Silver
Le dataset final est écrit en Parquet dans : data/silver/

## -----------------------------------------------------------------------------------------------------------------------------------------

## 4. Logs et métriques

À la fin du pipeline, le script enregistre un JSON de métriques dans :logs/metrics_silver.json


Les métriques enregistrées sont :

- `run_date` : date et heure du run
- `layer` : “Silver”
- `input_rows` : nombre de lignes lues en entrée
- `after_dedup` : nombre de lignes après déduplication
- `invalid_flagged` : nombre de produits invalides (is_valid = False)
- `coverage_score` : score global de complétude
- `anomalies_rate` : taux de produits avec au moins une anomalie
- `invalid_rate` : taux de produits invalides
- `missing_rate` : taux de produits avec champs manquants
- `execution_sec` : durée d’exécution

## -----------------------------------------------------------------------------------------------------------------------------------------

## 5. Résumé des règles de qualité appliquées

| Règle         | Description                                  |
|---------------|----------------------------------------------|
| Déduplication | Un produit par code-barres (dernier modifié) |
| Multilingue   | Priorité FR > EN > fallback                  |
| Unités        | kJ → kcal, sodium → sel                      |
| Bornes        | Nutriments hors bornes remplacés par null    |
| Complétude    | Score de complétude (0 à 1)                  |
| Audit         | JSON d’anomalies par nutriment               |
| Validité      | Produit valide si tous nutriments présents   |
| SCD2          | Métadonnées + hash diff                      |
|--------------------------------------------------------------|

## -----------------------------------------------------------------------------------------------------------------------------------------

## 6. Justification : audit (flagging) vs suppression
Dans un pipeline d’intégration de données, il est crucial de ne pas perdre d’information, même si certaines valeurs semblent incorrectes. Les données OpenFoodFacts peuvent contenir :
 - une erreur de saisie (ex : valeur négative)
 - un cas particulier réel mais rare (ex : produit très énergétique)

Si l’on supprime directement ces lignes, cela entraîne plusieurs problèmes :
 - biais sur les KPI (ex : taux de complétude)
 - perte d’historique des anomalies
 - impossibilité d’analyser la qualité des données

Dans la couche Silver, les valeurs invalides ne sont pas supprimées. Elles sont :

 - remplacées par null (pour ne pas fausser les calculs)
 - signalées via un champ quality_issues_json

Cela permet de conserver une traçabilité complète des anomalies tout en garantissant la fiabilité des mesures.

## -----------------------------------------------------------------------------------------------------------------------------------------

## 7.Taux d’anomalies / Coverage (métriques qualité)
Le pipeline Silver calcule des métriques qualité à chaque exécution afin de suivre la fiabilité des données au fil du temps.
Les métriques calculées sont :
|-----------------------------------------------------------------|
|  Métrique	       |   Description                                |
|------------------|----------------------------------------------|      
|  coverage_score  | % de champs clés non nuls                    |
|  anomalies_rate  | % de produits avec au moins 1 anomalie       |
|  invalid_rate	   | % de produits invalides (is_valid = False)   |
|  missing_rate	   | % de produits ayant des champs manquants     |
|-----------------------------------------------------------------|
# 7.1 Calculs réalisés:
Coverage score global : `coverage_score = (nb_champs_non_nulls / nb_champs_totaux) * 100`

Taux d’anomalies : `anomalies_rate = (nb_produits_avec_quality_issues / nb_produits_total) * 100`

Taux d’invalidité : `invalid_rate = (nb_produits_invalides / nb_produits_total) * 100`

Taux de champs manquants : `missing_rate = (nb_produits_avec_champs_manquants / nb_produits_total) * 100`

# 7.1 Stockage des métriques: 
logs/metrics_silver.json


## -----------------------------------------------------------------------------------------------------------------------------------------

## 8. Référentiels : 
Dans un datamart orienté analyse, les KPI métiers sont souvent calculés par catégorie, par pays, ou par type d’additifs.
Or, les données OpenFoodFacts sont très hétérogènes : les catégories et taxonomies peuvent être :
 - dans plusieurs langues,
 - sous forme de tags,
 - incomplets ou mal structurés.

Pour garantir des analyses fiables, la couche Silver doit être conçue pour normaliser et rattacher les produits à des référentiels.

-- Objectifs des référentiels
Les référentiels permettent de :
 - normaliser les catégories
 - rattacher les produits aux taxonomies OFF
 - enrichir les données avec des attributs stables (niveau, parent, nom FR/EN)

Référentiels prévus

catégories (taxonomy) :	Normalisation des catégories produit
additifs : Analyse du nombre/type d’additifs
allergènes : KPI “produits allergènes”
pays : KPI par pays (distribution, complétude)

-- Stratégie d’intégration (prévue)
Même si ces référentiels ne sont pas encore implémentés dans la version actuelle du pipeline, ils sont prévus et compatibles avec l’architecture Silver.
La stratégie d’intégration serait :
 - Charger les référentiels (fichiers JSON/CSV) en broadcast
   (car ce sont des tables de petite taille)
 - Faire un join avec les données Silver sur les clés pertinentes
   (ex : category_tags, additives_tags, allergens_tags)

Ajouter les colonnes normalisées au dataset Silver.

 - les référentiels suivants sont prévus pour la couche Silver :
 - NutriScore : table de correspondance des lettres A/B/C/D/E
 - Nova group : table de classification (1 à 4)
 - Pays / Langues : table de mapping des codes pays et langues
 - Catégories : table hiérarchique des catégories produits

## -----------------------------------------------------------------------------------------------------------------------------------------

## 9. Limitations & améliorations possibles
Limitations actuelles : 

 -Les taxonomies ne sont pas encore intégrées (catégories, additifs, allergènes)
 - La gestion SCD2 complète (fermeture d’ancienne version + ouverture nouvelle version) est réalisée dans Gold, pas en Silver
 - Les anomalies sont identifiées par des bornes simples (pas de détection statistique avancée)

Améliorations possibles
  * Détection d’anomalies avancée
      - utiliser IQR / z-score sur les nutriments
      - détecter les outliers par catégorie :
  * Refinement du score de complétude
      - pondérer certains champs (ex : product_name plus important que fiber)
  * Intégration des taxonomies OFF
      - join avec catégories, additifs, allergènes
  * Validation de cohérence
      - vérifier que salt_100g et sodium_100g sont cohérents
      - vérifier que energy_kcal_100g correspond aux nutriments (approx.)


## -----------------------------------------------------------------------------------------------------------------------------------------

## 10. Conclusion
La couche Silver assure la transformation des données OpenFoodFacts en un jeu de données conforme, nettoyé et auditable, prêt à alimenter le datamart.
Gâce au dédoublonnage, à la normalisation multilingue, à l’harmonisation des unités, aux règles de qualité, au système d’audit et aux métriques de suivi, la Silver garantit une base fiable pour l’analyse et la prise de décision.
La sortie Silver servira de source principale pour construire les dimensions et la table de faits du datamart (dim_product, dim_brand, dim_category, fact_nutrition_snapshot).
La couche Gold permettra également de mettre en place l’historisation SCD2 complète et de produire des KPI métiers (Nutri-Score par catégorie, qualité nutritionnelle par marque, etc.).
