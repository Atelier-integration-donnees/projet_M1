#                                Couche Bronze – Ingestion OpenFoodFacts avec Apache Spark

## Introduction  :
La couche **Bronze** constitue la première étape du pipeline d’intégration de données du projet OpenFoodFacts – Nutrition & Qualité, cette couche a pour rôle d’assurer une **ingestion robuste, reproductible et maîtrisée** de données massives issues d’OpenFoodFacts, en utilisant **Apache Spark**.

La couche Bronze représente la **zone d’atterrissage structurée** des données brutes. Elle ne vise pas à corriger ou enrichir les données, mais à établir un **socle fiable** sur lequel s’appuieront les couches Silver (conformation et qualité) et Gold (datamart analytique).

## -----------------------------------------------------------------------------------------------------------------------------------------

## Positionnement dans l’architecture globale  :
Le cahier des charges impose une architecture en couches distinctes afin de séparer clairement les responsabilités de traitement.
####################################
#       OpenFoodFacts (CSV brut)   #
#               ↓                  #
#             BRONZE               #
#               ↓                  #
#             SILVER               #
#               ↓                  #
#              GOLD                #
#               ↓                  #
#           Datamart SQL           #
####################################

Dans cette architecture, la couche Bronze est responsable de **la lecture des données sources**, de leur **structuration minimale**, et de leur **persistance dans un format Big Data**.

## -----------------------------------------------------------------------------------------------------------------------------------------

## Données sources :
Les données traitées proviennent d’un export complet OpenFoodFacts.
- Format : CSV
- Séparateur : tabulation (`\t`)
- Présence d’un header : oui
- Volume : plusieurs millions de lignes
- Schéma : large, hétérogène et évolutif

Ces caractéristiques rendent nécessaire une ingestion **tolérante aux évolutions du schéma** et **indépendante de l’ordre des colonnes**, conformément aux exigences du projet.

## -----------------------------------------------------------------------------------------------------------------------------------------

## Choix technologiques  :
L’ensemble du pipeline Bronze est implémenté en **PySpark**, répondant aux contraintes techniques du cahier des charges.

Apache Spark est utilisé pour :
- traiter des volumes de données importants,
- garantir la scalabilité du pipeline,
- bénéficier d’un moteur de traitement distribué.

Le script est exécuté en mode local avec une configuration mémoire adaptée (driver à 8 Go), permettant de manipuler efficacement les données OpenFoodFacts.

## -----------------------------------------------------------------------------------------------------------------------------------------

## Description du script Bronze

La couche Bronze est implémentée dans le script `1_ingest_bronze.py`.  
Ce script est responsable de l’intégralité du processus d’ingestion, depuis la lecture du fichier source jusqu’à l’écriture des données au format Parquet.

Les chemins d’entrée et de sortie sont externalisés dans un fichier de configuration (`settings.RAW_PATH` et `settings.BRONZE_PATH`), garantissant la portabilité et la reproductibilité du pipeline.

## -----------------------------------------------------------------------------------------------------------------------------------------

  ## Lecture robuste des données sources

La première étape du traitement consiste à lire le fichier CSV OpenFoodFacts de manière volontairement souple.

La lecture est réalisée avec :
- détection automatique des noms de colonnes via le header,
- Désactivation de l’inférence automatique de schéma (inferSchema=False).
- spécification explicite du séparateur.

Ce choix permet :
- d’éviter les erreurs de typage liées aux valeurs manquantes ou aberrantes,
- de sécuriser le pipeline face aux évolutions futures du schéma OpenFoodFacts,
- de garantir une ingestion robuste indépendamment de l’ordre des colonnes.

## -----------------------------------------------------------------------------------------------------------------------------------------

  ## Sélection et typage explicite des colonnes

Une fois les données lues, le script applique une **projection stricte** sur les colonnes utiles, conformément au périmètre fonctionnel défini dans le cahier des charges.

Seuls les champs nécessaires à l’analyse nutritionnelle et à la qualité des données sont conservés.  
Chaque colonne est explicitement castée dans un type Spark approprié, ce qui garantit la cohérence du schéma dès la couche Bronze.

Les colonnes contenant des caractères non standards (tirets) sont renommées afin de produire un schéma propre et exploitable dans les couches suivantes.

Cette étape permet :
- de réduire fortement le volume de données,
- de sécuriser les traitements ultérieurs,
- de préparer les données pour la conformation Silver.

## -----------------------------------------------------------------------------------------------------------------------------------------

  ## Premier filtrage qualité

Conformément aux attentes du cahier des charges en matière de qualité, la couche Bronze applique un **filtrage minimal non destructif**.

Un produit est conservé uniquement si :
- le nom du produit est renseigné,
- la valeur énergétique pour 100 g est présente,
- la teneur en sucres pour 100 g est présente.

Ces champs sont considérés comme indispensables pour toute analyse nutritionnelle pertinente.  
Les règles de qualité plus complexes (bornes, cohérence, unités) sont volontairement reportées à la couche Silver afin de respecter la séparation des responsabilités.

## -----------------------------------------------------------------------------------------------------------------------------------------

  ## Échantillonnage contrôlé

Afin de garantir des performances acceptables en environnement local et de faciliter les phases de test et de démonstration, un échantillonnage est appliqué après le filtrage qualité.

Le jeu de données est limité à 50 000 produits valides.  

## -----------------------------------------------------------------------------------------------------------------------------------------

  ## Stockage des données Bronze

Les données issues de la couche Bronze sont stockées au format **Parquet**, en mode écrasement (`overwrite`).

Le format Parquet a été retenu car il :
- est optimisé pour Spark,
- permet une compression efficace,
- autorise une lecture colonne par colonne,
- constitue un standard dans les architectures Data Lake.

Ce stockage constitue la référence de travail pour la couche Silver.

## -----------------------------------------------------------------------------------------------------------------------------------------

## Logs, traçabilité et gestion des erreurs

Le script Bronze intègre :
- des logs explicites indiquant l’avancement des différentes étapes,
- un comptage des produits valides ingérés,
- une gestion complète des exceptions avec affichage de la stacktrace.

Ces éléments garantissent la traçabilité du pipeline et facilitent le débogage en cas de problème.

## -----------------------------------------------------------------------------------------------------------------------------------------

## Conformité au cahier des charges

La couche Bronze répond pleinement aux exigences du cahier des charges :

- ingestion de données massives avec Apache Spark,
- lecture robuste sans inférence implicite,
- extraction des champs clés OpenFoodFacts,
- typage explicite et structuration minimale,
- stockage Big Data optimisé,
- préparation directe à la couche Silver.

## -----------------------------------------------------------------------------------------------------------------------------------------

## Dictionnaire des Données (Bronze)
oici les 17 champs normalisés disponibles en sortie du traitement Bronze (format Parquet) :

|-------------------------------------------------------------------------------------------|
|          Champ      |    Type Spark   |                  Description                      |
|---------------------|-----------------|---------------------------------------------------|
| `code`              | String          | Code barre (EAN) - Clé fonctionnelle unique       |
| `product_name`      | String          | Désignation du produit                            |
| `brands`            | String          | Marque du produit                                 |
| `categories_tags`   | String          | Tags bruts des catégories                         |
| `countries_tags`    | String          | Tags des pays où le produit est vendu             |
| `main_category`     | String          | Catégorie principale estimée                      |
| `last_modified_t`   | Long            | Timestamp de la dernière modification             |
| `created_t`         | Long            | Timestamp de la création de la fiche              |
| `nutriscore_grade`  | String          | Note nutritionnelle (a, b, c, d, e)               |
| `nova_group`        | Integer         | Groupe NOVA (degré de transformation 1-4)         |
| `completeness_score`| Float           | Score de complétude de la fiche (0.0 à 1.0)       |
| `energy_kcal_100g`  | Float           | Énergie (Kcal) pour 100g                          |
| `sugars_100g`       | Float           | Sucres (g) pour 100g                              |
| `fat_100g`          | Float           | Matières grasses totales (g) pour 100g            |
| `saturated_fat_100g`| Float           | Acides gras saturés (g) pour 100g                 |
| `salt_100g`         | Float           | Sel (g) pour 100g                                 |
| `proteins_100g`     | Float           | Protéines (g) pour 100g                           |
| `product_name_fr`   | String          | Désignation du produit (Français)                 |
| `sodium_100g`       | Float           | Sodium (g) pour 100g - Pour calcul du Sel manquant|
|-------------------------------------------------------------------------------------------|

## Conclusion

La couche Bronze fournit un socle fiable pour l’ensemble du pipeline. Elle respecte les bonnes pratiques Big Data 
(Schema enforcement, format Parquet) et assure que seules des données techniquement exploitables sont transmises à la couche Silver.