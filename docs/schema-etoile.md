# Schéma du Datamart — Modèle en Étoile (Gold Layer)

**Vue d’ensemble**

Le datamart OpenFoodFacts suit un modèle décisionnel de type étoile, avec une normalisation partielle (snowflake) et des tables de pont pour gérer les relations N–N.

* Table de faits centrale : `fact_nutrition_snapshot`
* Dimensions directement reliées à la table de faits :
  * `dim_time`
  * `dim_product` (SCD Type 2)
* Dimensions accessibles via la dimension produit :
  * `dim_brand` (via `dim_product.brand_sk`)
  * `dim_category` (via `bridge_product_category`)
  * `dim_country` (via `bridge_product_country`)

---

## Schéma en étoile (représentation logique)

```
                                   dim_time
                           (time_sk, date,
                            year, month,
                            week, iso_week)
                                       |
                                       |
                               fact_nutrition_snapshot
                               -------------------------
                               fact_id (PK)
                               product_sk (FK)
                               time_sk (FK)
                               energy_kcal_100g
                               fat_100g
                               saturated_fat_100g
                               sugars_100g
                               salt_100g
                               proteins_100g
                               fiber_100g
                               sodium_100g
                               nutriscore_grade
                               nova_group
                               ecoscore_grade
                               completeness_score
                               quality_issues_json
                                       |
                                       |
                                dim_product (SCD2)
                        (product_sk, code, product_name,
                         brand_sk (FK),
                         effective_from, effective_to,
                         is_current, hash_diff,
                         countries_multi)
                           |                 |
                           |                 |
                        dim_brand      bridge_product_country
                    (brand_sk,          (product_sk, country_sk)
                     brand_name)               |
                                               |
                                            dim_country
                                       (country_sk, country_code,
                                        country_name_fr)

                                       |
                                       |
                              bridge_product_category
                              (product_sk, category_sk)
                                       |
                                       |
                                  dim_category
                           (category_sk, category_code,
                            category_name_fr, level,
                            parent_category_sk)
```

---

### Description des composants

**Table de faits** : `fact_nutrition_snapshot`
* Grain : 1 produit × 1 date (snapshot)
* Contient :
  * les mesures nutritionnelles (énergie, sucres, lipides, etc.)
  * les scores analytiques (Nutri-Score, NOVA, Eco-Score)
  * les indicateurs de qualité (complétude, anomalies JSON)
* Sert de base à toutes les requêtes analytiques SQL

**Dimension produit** : `dim_product (SCD Type 2)`
* Dimension à évolution lente (historisation)
* Permet de :
  * conserver l’historique des changements produits
  * analyser l’évolution nutritionnelle dans le temps
* Une seule version active par produit (is_current = TRUE)

**Dimension temps** : `dim_time`
* Support des analyses temporelles :
  * par jour
  * par semaine
  * par mois / année

**Dimensions de normalisation**
* `dim_brand` : normalisation des marques
* `dim_category` : classification hiérarchique des produits
* `dim_country` : pays associés aux produits

`dim_brand` est reliée à `dim_product` (et non directement à la table de faits), ce qui permet de centraliser les attributs descriptifs du produit et de gérer l’historisation SCD2.

**Tables de pont (relations N–N)**
* `bridge_product_category`
* `bridge_product_country`
Elles permettent de gérer les cas où :
* un produit appartient à plusieurs catégories,
* un produit est commercialisé dans plusieurs pays.

---

### Avantages du modèle

* Séparation claire faits / dimensions
* Performances SQL optimisées
* Support de l’historisation (SCD2)
* Modèle extensible (nouveaux KPI, nouvelles dimensions)
