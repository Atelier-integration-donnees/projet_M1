# Schéma du Datamart — Modèle en Étoile (Gold Layer)

**Vue d’ensemble**

Le datamart OpenFoodFacts est modélisé selon un schéma en étoile (Star Schema), centré sur l’analyse nutritionnelle des produits alimentaires.
* **Table de faits centrale** : fact_nutrition_snapshot
* **Dimensions périphériques** :
  * dim_product (SCD Type 2)
  * dim_time
  * dim_brand
  * dim_category
  * dim_country
Les relations N–N entre produits et catégories/pays sont gérées via des tables de pont.

---

## Schéma en étoile (représentation logique)

```
                          dim_time
                       (time_sk, date,
                        year, month,
                        week, iso_week)
                              |
                              |
                              |
        dim_brand         fact_nutrition_snapshot          dim_product (SCD2)
     (brand_sk,           -------------------------     (product_sk, code,
      brand_name)         fact_id (PK)                   product_name,
           |              product_sk (FK) -------------- brand_sk (FK)
           |              time_sk (FK) ----------------- effective_from
           |              energy_kcal_100g               effective_to
           |              fat_100g                        is_current
           |              saturated_fat_100g             hash_diff
           |              sugars_100g                    countries_multi
           |              salt_100g
           |              sodium_100g
           |              proteins_100g
           |              fiber_100g
           |              nutriscore_grade
           |              nova_group
           |              ecoscore_grade
           |              completeness_score
           |              quality_issues_json
                              |
                              |
                  --------------------------------
                  |                              |
         bridge_product_category        bridge_product_country
         (product_sk, category_sk)     (product_sk, country_sk)
                  |                              |
             dim_category                   dim_country
        (category_sk, code,             (country_sk, code,
         name_fr, level,                 country_name_fr)
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
