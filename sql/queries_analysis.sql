/* PROJET OPENFOODFACTS - ANALYSE MÉTIER 
   Auteurs : [Tes Noms]
*/

-- 1. TOP 10 des marques avec le plus de produits référencés
-- Objectif : Identifier les acteurs majeurs de la base.
SELECT 
    b.brand_name, 
    COUNT(f.fact_id) as total_produits
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
ORDER BY total_produits DESC
LIMIT 10;

-- 2. Distribution des Nutri-Scores
-- Objectif : Voir la répartition de la qualité nutritionnelle globale.
SELECT 
    nutriscore_grade, 
    COUNT(*) as volume,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_nutrition_snapshot), 2) as pourcentage
FROM fact_nutrition_snapshot
WHERE nutriscore_grade IN ('a','b','c','d','e')
GROUP BY nutriscore_grade
ORDER BY nutriscore_grade;

-- 3. Les catégories les plus "sucrées" (Moyenne > 0)
-- Objectif : Santé publique.
SELECT 
    c.category_name_fr,
    ROUND(AVG(f.sugars_100g), 2) as sucre_moyen_g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category c ON p.primary_category_sk = c.category_sk
GROUP BY c.category_name_fr
HAVING count(*) > 50 -- On ignore les petites catégories
ORDER BY sucre_moyen_g DESC
LIMIT 10;

-- 4. Évolution de la qualité de la donnée (Complétude) par année
-- Objectif : Monitoring Data Quality.
SELECT 
    t.year,
    ROUND(AVG(f.completeness_score) * 100, 2) as completion_rate_pct
FROM fact_nutrition_snapshot f
JOIN dim_time t ON f.time_sk = t.time_sk
GROUP BY t.year
ORDER BY t.year DESC;