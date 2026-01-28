import sys
import os
import time
from pyspark.sql import SparkSession, functions as F

# ==========================================================
# CONFIGURATION SPARK
# ==========================================================
spark = SparkSession.builder \
    .appName("Test_Silver_Complete") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver")

# Couleurs terminal
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"

def print_header(title):
    print(f"\n{CYAN}{'='*60}")
    print(f">>> {title}")
    print(f"{'='*60}{RESET}")

def check(condition, message):
    if condition:
        print(f"{GREEN}✅ [SUCCÈS] {message}{RESET}")
    else:
        print(f"{RED}❌ [ÉCHEC] {message}{RESET}")

# ==========================================================
# MAIN TEST
# ==========================================================
def main():
    print_header("CHARGEMENT DES DONNÉES")
    
    # 1. Chargement Bronze
    if not os.path.exists(BRONZE_PATH):
        print(f"{RED}Erreur: Bronze introuvable.{RESET}")
        return
    df_bronze = spark.read.parquet(BRONZE_PATH)
    count_bronze = df_bronze.count()
    print(f"📄 BRONZE chargé : {count_bronze} lignes")

    # 2. Chargement Silver
    if not os.path.exists(SILVER_PATH):
        print(f"{RED}Erreur: Silver introuvable. Lancez '2_silver.py'.{RESET}")
        return
    df_silver = spark.read.parquet(SILVER_PATH)
    count_silver = df_silver.count()
    print(f"📄 SILVER chargé : {count_silver} lignes")

    # ==========================================================
    # VISUALISATION : BRONZE vs SILVER
    # ==========================================================
    print_header("1. VISUALISATION : AVANT (BRONZE) vs APRÈS (SILVER)")

    cols_bronze = ["code", "product_name", "product_name_fr", "salt_100g", "sodium_100g"]
    avail_cols_bronze = [c for c in cols_bronze if c in df_bronze.columns]
    df_bronze.select(avail_cols_bronze).show(5, truncate=True)

    cols_silver = ["code", "product_name", "salt_100g", "completeness_score", "is_valid",
                   "fiber_100g", "ecoscore_grade"]
    avail_cols_silver = [c for c in cols_silver if c in df_silver.columns]
    df_silver.select(avail_cols_silver).show(5, truncate=True)

    # ==========================================================
    # VALIDATIONS AUTOMATISÉES
    # ==========================================================
    print_header("2. TESTS AUTOMATISÉS")

    # 🔹 Dédoublonnage
    distinct_codes = df_silver.select("code").distinct().count()
    check(distinct_codes == count_silver, f"Dédoublonnage ({distinct_codes} codes uniques)")

    # 🔹 Nettoyage texte (minuscules)
    upper_case = df_silver.filter(F.col("product_name").rlike("[A-Z]")).count()
    check(upper_case == 0, "Nettoyage textuel (minuscules)")

    # 🔹 Bornes nutritionnelles
    err_kcal = df_silver.filter((F.col("energy_kcal_100g") < 0) | (F.col("energy_kcal_100g") > 1000)).count()
    err_fat = df_silver.filter((F.col("fat_100g") < 0) | (F.col("fat_100g") > 100)).count()
    check(err_kcal == 0 and err_fat == 0, "Valeurs nutritionnelles dans les bornes")

    # 🔹 Colonnes manquantes
    for c in ["fiber_100g", "ecoscore_grade"]:
        check(c in df_silver.columns, f"Colonne manquante ajoutée : {c}")

    # 🔹 Flag qualité
    invalid_rows = df_silver.filter(F.col("is_valid") == False).count()
    print(f"ℹ️ Produits invalides : {invalid_rows}")
    if invalid_rows > 0:
        df_silver.filter(F.col("is_valid") == False).select("quality_issues_json").show(2, truncate=False)
    check("is_valid" in df_silver.columns, "Colonne is_valid présente")

    # 🔹 Hash SCD2
    check("hash_diff" in df_silver.columns, "Hash diff présent pour SCD2")

    # 🔹 Completeness score
    comp_score_null = df_silver.filter(F.col("completeness_score").isNull()).count()
    check(comp_score_null == 0, "Completeness score calculé pour toutes les lignes")

    # 🔹 Bridge tables (tags)
    for tag_col in ["categories_tags", "countries_tags"]:
        check(tag_col in df_silver.columns, f"Bridge table présente : {tag_col}")

    print_header("✅ FIN DU RAPPORT SILVER")

if __name__ == "__main__":
    main()
