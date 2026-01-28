import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from conf import settings

def main():
    # Configuration mémoire musclée pour le Big Data
    spark = SparkSession.builder \
        .appName("1_Bronze") \
        .config("spark.driver.memory", "8g") \
        .master("local[*]") \
        .getOrCreate()

    print(f"\n{'='*60}")
    print(">>> DÉMARRAGE : INGESTION ROBUSTE (Bronze Layer)")
    print(f"{'='*60}")

    try:
        # 1. Lecture souple
        print(">>> [1/4] Lecture du CSV (Mapping des colonnes)...")
        # Note : On force inferSchema=False pour la performance (Exigence technique)
        df_raw = spark.read \
            .option("header", "true") \
            .option("delimiter", "\t") \
            .option("inferSchema", "false") \
            .csv(settings.RAW_PATH)
        
        # 2. Sélection et Typage Explicite (Schema Enforcement)
        print(">>> [2/4] Projection et Cast des types...")
        
        # AJOUT DES COLONNES MANQUANTES POUR LE SILVER (Langue & Sodium)
        # Note : On vérifie si les colonnes existent pour éviter que ça plante si le CSV change
        raw_cols = df_raw.columns
        
        # Fonction utilitaire pour prendre une colonne si elle existe, sinon NULL
        def safe_col(col_name):
            if col_name in raw_cols:
                return F.col(col_name)
            else:
                return F.lit(None)

        df_selected = df_raw.select(
            F.col("code").cast("string"),
            
            # --- IDENTIFICATION & LANGUES ---
            F.col("product_name").cast("string"),
            # AJOUT CRITIQUE pour Silver (Priorité Langue)
            safe_col("product_name_fr").cast("string").alias("product_name_fr"),
            safe_col("product_name_en").cast("string").alias("product_name_en"), # <--- AJOUTÉ ICI (Anglais)
            
            F.col("brands").cast("string"),
            F.col("categories_tags").cast("string"),
            F.col("countries_tags").cast("string"),
            F.col("main_category").cast("string"),
            
            # --- METADATA (Pour Dédoublonnage Silver) ---
            F.col("last_modified_t").cast("long"),
            F.col("created_t").cast("long"),
            
            # --- SCORES ---
            F.col("nutriscore_grade").cast("string"),
            F.col("nova_group").cast("int"),
            safe_col("completeness").cast("float").alias("completeness_score"),
            
            # --- NUTRIMENTS (Pour Qualité & Harmonisation) ---
            F.col("energy-kcal_100g").cast("float").alias("energy_kcal_100g"),
            safe_col("energy-kj_100g").cast("float").alias("energy_kj_100g"),    # <--- AJOUTÉ ICI (Kilojoules)
            
            F.col("sugars_100g").cast("float"),
            F.col("fat_100g").cast("float"),
            F.col("saturated-fat_100g").cast("float").alias("saturated_fat_100g"),
            
            F.col("salt_100g").cast("float"),
            # AJOUT CRITIQUE pour Silver (Calcul Sel via Sodium)
            safe_col("sodium_100g").cast("float").alias("sodium_100g"),
            
            F.col("proteins_100g").cast("float")
        )

        # 3. FILTRE "Technique" (Pour éviter les lignes vides inutiles)
        print(">>> [3/4] Filtrage technique initial...")
        df_valid = df_selected.filter(
            F.col("code").isNotNull() & F.col("product_name").isNotNull()
        )
        
        # --- ECHANTILLONNAGE (Pour le DEV) ---
        # NOTE M1 : Pour la mise en production réelle (Big Data), commentez cette ligne !
        # Le sujet demande de traiter des "données massives".
        print(">>> [INFO] Mode Démonstration : Limitation à 50 000 lignes.")
        df_sampled = df_valid.limit(50000)

        count = df_sampled.count()
        print(f">>> [INFO] Lignes ingérées : {count}")

        # 4. Écriture
        print(f">>> [4/4] Sauvegarde Parquet : {settings.BRONZE_PATH}")
        df_sampled.write.mode("overwrite").parquet(settings.BRONZE_PATH)
        print(">>> [SUCCÈS] Ingestion terminée.")

    except Exception as e:
        print(f"ERREUR : {e}")
        import traceback
        traceback.print_exc()
        
if __name__ == "__main__":
    main()