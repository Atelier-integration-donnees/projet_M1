import sys
import os
import json
import time
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ==========================================================
# CONFIGURATION
# ==========================================================
BRONZE_PATH = "data/bronze/"
SILVER_PATH = "data/silver/"
METRICS_PATH = "logs/metrics_silver.json"

NUTRI_COLS = [
    "energy_kcal_100g",
    "fat_100g",
    "saturated_fat_100g",
    "sugars_100g",
    "salt_100g",
    "proteins_100g",
    # ### MODIF ICI : Ajout pour ne pas l'oublier dans le nettoyage ###
    "fiber_100g",
    "sodium_100g"
]

# ==========================================================
# SPARK SESSION
# ==========================================================
def create_spark():
    return (
        SparkSession.builder
        .appName("Silver_OpenFoodFacts")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

# ==========================================================
# METRICS LOGGER
# ==========================================================
def save_metrics(metrics):
    os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
    history = []
    if os.path.exists(METRICS_PATH):
        try:
            with open(METRICS_PATH, "r") as f:
                history = json.load(f)
        except:
            pass
    history.append(metrics)
    with open(METRICS_PATH, "w") as f:
        json.dump(history, f, indent=4)

# ==========================================================
# MAIN SILVER
# ==========================================================
def main():
    start = time.time()
    spark = create_spark()
    run_date = datetime.now().isoformat()

    print("\n============================================================")
    print(f">>> SILVER LAYER - CONFORMATION & QUALITÉ ({run_date})")
    print("============================================================\n")

    try:
        # ------------------------------------------------------
        # 1. READ BRONZE
        # ------------------------------------------------------
        df = spark.read.parquet(BRONZE_PATH)
        input_rows = df.count()

        # ### MODIF ICI : GESTION DES COLONNES MANQUANTES ###
        # Pour éviter le crash si une colonne n'existe pas dans le Bronze
        cols_security = {
            "fiber_100g": "float",
            "ecoscore_grade": "string",
            "categories_tags": "string",
            "countries_tags": "string",
            "nutriscore_grade": "string",
            "nova_group": "int"
        }
        for col_name, col_type in cols_security.items():
            if col_name not in df.columns:
                print(f"⚠️ Création colonne manquante : {col_name}")
                df = df.withColumn(col_name, F.lit(None).cast(col_type))
        # ###################################################

        # ------------------------------------------------------
        # 2. DEDUP (BUSINESS KEY)
        # ------------------------------------------------------
        window = Window.partitionBy("code").orderBy(F.col("last_modified_t").desc())
        df = (
            df.withColumn("rn", F.row_number().over(window))
              .filter(F.col("rn") == 1)
              .drop("rn")
        )
        dedup_rows = df.count()

        # ------------------------------------------------------
        # 3. MULTILINGUE + CLEAN
        # ------------------------------------------------------
        df = df.withColumn(
            "product_name",
            F.coalesce(
                F.col("product_name_fr"),
                F.col("product_name_en"),
                F.col("product_name"),
                F.lit("produit inconnu")
            )
        )

        # ### MODIF ICI : Nettoyage étendu pour les Bridges Tables ###
        for col_text in ["product_name", "brands", "categories_tags", "countries_tags", "main_category"]:
            if col_text in df.columns:
                df = df.withColumn(col_text, F.lower(F.trim(F.col(col_text))))
        # ##########################################################

        # ------------------------------------------------------
        # 4. UNIT HARMONIZATION
        # ------------------------------------------------------
        # ### MODIF ICI : Sécurité si energy_kj n'existe pas ###
        if "energy_kj_100g" in df.columns:
            df = df.withColumn(
                "energy_kcal_100g",
                F.coalesce(F.col("energy_kcal_100g"), F.col("energy_kj_100g") / F.lit(4.184))
            )

        df = df.withColumn(
            "salt_100g",
            F.when(
                F.col("salt_100g").isNull() & F.col("sodium_100g").isNotNull(),
                F.col("sodium_100g") * 2.5
            ).otherwise(F.col("salt_100g"))
        )

        # ------------------------------------------------------
        # 5. NUTRITIONAL BOUNDS FIX (🔥 IMPORTANT)
        # ------------------------------------------------------
        bounds = {}
        for c in NUTRI_COLS:
            # Vérif si colonne existe grâce au bloc de sécurité plus haut
            max_val = 1000 if c == "energy_kcal_100g" else 100
            bounds[c] = max_val
            df = df.withColumn(
                c,
                F.when(
                    (F.col(c) < 0) | (F.col(c) > max_val),
                    F.lit(None)
                ).otherwise(F.col(c))
            )

        # ------------------------------------------------------
        # 6. COMPLETENESS SCORE
        # ------------------------------------------------------
        # ### MODIF ICI : Inclus fiber_100g et ecoscore s'ils existent ###
        possible_cols = ["product_name", "brands", "nutriscore_grade", "ecoscore_grade"] + NUTRI_COLS
        completeness_cols = [c for c in possible_cols if c in df.columns]
        
        completeness_score = sum(
            F.when(F.col(c).isNotNull(), 1).otherwise(0)
            for c in completeness_cols
        ) / float(len(completeness_cols))

        df = df.withColumn("completeness_score", completeness_score)

        # ------------------------------------------------------
        # 7. QUALITY AUDIT JSON (NULL + HORS BORNES)
        # ------------------------------------------------------
        anomalies = []
        for c in NUTRI_COLS:
            max_val = bounds[c]
            anomalies.append(
                F.when(F.col(c).isNull(), True)
                 .when((F.col(c) < 0) | (F.col(c) > max_val), True)
                 .otherwise(False)
                 .alias(f"{c}_error")
            )

        df = df.withColumn("quality_issues_struct", F.struct(*anomalies))
        df = df.withColumn("quality_issues_json", F.to_json("quality_issues_struct"))

        # ------------------------------------------------------
        # 8. VALIDITY FLAG
        # ------------------------------------------------------
        df = df.withColumn(
            "is_valid",
            reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in NUTRI_COLS if c in df.columns])
        )

        invalid_rows = df.filter(~F.col("is_valid")).count()

        # ------------------------------------------------------
        # 9. METADATA (SCD2 READY ✅)
        # ------------------------------------------------------
        df = (
            df.withColumn("effective_from", F.current_timestamp())
              .withColumn("is_current", F.lit(True))
        )

        # ------------------------------------------------------
        # 10. HASH DIFF
        # ------------------------------------------------------
        # ### MODIF ICI : On sécurise la liste des colonnes pour le hash ###
        cols_for_hash = ["code", "product_name", "brands", "nutriscore_grade"] + \
                        [c for c in NUTRI_COLS if c in df.columns]
        
        df = df.withColumn(
            "hash_diff",
            F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in cols_for_hash]))
        )

        # ------------------------------------------------------
        # 11. SELECT FINAL
        # ------------------------------------------------------
        # ### MODIF ICI : On ajoute les colonnes manquantes (fiber, ecoscore, tags) ###
        final_columns = [
            "code", "product_name", "product_name_fr", "product_name_en",
            "brands", "categories_tags", "countries_tags", "main_category",
            "last_modified_t", "created_t",
            "nutriscore_grade", "nova_group", "ecoscore_grade",  # Ajout ecoscore
            "completeness_score",
            "energy_kcal_100g", "energy_kj_100g",
            "sugars_100g", "fat_100g", "saturated_fat_100g",
            "salt_100g", "sodium_100g", "proteins_100g", "fiber_100g", # Ajout fiber
            "quality_issues_struct", "quality_issues_json",
            "is_valid", "effective_from", "hash_diff"
        ]
        # On ne sélectionne que ce qui existe vraiment pour éviter erreur
        sel_cols = [c for c in final_columns if c in df.columns]
        
        df = df.select(*sel_cols)

        # ------------------------------------------------------
        # 12. WRITE SILVER
        # ------------------------------------------------------
        df.write.mode("overwrite").parquet(SILVER_PATH)

        # ------------------------------------------------------
        # 13. METRICS
        # ------------------------------------------------------
        total_rows = df.count()
        non_null_count = df.select(
            sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in completeness_cols).alias("non_null_total")
        ).collect()[0]["non_null_total"]

        total_fields = total_rows * len(completeness_cols)
        coverage_score = (non_null_count / total_fields) * 100

        # Note : Pour les metrics, on simplifie car quality_issues_struct dépend des colonnes présentes
        # On garde ton code original mais on le protège :
        try:
            df = df.withColumn(
                "quality_issues_array",
                F.array([F.col(f"quality_issues_struct.{c}_error") for c in NUTRI_COLS if f"{c}_error" in df.select("quality_issues_struct.*").columns])
            )
            anomalies_count = df.filter(F.expr("array_contains(quality_issues_array, true)")).count()
            anomalies_rate = anomalies_count / total_rows * 100 
        except:
            anomalies_rate = 0.0

        invalid_rate = invalid_rows / total_rows * 100
        missing_count = df.filter(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in completeness_cols])).count()
        missing_rate = missing_count / total_rows * 100 

        metrics = {
            "run_date": run_date,
            "layer": "Silver",
            "input_rows": input_rows,
            "after_dedup": dedup_rows,
            "invalid_flagged": invalid_rows,
            "coverage_score": round(coverage_score, 2),
            "anomalies_rate": round(anomalies_rate, 2),
            "invalid_rate": round(invalid_rate, 2),
            "missing_rate": round(missing_rate, 2),
            "execution_sec": round(time.time() - start, 2)
        }
        save_metrics(metrics)

        print("✅ SILVER terminé avec succès")

    except Exception as e:
        print("❌ ERREUR SILVER :", e)
        raise
    finally:
        spark.stop()

# ==========================================================
if __name__ == "__main__":
    main()