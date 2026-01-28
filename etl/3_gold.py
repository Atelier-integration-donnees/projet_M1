# ========================================================================
# GOLD LAYER — VERSION FINALE (FIX STRICT CHAR-1)
# ========================================================================

import os, sys, shutil, json, time
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
import mysql.connector
from pyspark.sql.functions import col, when, to_json, array, split, explode, concat_ws, substring, to_date, trim, expr, lit

# ========================================================================
# 0. ENVIRONNEMENT
# ========================================================================
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
sys.stdout.reconfigure(encoding="utf-8")

# ========================================================================
# 1. PARAMÈTRES
# ========================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_PATH = os.path.join(BASE_DIR, "data/silver")
WAREHOUSE_PATH = os.path.join(BASE_DIR, "spark-warehouse")
METRICS_PATH = os.path.join(BASE_DIR, "logs/gold_metrics.json")

# Détection automatique du JAR
jars_dir = os.path.join(BASE_DIR, "jars")
try:
    jar_files = [f for f in os.listdir(jars_dir) if f.endswith(".jar")]
    JAR_PATH = os.path.join(jars_dir, jar_files[0])
except:
    print("⚠️ Attention: JAR introuvable, vérifiez le dossier jars/")
    JAR_PATH = ""

DB_HOST, DB_PORT = "localhost", "3309"
DB_NAME, DB_USER, DB_PASSWORD = "off_datamart", "root", "root"
JDBC_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"

# ========================================================================
# 2. MYSQL UTILS
# ========================================================================
def mysql_conn():
    return mysql.connector.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )

def insert_ignore(sql, rows):
    if not rows: return
    try:
        conn = mysql_conn()
        cur = conn.cursor()
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            cur.executemany(sql, rows[i:i+batch_size])
            conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ SQL Warning (Insert Ignore): {e}")

# ========================================================================
# 3. SPARK SESSION
# ========================================================================
def spark_session():
    if os.path.exists(WAREHOUSE_PATH):
        try: shutil.rmtree(WAREHOUSE_PATH)
        except: pass
        
    return (
        SparkSession.builder
        .appName("GOLD_OFF_FINAL_FIXED_V2")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", WAREHOUSE_PATH)
        .config("spark.driver.extraClassPath", JAR_PATH)
        .config("spark.executor.extraClassPath", JAR_PATH)
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

def read_jdbc(spark, table):
    return spark.read.format("jdbc") \
        .option("url", JDBC_URL).option("dbtable", table) \
        .option("user", DB_USER).option("password", DB_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver").load()

def write_jdbc(df, table, mode="append"):
    if df.isEmpty(): return
    df.write.format("jdbc") \
        .option("url", JDBC_URL).option("dbtable", table) \
        .option("user", DB_USER).option("password", DB_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode(mode).save()

# ========================================================================
# 4. MAIN PIPELINE
# ========================================================================
def main():
    start = time.time()
    spark = spark_session()

    try:
        print("\n" + "="*60)
        print(">>> STARTING GOLD PIPELINE (STRICT MODE)")
        print("="*60)

        # ----------------------------------------------------------------
        # A. CHARGEMENT SILVER
        # ----------------------------------------------------------------
        if not os.path.exists(SILVER_PATH): raise Exception("Silver path missing!")
        df = spark.read.parquet(SILVER_PATH)
        rows_silver = df.count()
        print(f"✅ Silver loaded: {rows_silver} rows")

        # Sécurisation des colonnes
        for c in ["sugars_100g", "fat_100g", "salt_100g", "energy_kcal_100g", "proteins_100g", "fiber_100g"]:
            if c not in df.columns: df = df.withColumn(c, lit(None).cast("float"))

        df = df.withColumn("quality_issues_json", 
            to_json(F.struct(
                (col("sugars_100g") > 100).alias("sugars_gt_100"),
                (col("energy_kcal_100g") <= 0).alias("energy_invalid")
            ))
        )

        # ----------------------------------------------------------------
        # B. DIMENSIONS
        # ----------------------------------------------------------------
        print("--- [1/4] Processing Dimensions ---")
        
        # TIME
        dim_time = df.select(to_date("effective_from").alias("date")).distinct() \
            .withColumn("year", F.year("date")) \
            .withColumn("month", F.month("date")) \
            .withColumn("day", F.dayofmonth("date")) \
            .withColumn("week", F.weekofyear("date")) \
            .withColumn("iso_week", F.weekofyear("date"))

        insert_ignore(
            "INSERT IGNORE INTO dim_time (date, year, month, day, week, iso_week) VALUES (%s,%s,%s,%s,%s,%s)",
            [tuple(r) for r in dim_time.collect()]
        )
        dim_time_jdbc = read_jdbc(spark, "dim_time")

        # BRAND
        brands = df.select(trim("brands").alias("brand_name")).filter("length(brand_name)>0").distinct()
        insert_ignore("INSERT IGNORE INTO dim_brand (brand_name) VALUES (%s)", [(r.brand_name,) for r in brands.collect()])
        dim_brand_jdbc = read_jdbc(spark, "dim_brand")

        # CATEGORY
        categories = df.select(explode(split("categories_tags", ",")).alias("cat")).distinct()
        insert_ignore("INSERT IGNORE INTO dim_category (category_code, category_name_fr, level) VALUES (%s,%s,1)",
                      [(r.cat, r.cat) for r in categories.collect()])
        dim_category_jdbc = read_jdbc(spark, "dim_category")

        # COUNTRY
        countries = df.select(explode(split("countries_tags", ",")).alias("ctry")).distinct()
        insert_ignore("INSERT IGNORE INTO dim_country (country_code) VALUES (%s)",
                      [(r.ctry,) for r in countries.collect()])
        dim_country_jdbc = read_jdbc(spark, "dim_country")

        # ----------------------------------------------------------------
        # C. SCD2 PRODUCT
        # ----------------------------------------------------------------
        print("--- [2/4] Processing SCD2 Product ---")
        
        enriched = df.join(dim_brand_jdbc, trim(df.brands) == dim_brand_jdbc.brand_name, "left") \
                     .join(dim_time_jdbc, to_date(df.effective_from) == dim_time_jdbc.date, "left")

        # === FIX SCD2 ===
        # Force Nutriscore a être 'a','b','c','d','e' ou NULL (1 seul char)
        nutri_clean = when(col("nutriscore_grade").isin("a","b","c","d","e"), col("nutriscore_grade")).otherwise(lit(None))

        df_new = enriched.select(
            "code",
            substring("product_name", 1, 255).alias("product_name"),
            "brand_sk",
            to_json(expr("transform(split(countries_tags, ','), x -> trim(x))")).alias("countries_multi"),
            "effective_from",
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            "hash_diff",
            "quality_issues_json", "completeness_score",
            nutri_clean.alias("nutriscore_grade"), 
            "nova_group",
            "energy_kcal_100g", "fat_100g", "sugars_100g", "salt_100g", "proteins_100g", "fiber_100g"
        ).distinct()

        df_old = read_jdbc(spark, "dim_product")
        
        merge = df_new.alias("n").join(df_old.alias("o"), "code", "left")

        # Close old
        to_close = merge.filter(
            (col("o.hash_diff").isNotNull()) &
            (col("n.hash_diff") != col("o.hash_diff")) &
            (col("o.is_current") == True)
        ).select("o.product_sk").collect()

        if to_close:
            print(f"⚠️ SCD2 Update: Closing {len(to_close)} rows.")
            ids = [str(r.product_sk) for r in to_close]
            chunk_size = 1000
            for i in range(0, len(ids), chunk_size):
                chunk_ids = ",".join(ids[i:i+chunk_size])
                sql = f"UPDATE dim_product SET is_current=0, effective_to=NOW() WHERE product_sk IN ({chunk_ids})"
                conn = mysql_conn()
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                cur.close()
                conn.close()

        # Insert new
        cols_write = ["code", "product_name", "brand_sk", "countries_multi", "effective_from", "effective_to", 
                      "is_current", "hash_diff", "quality_issues_json", "completeness_score", 
                      "nutriscore_grade", "nova_group", "energy_kcal_100g", "fat_100g", "sugars_100g", 
                      "salt_100g", "proteins_100g", "fiber_100g"]
        
        to_insert = merge.filter(
            (col("o.hash_diff").isNull()) | (col("n.hash_diff") != col("o.hash_diff"))
        ).select(*[col(f"n.{c}").alias(c) for c in cols_write])

        write_jdbc(to_insert, "dim_product")
        
        # ----------------------------------------------------------------
        # D. FACT TABLE (LE FIX STRICT EST ICI)
        # ----------------------------------------------------------------
        print("--- [3/4] Fact Table ---")
        
        dim_prod_fresh = read_jdbc(spark, "dim_product").select("product_sk", "hash_diff")

        # === FIX CRITIQUE FACT TABLE ===
        # Si nutriscore n'est pas dans [a,b,c,d,e], on envoie NULL.
        # Cela garantit que ça rentre dans un CHAR(1).
        nutri_strict_filter = when(F.lower(col("nutriscore_grade")).isin(["a", "b", "c", "d", "e"]), F.lower(col("nutriscore_grade"))).otherwise(lit(None))

        fact = enriched.join(dim_prod_fresh, "hash_diff", "inner") \
            .select(
                "product_sk", "time_sk",
                col("energy_kcal_100g"), col("fat_100g"), col("sugars_100g"),
                col("proteins_100g"), col("salt_100g"), col("fiber_100g"),
                
                # ICI : ON FORCE LA COMPATIBILITÉ CHAR(1)
                nutri_strict_filter.alias("nutriscore_grade"),
                
                "nova_group", 
                lit(None).cast("string").alias("ecoscore_grade"),
                "completeness_score", "quality_issues_json"
            ).dropDuplicates(["product_sk", "time_sk"])
        
        write_jdbc(fact, "fact_nutrition_snapshot")

        # ----------------------------------------------------------------
        # E. BRIDGE TABLES
        # ----------------------------------------------------------------
        print("--- [4/4] Bridge Tables ---")
        
        b_cat = df.select("hash_diff", explode(split("categories_tags", ",")).alias("cat")) \
            .join(dim_prod_fresh, "hash_diff") \
            .join(dim_category_jdbc, col("cat") == dim_category_jdbc.category_code) \
            .select("product_sk", "category_sk").distinct()
        write_jdbc(b_cat, "bridge_product_category", "append")

        b_ctry = df.select("hash_diff", explode(split("countries_tags", ",")).alias("ctry")) \
            .join(dim_prod_fresh, "hash_diff") \
            .join(dim_country_jdbc, col("ctry") == dim_country_jdbc.country_code) \
            .select("product_sk", "country_sk").distinct()
        write_jdbc(b_ctry, "bridge_product_country", "append")

        # ----------------------------------------------------------------
        # F. METRICS
        # ----------------------------------------------------------------
        metrics = {
            "run_date": datetime.now().isoformat(),
            "rows_fact": fact.count(),
            "duration": round(time.time() - start, 2)
        }
        os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
        with open(METRICS_PATH, "a") as f:
            f.write(json.dumps(metrics) + "\n")

        print(f"\n✅ GOLD terminé avec succès en {metrics['duration']} sec ! 🏆")

    except Exception as e:
        print(f"\n❌ ERREUR GOLD: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()