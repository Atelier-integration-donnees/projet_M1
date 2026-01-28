import sys
import os
import shutil
from pyspark.sql import SparkSession, functions as F

# --------------------------------------------------------------------
# Configuration Spark
# --------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Test_Bronze_Expert") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# --------------------------------------------------------------------
# Chemins
# --------------------------------------------------------------------
# On remonte d'un niveau pour trouver le dossier data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze")

print(f"\n{'='*60}")
print(f">>> TEST AUTOMATISÉ : COUCHE BRONZE (EXPERT)")
print(f"{'='*60}")

# --------------------------------------------------------------------
# Lecture Parquet Bronze
# --------------------------------------------------------------------
try:
    if not os.path.exists(BRONZE_PATH):
        raise Exception(f"Dossier introuvable : {BRONZE_PATH}")
        
    df = spark.read.parquet(BRONZE_PATH)
    print(f"✅ [INIT] Lecture Parquet réussie")
except Exception as e:
    print(f"❌ [INIT] Erreur lecture Parquet : {e}")
    sys.exit(1)

# --------------------------------------------------------------------
# Test 1 : Colonnes clés présentes (Mise à jour Expert)
# --------------------------------------------------------------------
expected_cols = [
    "code", 
    "product_name", 
    "product_name_fr", # Ajouté
    "product_name_en", # Ajouté (Expert)
    "brands", 
    "categories_tags", 
    "countries_tags",
    "main_category", 
    "last_modified_t", 
    "created_t", 
    "nutriscore_grade",
    "nova_group", 
    "completeness_score", 
    "energy_kcal_100g", 
    "energy_kj_100g",  # Ajouté (Expert)
    "sugars_100g",
    "fat_100g", 
    "saturated_fat_100g", 
    "salt_100g", 
    "sodium_100g",     # Ajouté (Expert)
    "proteins_100g"
]

missing_cols = [col for col in expected_cols if col not in df.columns]

if missing_cols:
    print(f"❌ [TEST 1] Colonnes manquantes : {missing_cols}")
    print("   -> Avez-vous bien relancé '1_ingest_bronze.py' avec la version Expert ?")
else:
    print(f"✅ [TEST 1] Les 21 colonnes (y compris Expert) sont présentes.")

# --------------------------------------------------------------------
# Test 2 : Nulls sur colonnes critiques
# --------------------------------------------------------------------
null_count = df.filter(F.col("code").isNull() | F.col("product_name").isNull()).count()
if null_count > 0:
    print(f"❌ [TEST 2] {null_count} lignes ont des code ou product_name null")
else:
    print(f"✅ [TEST 2] Identifiants produits OK (Pas de Nulls sur Code/Nom)")

# --------------------------------------------------------------------
# Test 3 : Types Spark corrects
# --------------------------------------------------------------------
schema_dict = dict(df.dtypes)
type_checks = {
    "code": "string",
    "product_name": "string",
    "product_name_fr": "string",
    "product_name_en": "string", # Vérification du type ajouté
    "brands": "string",
    "nutriscore_grade": "string",
    "nova_group": "int",
    "completeness_score": "float",
    "energy_kcal_100g": "float",
    "energy_kj_100g": "float",   # Vérification du type ajouté
    "sugars_100g": "float",
    "fat_100g": "float",
    "saturated_fat_100g": "float",
    "salt_100g": "float",
    "sodium_100g": "float",      # Vérification du type ajouté
    "proteins_100g": "float"
}

type_errors = []
for col, expected_type in type_checks.items():
    if col in schema_dict:
        actual_type = schema_dict[col]
        # On accepte "double" pour "float" car Spark convertit parfois
        if not actual_type.startswith(expected_type) and not (expected_type=="float" and actual_type=="double"):
            type_errors.append(f"{col} (Attendu: {expected_type}, Reçu: {actual_type})")

if type_errors:
    print(f"❌ [TEST 3] Erreurs de typage : {type_errors}")
else:
    print(f"✅ [TEST 3] Tous les types de données sont valides.")

# --------------------------------------------------------------------
# Test 4 : Comptage lignes
# --------------------------------------------------------------------
total_lines = df.count()
print(f"ℹ️ [INFO] Total lignes Bronze : {total_lines}")
if total_lines > 0:
    print(f"✅ [TEST 4] Le fichier n'est pas vide.")
else:
    print(f"❌ [TEST 4] Le fichier est vide !")

# --------------------------------------------------------------------
# Test 5 : Vérification Bonus (Anglais & KJ)
# --------------------------------------------------------------------
# On vérifie s'il y a au moins une valeur non-nulle dans les nouvelles colonnes
# pour être sûr qu'on n'a pas ingéré que du vide.
bonus_check = df.select(
    F.max(F.col("product_name_en").isNotNull().cast("int")).alias("has_en"),
    F.max(F.col("energy_kj_100g").isNotNull().cast("int")).alias("has_kj")
).collect()[0]

if bonus_check["has_en"] == 1 and bonus_check["has_kj"] == 1:
    print(f"✅ [TEST 5] Données Bonus détectées (Noms anglais et KJ présents).")
else:
    print(f"⚠️ [TEST 5] Attention : Les colonnes Bonus existent mais semblent vides.")

# --------------------------------------------------------------------
# AJOUT : AFFICHAGE DU TABLEAU (5 LIGNES)
# --------------------------------------------------------------------
print("\n" + "="*50)
print(">>> APERÇU DES DONNÉES (5 PREMIÈRES LIGNES)")
print("="*50)

# Affiche les 5 premières lignes dans le terminal
df.show(5, truncate=True)

print("\n🎯 Fin des tests Bronze.")
spark.stop()