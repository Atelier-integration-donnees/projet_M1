# ==============================================================================
# SCRIPT DE TEST AUTOMATISÉ - GOLD LAYER VALIDATION
# ==============================================================================
import mysql.connector
import sys

# CONFIGURATION
DB_CONFIG = {
    'host': 'localhost',
    'port': 3309,  # Ton port (3308 ou 3309)
    'user': 'root',
    'password': 'root',
    'database': 'off_datamart'
}

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def run_test(test_name, query, expected_condition, error_msg):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        
        # Si le test est une validation de COUNT > 0
        if expected_condition == "GT_0":
            val = result[0]
            if val > 0:
                print(f"✅ {test_name}: PASS ({val} lignes)")
            else:
                print(f"❌ {test_name}: FAIL (Table vide)")
        
        # Si le test cherche des erreurs (doit être égal à 0)
        elif expected_condition == "EQ_0":
            val = result[0]
            if val == 0:
                print(f"✅ {test_name}: PASS (0 erreur)")
            else:
                print(f"❌ {test_name}: FAIL ({val} erreurs trouvées) -> {error_msg}")
        
        # Si le test compare deux valeurs (Intégrité référentielle)
        elif expected_condition == "INTEGRITY":
            val1, val2 = result
            if val1 == val2:
                print(f"✅ {test_name}: PASS ({val1} == {val2})")
            else:
                print(f"⚠️ {test_name}: WARNING ({val1} faits vs {val2} produits actifs)")

    except Exception as e:
        print(f"❌ {test_name}: CRASH ({e})")
    finally:
        cursor.close()
        conn.close()

def main():
    print("\n" + "="*50)
    print("🛡️  DÉMARRAGE DES TESTS DE QUALITÉ DONNÉES")
    print("="*50)

    # 1. TEST VOLUMÉTRIE
    run_test("Volumétrie Produits", "SELECT COUNT(*) FROM dim_product", "GT_0", "")
    run_test("Volumétrie Faits", "SELECT COUNT(*) FROM fact_nutrition_snapshot", "GT_0", "")

    # 2. TEST QUALITÉ NUTRISCORE (Le bug qu'on vient de corriger)
    # On vérifie qu'il n'y a AUCUNE ligne qui dépasse 1 caractère ou qui n'est pas a,b,c,d,e,NULL
    sql_nutri = """
        SELECT COUNT(*) FROM dim_product 
        WHERE LENGTH(nutriscore_grade) > 1 
        OR (nutriscore_grade IS NOT NULL AND nutriscore_grade NOT IN ('a','b','c','d','e'))
    """
    run_test("Qualité Nutri-Score", sql_nutri, "EQ_0", "Il reste des valeurs 'unknown' ou > 1 char !")

    # 3. TEST LOGIQUE SCD2 (Unicité)
    # Vérifier qu'un code produit n'a pas 2 lignes actives en même temps
    sql_scd2 = """
        SELECT COUNT(*) FROM (
            SELECT code FROM dim_product 
            WHERE is_current = 1 
            GROUP BY code 
            HAVING COUNT(*) > 1
        ) sub
    """
    run_test("Unicité SCD2 (Actifs)", sql_scd2, "EQ_0", "Des produits ont plusieurs lignes actives simultanément !")

    # 4. TEST INTÉGRITÉ (Orphelins)
    # Est-ce que tous les faits sont liés à un produit qui existe ?
    sql_orphans = """
        SELECT COUNT(*) FROM fact_nutrition_snapshot f
        LEFT JOIN dim_product p ON f.product_sk = p.product_sk
        WHERE p.product_sk IS NULL
    """
    run_test("Intégrité Faits -> Produits", sql_orphans, "EQ_0", "Des lignes de faits n'ont pas de produit parent !")

    # 5. TEST BRIDGE TABLES
    run_test("Bridge Catégories", "SELECT COUNT(*) FROM bridge_product_category", "GT_0", "")
    run_test("Bridge Pays", "SELECT COUNT(*) FROM bridge_product_country", "GT_0", "")

    print("-" * 50)
    print("🏁 Fin des tests.\n")

if __name__ == "__main__":
    main()