import mysql.connector

DB_CONFIG = {
    'host': 'localhost',
    'port': 3309, # Ton port
    'user': 'root',
    'password': 'root',
    'database': 'off_datamart'
}

def reset():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    tables = [
        "fact_nutrition_snapshot",
        "bridge_product_category",
        "bridge_product_country",
        "dim_product",
        "dim_brand",
        "dim_category",
        "dim_country",
        "dim_time"
    ]
    
    print("🔥 DESTRUCTION DES DONNÉES (RESET)...")
    try:
        # On désactive les clés étrangères pour pouvoir vider dans n'importe quel ordre
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        for t in tables:
            print(f"   - Vidage de {t}...")
            cursor.execute(f"TRUNCATE TABLE {t};")
            
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        print("✅ Base de données propre et vide !")
        
    except Exception as e:
        print(f"❌ Erreur : {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    reset()