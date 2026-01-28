import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_PATH = os.path.join(BASE_DIR, "data/silver")
WAREHOUSE_PATH = os.path.join(BASE_DIR, "spark-warehouse")
METRICS_PATH = os.path.join(BASE_DIR, "logs/gold_metrics.json")
JAR_PATH = os.path.join(BASE_DIR, "jars/mysql-connector-j-8.2.0.jar")

DB_HOST, DB_PORT = "localhost", "3309"
DB_NAME, DB_USER, DB_PASSWORD = "off_datamart", "root", "root"
JDBC_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?useSSL=false"
