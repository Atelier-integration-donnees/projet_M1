-- ===============================================================
-- DDL DATAMART OPENFOODFACTS — GOLD LAYER (MySQL)
-- ===============================================================

-- ----------------------------------------------------------------
-- DIM_TIME
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_time (
    time_sk INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INT,
    month INT,
    day INT,
    week INT,
    iso_week INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------
-- DIM_BRAND
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_brand (
    brand_sk INT AUTO_INCREMENT PRIMARY KEY,
    brand_name VARCHAR(255) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------
-- DIM_CATEGORY
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_category (
    category_sk INT AUTO_INCREMENT PRIMARY KEY,
    category_code VARCHAR(255) NOT NULL UNIQUE,
    category_name_fr VARCHAR(255),
    level INT,
    parent_category_sk INT DEFAULT NULL,
    FOREIGN KEY (parent_category_sk) REFERENCES dim_category(category_sk)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------
-- DIM_COUNTRY
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_sk INT AUTO_INCREMENT PRIMARY KEY,
    country_code VARCHAR(50) NOT NULL UNIQUE,
    country_name_fr VARCHAR(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------
-- DIM_PRODUCT (SCD2)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255),
    brand_sk INT,
    effective_from DATETIME,
    effective_to DATETIME DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    hash_diff VARCHAR(64),
    countries_multi JSON DEFAULT NULL,
    FOREIGN KEY (brand_sk) REFERENCES dim_brand(brand_sk)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------
-- FACT_NUTRITION_SNAPSHOT
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    product_sk INT NOT NULL,
    time_sk INT NOT NULL,
    energy_kcal_100g FLOAT,
    fat_100g FLOAT,
    saturated_fat_100g FLOAT,
    sugars_100g FLOAT,
    salt_100g FLOAT,
    proteins_100g FLOAT,
    fiber_100g FLOAT,
    sodium_100g FLOAT,
    nutriscore_grade CHAR(1),
    nova_group INT,
    ecoscore_grade CHAR(1),
    completeness_score FLOAT,
    quality_issues_json JSON DEFAULT NULL,
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk)
        ON DELETE CASCADE,
    FOREIGN KEY (time_sk) REFERENCES dim_time(time_sk)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------
-- BRIDGE PRODUCT ↔ CATEGORY
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_product_category (
    product_sk INT NOT NULL,
    category_sk INT NOT NULL,
    PRIMARY KEY (product_sk, category_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk)
        ON DELETE CASCADE,
    FOREIGN KEY (category_sk) REFERENCES dim_category(category_sk)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------
-- BRIDGE PRODUCT ↔ COUNTRY
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_product_country (
    product_sk INT NOT NULL,
    country_sk INT NOT NULL,
    PRIMARY KEY (product_sk, country_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk)
        ON DELETE CASCADE,
    FOREIGN KEY (country_sk) REFERENCES dim_country(country_sk)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;






ADD COLUMN quality_issues_json JSON DEFAULT NULL,
    -> ADD COLUMN completeness_score FLOAT DEFAULT NULL,
    -> ADD COLUMN nutriscore_grade CHAR(1) DEFAULT NULL,
    -> ADD COLUMN nova_group INT DEFAULT NULL,
    -> ADD COLUMN energy_kcal_100g FLOAT DEFAULT NULL,
    -> ADD COLUMN fat_100g FLOAT DEFAULT NULL,
    -> ADD COLUMN sugars_100g FLOAT DEFAULT NULL,
    -> ADD COLUMN salt_100g FLOAT DEFAULT NULL,
    -> ADD COLUMN proteins_100g FLOAT DEFAULT NULL,
    -> ADD COLUMN fiber_100g FLOAT DEFAULT NULL;