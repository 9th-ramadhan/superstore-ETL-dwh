-- ==============================================================================
-- DDL & DML SCHEMA DATA WAREHOUSE (STAR SCHEMA)
-- Superstore Sales Data Warehouse
-- Dialek: ANSI SQL / PostgreSQL Compatible (Dapat diadaptasi ke MySQL / SQL Server)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. DROP EXISTING TABLES (Urutan: Tabel Fakta terlebih dahulu, lalu Tabel Dimensi)
-- ------------------------------------------------------------------------------
create database if not exists superstore_dwh;
use superstore_dwh;

DROP TABLE IF EXISTS fact_customer_order CASCADE;
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_ship_mode CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;

-- ------------------------------------------------------------------------------
-- 2. TABEL-TABEL DIMENSI
-- ------------------------------------------------------------------------------

-- A. Dimensi Customer (dim_customer)
CREATE TABLE dim_customer (
    customer_key    INT NOT NULL,
    customer_id     VARCHAR(50) NOT NULL,
    customer_name   VARCHAR(255) NOT NULL,
    segment         VARCHAR(50) NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key),
    CONSTRAINT uq_dim_customer_id UNIQUE (customer_id)
);

COMMENT ON TABLE dim_customer IS 'Tabel dimensi yang menyimpan profil dan segmentasi pelanggan.';
COMMENT ON COLUMN dim_customer.customer_key IS 'Surrogate Key (Primary Key) bertipe integer berurut.';
COMMENT ON COLUMN dim_customer.customer_id IS 'Natural/Business Key pelanggan dari sistem sumber.';

-- B. Dimensi Produk (dim_product)
CREATE TABLE dim_product (
    product_key     INT NOT NULL,
    product_id      VARCHAR(50) NOT NULL,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    sub_category    VARCHAR(100) NOT NULL,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key),
    CONSTRAINT uq_dim_product_id UNIQUE (product_id)
);

COMMENT ON TABLE dim_product IS 'Tabel dimensi katalog produk dan hierarki kategorinya.';
COMMENT ON COLUMN dim_product.product_key IS 'Surrogate Key (Primary Key) bertipe integer berurut.';
COMMENT ON COLUMN dim_product.product_id IS 'Natural/Business Key produk dari sistem sumber.';

-- C. Dimensi Lokasi (dim_location)
CREATE TABLE dim_location (
    location_key    INT NOT NULL,
    country         VARCHAR(100) NOT NULL,
    region          VARCHAR(50) NOT NULL,
    state           VARCHAR(100) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    postal_code     VARCHAR(20) NOT NULL,
    CONSTRAINT pk_dim_location PRIMARY KEY (location_key)
);

COMMENT ON TABLE dim_location IS 'Tabel dimensi wilayah geografis dan alamat pengiriman.';
COMMENT ON COLUMN dim_location.location_key IS 'Surrogate Key (Primary Key) bertipe integer berurut.';

-- D. Dimensi Moda Pengiriman (dim_ship_mode)
CREATE TABLE dim_ship_mode (
    ship_mode_key   INT NOT NULL,
    ship_mode       VARCHAR(50) NOT NULL,
    CONSTRAINT pk_dim_ship_mode PRIMARY KEY (ship_mode_key),
    CONSTRAINT uq_dim_ship_mode UNIQUE (ship_mode)
);

COMMENT ON TABLE dim_ship_mode IS 'Tabel dimensi jenis/metode logistik pengiriman barang.';
COMMENT ON COLUMN dim_ship_mode.ship_mode_key IS 'Surrogate Key (Primary Key) bertipe integer berurut.';

-- E. Dimensi Waktu / Kalender (dim_date)
CREATE TABLE dim_date (
    date_key        INT NOT NULL,             -- Format: YYYYMMDD (contoh: 20161108)
    full_date       DATE NOT NULL,
    day             INT NOT NULL,
    month           INT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    quarter         INT NOT NULL,
    year            INT NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

COMMENT ON TABLE dim_date IS 'Tabel dimensi kalender untuk analisis berbasis deret waktu.';
COMMENT ON COLUMN dim_date.date_key IS 'Surrogate Key bertipe integer dengan format cerdas YYYYMMDD.';

-- ------------------------------------------------------------------------------
-- 3. TABEL-TABEL FAKTA
-- ------------------------------------------------------------------------------

-- F. Tabel Fakta 1: Penjualan per Baris Item (fact_sales)
-- Grain: 1 baris untuk setiap item produk dalam transaksi (Row ID)
CREATE TABLE fact_sales (
    sales_key       BIGINT NOT NULL,
    row_id          VARCHAR(50) NOT NULL,
    order_id        VARCHAR(50) NOT NULL,
    order_date_key  INT NOT NULL,
    ship_date_key   INT NOT NULL,
    customer_key    INT NOT NULL,
    product_key     INT NOT NULL,
    location_key    INT NOT NULL,
    ship_mode_key   INT NOT NULL,
    quantity        INT NOT NULL,
    sales           NUMERIC(12, 4) NOT NULL,
    discount        NUMERIC(5, 4) NOT NULL,
    profit          NUMERIC(12, 4) NOT NULL,
    CONSTRAINT pk_fact_sales PRIMARY KEY (sales_key),
    CONSTRAINT fk_fact_sales_order_date FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_sales_ship_date  FOREIGN KEY (ship_date_key)  REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_sales_customer   FOREIGN KEY (customer_key)   REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_fact_sales_product    FOREIGN KEY (product_key)    REFERENCES dim_product(product_key),
    CONSTRAINT fk_fact_sales_location   FOREIGN KEY (location_key)   REFERENCES dim_location(location_key),
    CONSTRAINT fk_fact_sales_ship_mode  FOREIGN KEY (ship_mode_key)  REFERENCES dim_ship_mode(ship_mode_key)
);

COMMENT ON TABLE fact_sales IS 'Tabel fakta transaksi penjualan pada granularitas baris item produk.';
COMMENT ON COLUMN fact_sales.order_id IS 'Degenerate dimension (nomor pesanan asli).';

-- G. Tabel Fakta 2: Pesanan Pelanggan (fact_customer_order)
-- Grain: 1 baris untuk setiap Order ID (Order Header Summary)
CREATE TABLE fact_customer_order (
    customer_order_key  BIGINT NOT NULL,
    order_id            VARCHAR(50) NOT NULL,
    customer_key        INT NOT NULL,
    order_date_key      INT NOT NULL,
    location_key        INT NOT NULL,
    ship_mode_key       INT NOT NULL,
    total_sales         NUMERIC(14, 4) NOT NULL,
    total_quantity      INT NOT NULL,
    total_profit        NUMERIC(14, 4) NOT NULL,
    total_discount      NUMERIC(8, 4) NOT NULL,
    product_line_count  INT NOT NULL,
    CONSTRAINT pk_fact_customer_order PRIMARY KEY (customer_order_key),
    CONSTRAINT fk_fact_order_customer   FOREIGN KEY (customer_key)   REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_fact_order_order_date FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_order_location   FOREIGN KEY (location_key)   REFERENCES dim_location(location_key),
    CONSTRAINT fk_fact_order_ship_mode  FOREIGN KEY (ship_mode_key)  REFERENCES dim_ship_mode(ship_mode_key)
);

COMMENT ON TABLE fact_customer_order IS 'Tabel fakta pesanan pada granularitas header Order ID.';
COMMENT ON COLUMN fact_customer_order.product_line_count IS 'Jumlah baris item produk berbeda dalam pesanan.';

-- ------------------------------------------------------------------------------
-- 4. INDEKS UNTUK OPTIMALISASI QUERY ANALITIK (OLAP)
-- ------------------------------------------------------------------------------

-- Indeks Foreign Keys pada fact_sales
CREATE INDEX idx_fact_sales_order_date ON fact_sales(order_date_key);
CREATE INDEX idx_fact_sales_ship_date  ON fact_sales(ship_date_key);
CREATE INDEX idx_fact_sales_customer   ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product    ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_location   ON fact_sales(location_key);
CREATE INDEX idx_fact_sales_ship_mode  ON fact_sales(ship_mode_key);
CREATE INDEX idx_fact_sales_order_id   ON fact_sales(order_id);

-- Indeks Foreign Keys pada fact_customer_order
CREATE INDEX idx_fact_order_customer   ON fact_customer_order(customer_key);
CREATE INDEX idx_fact_order_order_date ON fact_customer_order(order_date_key);
CREATE INDEX idx_fact_order_location   ON fact_customer_order(location_key);
CREATE INDEX idx_fact_order_ship_mode  ON fact_customer_order(ship_mode_key);
CREATE INDEX idx_fact_order_order_id   ON fact_customer_order(order_id);

-- ------------------------------------------------------------------------------
-- 5. CONTOH SINTAKS LOAD DATA DARI CSV KE DATABASE
-- ------------------------------------------------------------------------------
/*
-- [Opsi A: PostgreSQL (via psql \copy)]
\copy dim_customer FROM 'dwh_output/dim_customer.csv' WITH (FORMAT csv, HEADER true);
\copy dim_product FROM 'dwh_output/dim_product.csv' WITH (FORMAT csv, HEADER true);
\copy dim_location FROM 'dwh_output/dim_location.csv' WITH (FORMAT csv, HEADER true);
\copy dim_ship_mode FROM 'dwh_output/dim_ship_mode.csv' WITH (FORMAT csv, HEADER true);
\copy dim_date FROM 'dwh_output/dim_date.csv' WITH (FORMAT csv, HEADER true);
\copy fact_sales FROM 'dwh_output/fact_sales.csv' WITH (FORMAT csv, HEADER true);
\copy fact_customer_order FROM 'dwh_output/fact_customer_order.csv' WITH (FORMAT csv, HEADER true);

-- [Opsi B: MySQL (via LOAD DATA INFILE)]
LOAD DATA LOCAL INFILE 'dwh_output/dim_customer.csv' INTO TABLE dim_customer FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'dwh_output/dim_product.csv' INTO TABLE dim_product FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'dwh_output/dim_location.csv' INTO TABLE dim_location FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'dwh_output/dim_ship_mode.csv' INTO TABLE dim_ship_mode FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'dwh_output/dim_date.csv' INTO TABLE dim_date FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'dwh_output/fact_sales.csv' INTO TABLE fact_sales FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'dwh_output/fact_customer_order.csv' INTO TABLE fact_customer_order FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
*/

-- ------------------------------------------------------------------------------
-- 6. CONTOH QUERY ANALITIK VERIFIKASI (STAR SCHEMA JOINS)
-- ------------------------------------------------------------------------------
/*
-- Contoh 1: Performa Penjualan & Profit per Kategori Produk dan Tahun
SELECT 
    d.year,
    p.category,
    COUNT(f.sales_key) AS total_item_terjual,
    ROUND(SUM(f.sales), 2) AS total_revenue,
    ROUND(SUM(f.profit), 2) AS total_profit
FROM fact_sales f
JOIN dim_date d ON f.order_date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY d.year, p.category
ORDER BY d.year DESC, total_revenue DESC;

-- Contoh 2: Analisis Rata-Rata Nilai Pesanan (AOV) per Segmen Pelanggan
SELECT 
    c.segment,
    COUNT(o.customer_order_key) AS total_orders,
    ROUND(SUM(o.total_sales), 2) AS total_sales_volume,
    ROUND(AVG(o.total_sales), 2) AS average_order_value,
    ROUND(AVG(o.product_line_count), 2) AS avg_items_per_order
FROM fact_customer_order o
JOIN dim_customer c ON o.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY total_sales_volume DESC;
*/
