-- ==============================================================================
-- DDL SKEMA DATABASE RELASIONAL MYSQL OLTP (NORMALISASI 3NF DENGAN TRANSACTION SNAPSHOT)
-- Database: superstore_oltp
-- Dialek: MySQL 8.x Compatible
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS superstore_oltp
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE superstore_oltp;

-- ------------------------------------------------------------------------------
-- 1. DROP EXISTING TABLES (Urutan: Child table / transaksi terlebih dahulu)
-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS subcategories;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS ship_modes;

-- ------------------------------------------------------------------------------
-- 2. TABEL-TABEL MASTER / ENTITAS TERNORMALISASI (3NF)
-- ------------------------------------------------------------------------------

-- A. Master Kategori Produk
CREATE TABLE categories (
    category_id       INT AUTO_INCREMENT PRIMARY KEY,
    category_name     VARCHAR(100) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- B. Master Subkategori Produk (Relasi Many-to-One ke Categories)
CREATE TABLE subcategories (
    subcategory_id    INT AUTO_INCREMENT PRIMARY KEY,
    category_id       INT NOT NULL,
    subcategory_name  VARCHAR(100) NOT NULL UNIQUE,
    CONSTRAINT fk_subcategories_category FOREIGN KEY (category_id) REFERENCES categories(category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- C. Master Katalog Produk (Relasi Many-to-One ke Subcategories)
CREATE TABLE products (
    product_id        VARCHAR(50) NOT NULL PRIMARY KEY,
    subcategory_id    INT NOT NULL,
    product_name      VARCHAR(255) NULL,
    CONSTRAINT fk_products_subcategory FOREIGN KEY (subcategory_id) REFERENCES subcategories(subcategory_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- D. Master Pelanggan
CREATE TABLE customers (
    customer_id       VARCHAR(50) NOT NULL PRIMARY KEY,
    customer_name     VARCHAR(255) NOT NULL,
    segment           VARCHAR(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- E. Master Moda Pengiriman (Logistik)
CREATE TABLE ship_modes (
    ship_mode_id      INT AUTO_INCREMENT PRIMARY KEY,
    ship_mode_name    VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- F. Master Alamat & Lokasi Geografis
-- Menampung 733 kombinasi lokasi unik (termasuk 101 entri dengan postal_code NULL)
CREATE TABLE locations (
    location_id       INT AUTO_INCREMENT PRIMARY KEY,
    country           VARCHAR(100) NOT NULL,
    region            VARCHAR(50) NOT NULL,
    state             VARCHAR(100) NOT NULL,
    city              VARCHAR(100) NOT NULL,
    postal_code       VARCHAR(50) NULL,
    INDEX idx_locations_geo (country, state, city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------------------------------------------------
-- 3. TABEL-TABEL TRANSAKSI OLTP
-- ------------------------------------------------------------------------------

-- G. Header Pesanan (Orders)
-- Menyimpan atribut level order: pemesan, tanggal pesanan, moda kirim, dan alamat order
CREATE TABLE orders (
    order_id          VARCHAR(50) NOT NULL PRIMARY KEY,
    customer_id       VARCHAR(50) NOT NULL,
    order_date        VARCHAR(50) NOT NULL,
    ship_mode_id      INT NOT NULL,
    location_id       INT NOT NULL,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_orders_ship_mode FOREIGN KEY (ship_mode_id) REFERENCES ship_modes(ship_mode_id),
    CONSTRAINT fk_orders_location FOREIGN KEY (location_id) REFERENCES locations(location_id),
    INDEX idx_orders_customer (customer_id),
    INDEX idx_orders_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- H. Detail Baris Item Transaksi (Order Items)
-- Menyimpan granularitas line item per produk dalam pesanan (10.093 baris transaksi).
-- Menggunakan Surrogate Key order_item_id sebagai PK agar 99 duplikat Row ID tetap tersimpan.
-- Menyimpan transaction snapshot untuk tanggal pengiriman, nama item, dan alamat tujuan item
-- guna mempertahankan nilai kotor (NULL, tanggal terbalik, dsb) sebelum dibersihkan oleh ETL Transform.
CREATE TABLE order_items (
    order_item_id     INT AUTO_INCREMENT PRIMARY KEY,
    row_id            INT NULL,
    order_id          VARCHAR(50) NOT NULL,
    product_id        VARCHAR(50) NOT NULL,
    location_id       INT NOT NULL,
    ship_date         VARCHAR(50) NULL,
    customer_name     VARCHAR(255) NULL,
    product_name      VARCHAR(255) NULL,
    sales             DECIMAL(12, 4) NULL,
    quantity          INT NULL,
    discount          DECIMAL(5, 4) NULL,
    profit            DECIMAL(12, 4) NULL,
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_order_items_location FOREIGN KEY (location_id) REFERENCES locations(location_id),
    INDEX idx_order_items_order (order_id),
    INDEX idx_order_items_product (product_id),
    INDEX idx_order_items_row_id (row_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
