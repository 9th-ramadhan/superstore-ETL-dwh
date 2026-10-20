# Superstore Relational OLTP to Star Schema DWH ETL Pipeline

Proyek Data Engineering untuk memproses dataset transaksi Superstore dari sistem sumber **MySQL OLTP Ternormalisasi (3NF)** menuju **Data Warehouse (Star Schema)** menggunakan Python, Pandas, SQLAlchemy, dan MySQL.

---

## 🏛️ Arsitektur Data End-to-End

```
                      [ data_source/superstore_dirty.csv ] (Dataset Awal)
                                      │
                                      ▼ (seed_oltp.py / Ingestion Portabel)
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                 DATABASE SUMBER: MySQL OLTP (superstore_oltp)               │
  │                                                                             │
  │   [categories] ──< [subcategories] ──< [products]                           │
  │                                           │                                 │
  │   [customers]  ──────< [orders]           │                                 │
  │   [ship_modes] ──────<    │               ▼                                 │
  │   [locations]  ──────<    └───< [order_items] (10.093 Line Items)           │
  │         │                            ▲                                      │
  │         └────────────────────────────┘ (Alamat per line item)               │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ (Extract: SELECT / JOIN 8 Tabel)
                           [ Python / Pandas ETL ]
                        (Pembersihan Anomali Bisnis)
                                      │
                                      ▼ (Transform: Modeling Star Schema)
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │              DATABASE TUJUAN: MySQL Data Warehouse (superstore_dwh)         │
  │                                                                             │
  │    Tabel Dimensi:                                                           │
  │    • dim_customer  • dim_product  • dim_location  • dim_ship_mode           │
  │    • dim_date                                                               │
  │                                                                             │
  │    Tabel Fakta:                                                             │
  │    • fact_sales          (Grain: Line Item Penjualan - 9.894 baris)         │
  │    • fact_customer_order (Grain: Header Order ID    - 4.985 baris)         │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
            [MySQL DWH: superstore_dwh]      [CSV: dwh_output/]
```

---

## 📐 Desain Normalisasi Database OLTP (`superstore_oltp`)

Database OLTP dirancang mengikuti prinsip normalisasi relasional (mendekati 3NF) dengan memperhatikan **Functional Dependency** data aktual:

1. **`categories`** (Master Kategori):
   - `category_id` (PK), `category_name` (UNIQUE).
2. **`subcategories`** (Master Subkategori):
   - `subcategory_id` (PK), `category_id` (FK ke `categories`), `subcategory_name` (UNIQUE).
3. **`products`** (Master Katalog Produk):
   - `product_id` (PK), `subcategory_id` (FK ke `subcategories`), `product_name`.
4. **`customers`** (Master Profil Pelanggan):
   - `customer_id` (PK), `customer_name`, `segment`.
5. **`ship_modes`** (Master Metode Logistik):
   - `ship_mode_id` (PK), `ship_mode_name` (UNIQUE).
6. **`locations`** (Master Wilayah Geografis):
   - `location_id` (PK), `country`, `region`, `state`, `city`, `postal_code` (menampung nilai NULL dan kode pos mentah).
7. **`orders`** (Header Transaksi Pesanan):
   - `order_id` (PK), `customer_id` (FK), `order_date`, `ship_mode_id` (FK), `location_id` (FK).
8. **`order_items`** (Detail Transaksi / Line Items):
   - `order_item_id` (PK Surrogate Auto-Increment agar 99 duplikat Row ID tetap tersimpan).
   - `row_id`, `order_id` (FK), `product_id` (FK), `location_id` (FK).
   - **Transaction Snapshot**: `ship_date`, `customer_name`, `product_name`, `sales`, `quantity`, `discount`, `profit`.
   - *Alasan*: Menampung anomali tanggal pengiriman per item (tanggal terbalik), anomali nama produk/pelanggan yang tidak tercatat (NULL), serta kuantitas invalid ($\le 0$) tanpa dibersihkan prematur di database sumber.

---

## 🚀 Panduan Setup & Menjalankan Pipeline

### 1. Prasyarat Sistem
- **Python**: Versi 3.10 atau lebih baru.
- **MySQL Server**: Versi 8.0 atau MariaDB (misalnya via Laragon, XAMPP, atau MySQL Community) pada port `3306`.

### 2. Instalasi Dependensi Python
```bash
pip install -r requirements.txt
```

### 3. Konfigurasi Lingkungan (`.env`)
Salin file template `.env.example` menjadi `.env`:
```bash
copy .env.example .env
```
Isi konfigurasi database:
```ini
# OLTP Database (Sumber)
OLTP_DB_HOST=localhost
OLTP_DB_PORT=3306
OLTP_DB_USER=root
OLTP_DB_PASSWORD=
OLTP_DB_NAME=superstore_oltp

# DWH Database (Tujuan)
DWH_DB_HOST=localhost
DWH_DB_PORT=3306
DWH_DB_USER=root
DWH_DB_PASSWORD=
DWH_DB_NAME=superstore_dwh

LOAD_TO_MYSQL=true
```

### 4. Siapkan Database OLTP & Impor Dataset CSV ke MySQL OLTP
Jalankan skrip seeder relasional untuk membuat 8 tabel dan mengimpor 10.093 record mentah:
```bash
python seed_oltp.py
```
*(Atau eksekusi file DDL `sql/oltp_schema.sql` di MySQL client sebelum seeding).*

### 5. Siapkan Database DWH Tujuan
Jalankan skrip DDL DWH untuk membuat database `superstore_dwh` beserta skema Star Schema dan indeks relasionalnya:
```sql
SOURCE sql/dwh_schema.sql;
```

### 6. Jalankan Pipeline ETL Utama (Extract dari MySQL OLTP)
```bash
python etl_pipeline.py
```
Atau menggunakan runner otomatis Windows:
```cmd
run_etl.bat
```

Pipeline akan mengeksekusi:
1. **Extract**: Menjalankan query SQL relasional JOIN 8 tabel pada database `superstore_oltp` dan memvalidasi tipe data/kolom.
2. **Transform (Clean)**: Menangani 95 duplikat identik, 4 duplikat Row ID, 199 imputasi nama pelanggan/produk/postal code, swap 100 tanggal terbalik, dan drop 100 kuantitas invalid.
3. **Transform (Model)**: Menghasilkan surrogate keys untuk 5 tabel dimensi dan 2 tabel fakta.
4. **Load**: Memuat hasil ke `superstore_dwh` dan mengekspor CSV ke `dwh_output/`.

### 7. Verifikasi Tabel Fakta, Dimensi, dan Kualitas Data
Jalankan script audit:
```bash
python audit.py
```
Ringkasan laporan tersimpan di direktori `audit_report/`.

### 8. Menjalankan Automated Test Suite
Untuk memvalidasi integritas relasi OLTP, preservasi nilai kotor, dan grain hasil JOIN:
```bash
python test_oltp_pipeline.py
```

---

## 📈 Baseline Kualitas Data (Hasil Audit)

| Tabel / Tahapan | Baris Mentah | Baris Bersih | Total Sel NULL | Duplikat Dihapus | Anomali Tanggal Ditangani |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **MySQL OLTP: order_items** | 10.093 | - | 600 | 99 baris | 100 tanggal terbalik |
| **Hasil Extract (JOIN)** | 10.093 | - | 600 | 99 baris | 100 tanggal terbalik |
| **Setelah Transform Clean** | 10.093 | 9.894 | 0 | 99 baris | 100 baris di-swap |
| **DWH: dim_customer** | - | 793 | 0 | - | - |
| **DWH: dim_product** | - | 1.861 | 0 | - | - |
| **DWH: dim_location** | - | 1.651 | 0 | - | - |
| **DWH: dim_ship_mode** | - | 4 | 0 | - | - |
| **DWH: dim_date** | - | 1.434 | 0 | - | - |
| **DWH: fact_sales** | - | 9.894 | 0 | - | - |
| **DWH: fact_customer_order** | - | 4.985 | 0 | - | - |
