# Panduan Alur Menjalankan Sistem ETL Superstore (OLTP to DWH)

Dokumen ini memuat panduan komprehensif mengenai **alur operasional, diagram alir data, dan langkah-langkah praktis** untuk menjalankan seluruh ekosistem pipeline ETL dari sumber database relasional MySQL OLTP (3NF) ke MySQL Data Warehouse (Star Schema).

---

## 🗺️ 1. Diagram Alur Sistem (End-to-End Flow)

```mermaid
flowchart TD
    subgraph S1["Tahap 1: Inisialisasi Sumber (OLTP Ingestion)"]
        CSV["data_source/superstore_dirty.csv<br/>(10.093 baris transaksi kotor)"]
        SEED["seed_oltp.py<br/>(Inisialisasi Skema & Pemetaan 3NF)"]
        SQL_OLTP["sql/oltp_schema.sql<br/>(DDL 8 Tabel Relasional)"]
        
        CSV --> SEED
        SQL_OLTP -.-> SEED
    end

    subgraph S2["Tahap 2: Database Sumber (MySQL OLTP: superstore_oltp)"]
        SEED --> CAT["categories (3)"]
        SEED --> SUBCAT["subcategories (17)"]
        SEED --> PROD["products (1.862)"]
        SEED --> CUST["customers (793)"]
        SEED --> SM["ship_modes (4)"]
        SEED --> LOC["locations (733)"]
        SEED --> ORD["orders (5.009)"]
        SEED --> ITEMS["order_items (10.093)<br/><i>Snapshot data kotor terpreservasi</i>"]
        
        CAT --> SUBCAT --> PROD
        CUST --> ORD
        SM --> ORD
        LOC --> ORD
        ORD --> ITEMS
        PROD --> ITEMS
        LOC --> ITEMS
    end

    subgraph S3["Tahap 3: Pipeline ETL (etl_pipeline.py)"]
        EXTRACT["1. EXTRACT<br/>Query SQL JOIN 8 Tabel OLTP<br/>(10.093 baris, 21 kolom)"]
        CLEAN["2. TRANSFORM I (Data Cleaning)<br/>• Deduplikasi 95 baris identik + 4 Row ID<br/>• Imputasi 199 Customer/Product/Postal<br/>• Swap 100 tanggal terbalik<br/>• Drop 100 kuantitas <= 0<br/>➔ Output: 9.894 baris bersih"]
        MODEL["3. TRANSFORM II (Dimensional Modeling)<br/>Pembentukan Surrogate Keys & Star Schema"]
        
        ITEMS --> EXTRACT
        EXTRACT --> CLEAN --> MODEL
    end

    subgraph S4["Tahap 4: Load & Distribusi Output (DWH)"]
        LOAD_DWH["4. LOAD DWH<br/>MySQL DWH (superstore_dwh) & CSV"]
        
        MODEL --> LOAD_DWH
        
        LOAD_DWH --> DIM["Tabel Dimensi:<br/>• dim_customer (793)<br/>• dim_product (1.861)<br/>• dim_location (1.651)<br/>• dim_ship_mode (4)<br/>• dim_date (1.434)"]
        LOAD_DWH --> FACT["Tabel Fakta:<br/>• fact_sales (9.894)<br/>• fact_customer_order (4.985)"]
        LOAD_DWH --> DWH_CSV["dwh_output/*.csv<br/>(Salinan Berkas CSV)"]
    end

    subgraph S5["Tahap 5: Verifikasi Kualitas Data & Pengujian"]
        AUDIT["audit.py<br/>(Laporan Kualitas di audit_report/)"]
        TESTS["test_oltp_pipeline.py<br/>(Unit & Integration Test Suite)"]
        
        DIM -.-> AUDIT
        FACT -.-> AUDIT
        ITEMS -.-> TESTS
        DIM -.-> TESTS
        FACT -.-> TESTS
    end

    style S1 fill:#f0f7ff,stroke:#0066cc,stroke-width:2px
    style S2 fill:#eef9f2,stroke:#008844,stroke-width:2px
    style S3 fill:#fff8ee,stroke:#dd7700,stroke-width:2px
    style S4 fill:#fdf0f2,stroke:#cc1133,stroke-width:2px
    style S5 fill:#f5f0fa,stroke:#6622aa,stroke-width:2px
```

---

## 📁 2. Struktur Direktori Proyek yang Rapi

```
superstore-ETL-dwh/
│
├── sql/                             # Naskah DDL Database
│   ├── oltp_schema.sql              # Skema 8 tabel ternormalisasi OLTP (3NF)
│   └── dwh_schema.sql               # Skema Star Schema Data Warehouse
│
├── data_source/                     # Dataset awal mentah
│   └── superstore_dirty.csv         # 10.093 baris transaksi kotor
│
├── dwh_output/                      # Hasil keluaran Star Schema (CSV)
│   ├── dim_customer.csv
│   ├── dim_product.csv
│   ├── dim_location.csv
│   ├── dim_ship_mode.csv
│   ├── dim_date.csv
│   ├── fact_sales.csv
│   └── fact_customer_order.csv
│
├── audit_report/                    # Hasil laporan kualitas data
│   ├── audit_summary.json
│   ├── table_summary.csv
│   ├── null_audit.csv
│   ├── duplicate_audit.csv
│   ├── business_logic_audit.csv
│   └── key_integrity_audit.csv
│
├── logs/                            # Log eksekusi scheduler Windows
│   ├── etl_latest.log
│   └── etl_YYYYMMDD_HHMMSS.log
│
├── .env.example                     # Template konfigurasi kredensial
├── .env                             # Kredensial aktif lokal (terabaikan di git)
├── .gitignore                       # Proteksi file kredensial dan cache
├── requirements.txt                 # Dependensi pustaka Python
├── README.md                        # Dokumentasi teknis proyek
├── ALUR_SISTEM.md                   # Dokumen panduan alur operasional (file ini)
│
├── etl_pipeline.py                  # Core Engine ETL Pipeline (MySQL -> DWH)
├── seed_oltp.py                     # Ingestion tool: CSV -> MySQL OLTP (3NF)
├── audit.py                         # Quality control & audit runner
├── test_oltp_pipeline.py            # Automated Unit & Integration Test Suite
└── run_etl.bat                      # Windows Task Scheduler automation runner
```

---

## ⚡ 3. Langkah Operasional Menjalankan Sistem

### Langkah 0: Persiapan Lingkungan & Dependensi
Pastikan layanan **MySQL Server** (versi 8.0 / MariaDB) telah aktif pada port `3306`, kemudian pasang seluruh pustaka Python:
```bash
pip install -r requirements.txt
```

---

### Langkah 1: Pengaturan File Konfigurasi Lingkungan (`.env`)
Salin file template `.env.example` menjadi `.env`:
```cmd
copy .env.example .env
```
Pastikan kredensial basis data sesuai dengan lingkungan server Anda:
```ini
# Database Sumber (OLTP)
OLTP_DB_HOST=localhost
OLTP_DB_PORT=3306
OLTP_DB_USER=root
OLTP_DB_PASSWORD=
OLTP_DB_NAME=superstore_oltp

# Database Tujuan (DWH)
DWH_DB_HOST=localhost
DWH_DB_PORT=3306
DWH_DB_USER=root
DWH_DB_PASSWORD=
DWH_DB_NAME=superstore_dwh

LOAD_TO_MYSQL=true
```

---

### Langkah 2: Menyiapkan Skema Database DWH (`superstore_dwh`)
Sebelum memuat data analitik, buat struktur Star Schema di database Data Warehouse:
- **Melalui MySQL CLI**:
  ```bash
  mysql -u root -p < sql/dwh_schema.sql
  ```
- **Atau melalui MySQL Workbench / DBeaver**:
  Buka berkas `sql/dwh_schema.sql` lalu jalankan (**Execute Script**).

---

### Langkah 3: Mengisi Database Sumber OLTP (`superstore_oltp`)
Jalankan skrip seeder relasional untuk membuat 8 tabel ternormalisasi dan memetakan 10.093 baris transaksi mentah:
```bash
python seed_oltp.py
```
> **Catatan Data Engineer**: Proses ini menginjeksi data mentah apa adanya (nilai NULL, tanggal terbalik, dan duplikasi sengaja dipertahankan di level `order_items` sebagai *transaction snapshot*).

---

### Langkah 4: Menjalankan Pipeline ETL
Jalankan pemrosesan ETL end-to-end:
```bash
python etl_pipeline.py
```
*Atau di Windows CLI / Task Scheduler:*
```cmd
run_etl.bat
```

**Rangkaian Eksekusi yang Terjadi:**
1. **Extract**: Mengekstrak data melalui query `SELECT ... JOIN` 8 tabel relasional dari database `superstore_oltp`.
2. **Transform (Clean)**: Membersihkan 95 duplikat identik, 4 duplikat `Row ID`, imputasi 199 sel NULL per kolom, menukar 100 tanggal terbalik, dan membuang 100 kuantitas $\le 0$.
3. **Transform (Model)**: Menghasilkan *Surrogate Keys* berurutan untuk tabel dimensi dan fakta.
4. **Load**: Memuat 7 tabel ke MySQL `superstore_dwh` (dengan pengabaian sementara pemeriksaan Foreign Key untuk menjamin integritas reload) serta mengekspor berkas ke direktori `dwh_output/`.

---

### Langkah 5: Memverifikasi Integritas dan Kualitas Data
Jalankan audit kualitas data:
```bash
python audit.py
```
Hasil audit akan langsung ditampilkan di terminal dan seluruh metrik detail tersimpan di folder `audit_report/`.

---

### Langkah 6: Menjalankan Automated Test Suite
Jalankan seluruh pengujian otomatis untuk memverifikasi kesehatan sistem relasional dan integritas data:
```bash
python test_oltp_pipeline.py
```
Skrip ini akan memvalidasi:
- `test_01_oltp_table_counts`: Memverifikasi jumlah record di 8 tabel OLTP.
- `test_02_foreign_key_integrity`: Memastikan tidak ada *orphan foreign key*.
- `test_03_dirty_data_preservation`: Memastikan nilai NULL, anomali tanggal, dan duplikasi tetap terjaga di OLTP.
- `test_04_relational_extract_grain_and_semantics`: Memastikan ekstraksi JOIN menghasilkan tepat 10.093 baris dan 21 kolom.
- `test_05_clean_and_dwh_compatibility`: Memastikan hasil ekstraksi kompatibel 100% dengan tahap Transform dan DWH.

---

## 🛠️ 4. Panduan Penyelesaian Masalah (Troubleshooting)

| Gejala Masalah | Penyebab Umum | Solusi Praktis |
| :--- | :--- | :--- |
| `Can't connect to MySQL server on 'localhost'` | Service MySQL belum berjalan di port 3306. | Buka Laragon/XAMPP/Services dan jalankan service MySQL. |
| `Table 'superstore_oltp.order_items' doesn't exist` | Database OLTP belum diinisialisasi. | Jalankan `python seed_oltp.py` untuk menginisialisasi 8 tabel relasional sumber. |
| `Cannot drop table 'dim_customer' referenced by foreign key` | Foreign key checks aktif saat reload DWH. | Gunakan fungsi `load_to_mysql()` terintegrasi di `etl_pipeline.py` yang otomatis menangani `SET FOREIGN_KEY_CHECKS = 0;`. |
| `ModuleNotFoundError: No module named 'dotenv'` / `'pymysql'` | Dependensi pustaka belum terpasang. | Jalankan `pip install -r requirements.txt`. |
