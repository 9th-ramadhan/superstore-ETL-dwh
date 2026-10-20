"""
ETL Pipeline: Superstore Sales Data (OLTP to Star Schema Data Warehouse)
========================================================================
Script ini melakukan proses Extract, Transform, dan Load (ETL) end-to-end:
1. Extract  : Membaca dataset transaksi mentah dari tabel MySQL OLTP.
2. Clean    : Menangani Missing Values, Duplikasi Data, dan Anomali Logika Bisnis.
3. Model    : Membentuk tabel dimensi (dim_customer, dim_product, dim_location,
              dim_ship_mode, dim_date) dengan Surrogate Keys berurut.
4. Fact Gen : Membentuk dua tabel fakta:
              - fact_sales          : Granularitas per baris item produk (sales line item)
              - fact_customer_order : Granularitas per Order ID (order header summary)
5. Load     : Memuat seluruh tabel dimensi dan fakta ke Data Warehouse MySQL
              dan mengekspor salinan ke format CSV di dwh_output/.

Author: Senior Data Engineer
"""

import os
import re
import logging
from urllib.parse import quote_plus
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Memuat variabel lingkungan dari file .env jika tersedia
load_dotenv()

# Konfigurasi logging standar industri untuk memantau siklus hidup ETL
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ETL_Pipeline")


# ==============================================================================
# HELPER: KONEKSI DATABASE & VALIDASI SQL
# ==============================================================================
def validate_sql_identifier(identifier: str) -> str:
    """
    Validasi SQL identifier (nama database, tabel) untuk mencegah SQL Injection.
    Hanya mengizinkan karakter alfanumerik dan underscore.
    """
    if not identifier or not re.match(r"^[a-zA-Z0-9_]+$", str(identifier)):
        raise ValueError(
            f"Identifier SQL '{identifier}' tidak valid. "
            f"Hanya karakter alfanumerik dan underscore yang diperbolehkan."
        )
    return str(identifier)


def get_database_engine(prefix: str = "DWH", default_db: str = "superstore_dwh") -> Engine:
    """
    Membangun SQLAlchemy Engine untuk MySQL menggunakan konfigurasi environment variables.
    Parameter:
        prefix (str): 'OLTP' untuk database sumber atau 'DWH' untuk target data warehouse.
        default_db (str): Nama database default jika env var tidak ditentukan.
    """
    host = os.getenv(f"{prefix}_DB_HOST", "localhost")
    port = os.getenv(f"{prefix}_DB_PORT", "3306")
    user = os.getenv(f"{prefix}_DB_USER", "root")
    password = os.getenv(f"{prefix}_DB_PASSWORD", "")
    database = os.getenv(f"{prefix}_DB_NAME", default_db)

    # URL-encode password untuk menangani karakter khusus dengan aman
    auth_part = f"{user}:{quote_plus(password)}" if password else f"{user}:"
    connection_string = f"mysql+pymysql://{auth_part}@{host}:{port}/{database}"

    return create_engine(connection_string)


# ==============================================================================
# 1. TAHAP EXTRACT
# ==============================================================================
def extract_data(
    engine: Engine | None = None,
    table_name: str | None = None
) -> pd.DataFrame:
    """
    Mengekstrak data transaksi mentah langsung dari database MySQL OLTP.
    Secara default mengekstrak melalui query relasional (JOIN 8 tabel ternormalisasi:
    order_items, orders, customers, products, subcategories, categories, locations, ship_modes)
    sehingga mencerminkan sumber transaksi sungguhan namun tetap mempertahankan
    seluruh anomali data mentah (NULL, tanggal terbalik, duplikat) untuk dibersihkan
    pada tahap Transform.
    """
    # 1. Verifikasi ketersediaan driver pymysql
    try:
        import pymysql
    except ImportError:
        logger.error(
            "Driver 'pymysql' belum terpasang. Jalankan 'pip install pymysql cryptography' "
            "untuk mengaktifkan koneksi ke MySQL."
        )
        raise

    # 2. Inisialisasi engine database OLTP
    if engine is None:
        engine = get_database_engine(prefix="OLTP", default_db="superstore_oltp")

    db_name = engine.url.database or "superstore_oltp"
    logger.info(
        f"Memulai proses Extract dari MySQL OLTP "
        f"({engine.url.host}:{engine.url.port}/{db_name})..."
    )

    # 3. Validasi koneksi dan pengecekan keberadaan tabel relasional
    try:
        with engine.connect() as conn:
            # Periksa ketersediaan tabel transaksi utama order_items
            check_table_sql = text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = :db_name AND table_name = 'order_items'"
            )
            has_order_items = conn.execute(
                check_table_sql,
                {"db_name": db_name}
            ).scalar()

            if has_order_items:
                logger.info("Mengekstrak data dari skema relasional OLTP (JOIN 8 tabel: orders, order_items, customers, products, dll)...")
                relational_query = text("""
                SELECT
                    oi.row_id AS `Row ID`,
                    o.order_id AS `Order ID`,
                    o.order_date AS `Order Date`,
                    oi.ship_date AS `Ship Date`,
                    sm.ship_mode_name AS `Ship Mode`,
                    c.customer_id AS `Customer ID`,
                    oi.customer_name AS `Customer Name`,
                    c.segment AS `Segment`,
                    loc.country AS `Country`,
                    loc.city AS `City`,
                    loc.state AS `State`,
                    loc.postal_code AS `Postal Code`,
                    loc.region AS `Region`,
                    p.product_id AS `Product ID`,
                    cat.category_name AS `Category`,
                    sc.subcategory_name AS `Sub-Category`,
                    oi.product_name AS `Product Name`,
                    oi.sales AS `Sales`,
                    oi.quantity AS `Quantity`,
                    oi.discount AS `Discount`,
                    oi.profit AS `Profit`
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
                JOIN customers c ON o.customer_id = c.customer_id
                JOIN products p ON oi.product_id = p.product_id
                JOIN subcategories sc ON p.subcategory_id = sc.subcategory_id
                JOIN categories cat ON sc.category_id = cat.category_id
                JOIN locations loc ON oi.location_id = loc.location_id
                JOIN ship_modes sm ON o.ship_mode_id = sm.ship_mode_id
                ORDER BY oi.order_item_id ASC;
                """)
                df = pd.read_sql_query(relational_query, conn)
            else:
                # Fallback jika hanya tabel flat superstore_raw yang tersedia
                target_table = validate_sql_identifier(table_name or os.getenv("OLTP_TABLE_NAME", "superstore_raw"))
                check_raw_sql = text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = :db_name AND table_name = :tbl_name"
                )
                raw_exists = conn.execute(check_raw_sql, {"db_name": db_name, "tbl_name": target_table}).scalar()
                if not raw_exists:
                    raise ValueError(
                        f"Tabel sumber 'order_items' maupun '{target_table}' tidak ditemukan di database '{db_name}'. "
                        f"Pastikan database OLTP telah dibuat dan diisi data "
                        f"(jalankan 'python seed_oltp.py' atau 'sql/oltp_schema.sql')."
                    )
                logger.info(f"Mengekstrak data dari tabel tunggal '{target_table}'...")
                query = text(f"SELECT * FROM `{target_table}`")
                df = pd.read_sql_query(query, conn)

    except Exception as e:
        logger.error(f"Gagal melakukan proses Extract dari database MySQL OLTP: {e}")
        logger.warning(
            "Tips: Pastikan layanan MySQL aktif, konfigurasi OLTP pada .env benar, "
            "dan database sumber sudah diinisialisasi dengan 'python seed_oltp.py'."
        )
        raise

    logger.info(f"Extract berhasil. Baris mentah: {len(df):,}, Kolom: {df.shape[1]}")
    return df


def extract_data_from_csv(file_path: str = "data_source/superstore_dirty.csv") -> pd.DataFrame:
    """
    Fungsi utilitas ekstraksi alternatif langsung dari file CSV lokal.
    Dipertahankan untuk pengujian isolasi dan benchmarking.
    """
    logger.info(f"Memulai proses Extract alternatif dari CSV: {file_path}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File sumber tidak ditemukan di path: {file_path}")

    df = pd.read_csv(file_path)
    logger.info(f"Extract CSV berhasil. Baris mentah: {len(df):,}, Kolom: {df.shape[1]}")
    return df


# ==============================================================================
# 2. TAHAP CLEAN (TRANSFORM I)
# ==============================================================================
def clean_data(
    df_raw: pd.DataFrame,
    inverted_date_strategy: str = "swap",
    postal_code_fill: str = "00000"
) -> pd.DataFrame:
    """
    Membersihkan data mentah berdasarkan hasil audit spesifik:
    - Missing Values : Imputasi NULL Customer Name, Product Name, dan Postal Code.
    - Deduplikasi    : Drop duplicate seluruh baris dan Row ID.
    - Tipe Data      : Konversi Order Date & Ship Date ke datetime.
    - Anomali Bisnis : Mengatasi Ship Date < Order Date (swap/drop) dan drop Quantity <= 0.
    """
    logger.info("Memulai proses Data Cleaning (Transform I)...")
    df = df_raw.copy()
    initial_rows = len(df)

    # --------------------------------------------------------------------------
    # A. Penanganan Duplikasi Data
    # --------------------------------------------------------------------------
    # 1. Hapus duplikasi baris identik secara keseluruhan
    duplicate_rows = df.duplicated().sum()
    df = df.drop_duplicates()
    logger.info(f"[Deduplikasi] Menghapus {duplicate_rows:,} baris duplikat identik.")

    # 2. Hapus duplikasi berdasarkan 'Row ID' (mempertahankan baris pertama)
    duplicate_row_ids = df.duplicated(subset=["Row ID"]).sum()
    df = df.drop_duplicates(subset=["Row ID"], keep="first")
    logger.info(f"[Deduplikasi] Menghapus {duplicate_row_ids:,} baris dengan duplikasi 'Row ID'.")

    # --------------------------------------------------------------------------
    # B. Penanganan Missing Values (NULL)
    # --------------------------------------------------------------------------
    # 1. Kolom 'Customer Name' -> "Unknown"
    null_cust = df["Customer Name"].isna().sum()
    df["Customer Name"] = df["Customer Name"].fillna("Unknown")
    logger.info(f"[Imputasi NULL] Mengisi {null_cust:,} nilai NULL pada 'Customer Name' dengan 'Unknown'.")

    # 2. Kolom 'Product Name' -> "Unknown Product"
    null_prod = df["Product Name"].isna().sum()
    df["Product Name"] = df["Product Name"].fillna("Unknown Product")
    logger.info(f"[Imputasi NULL] Mengisi {null_prod:,} nilai NULL pada 'Product Name' dengan 'Unknown Product'.")

    # 3. Kolom 'Postal Code' -> Standarisasi format 5-digit string / "00000" / "Unknown"
    null_postal = df["Postal Code"].isna().sum()
    if postal_code_fill == "00000":
        df["Postal Code"] = df["Postal Code"].apply(
            lambda x: f"{int(float(x)):05d}" if pd.notnull(x) and str(x).strip() != "" else "00000"
        )
    else:
        df["Postal Code"] = df["Postal Code"].apply(
            lambda x: f"{int(float(x)):05d}" if pd.notnull(x) and str(x).strip() != "" else str(postal_code_fill)
        )
    logger.info(f"[Imputasi NULL] Mengisi {null_postal:,} nilai NULL pada 'Postal Code' dengan '{postal_code_fill}'.")

    # --------------------------------------------------------------------------
    # C. Format Tipe Data (Datetime)
    # --------------------------------------------------------------------------
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce")
    logger.info("[Format Tipe Data] 'Order Date' dan 'Ship Date' dikonversi ke tipe datetime.")

    # --------------------------------------------------------------------------
    # D. Penanganan Anomali Logika Bisnis
    # --------------------------------------------------------------------------
    # 1. Tanggal Terbalik (Ship Date terjadi sebelum Order Date)
    inverted_mask = df["Ship Date"] < df["Order Date"]
    inverted_count = inverted_mask.sum()

    if inverted_count > 0:
        if inverted_date_strategy == "swap":
            logger.info(
                f"[Anomali Bisnis] Menemukan {inverted_count:,} baris tanggal terbalik. "
                f"Melakukan swap (menukar nilai Order Date dan Ship Date)."
            )
            # Menukar nilai tanggal terbalik untuk memulihkan urutan kronologis transaksi
            df.loc[inverted_mask, ["Order Date", "Ship Date"]] = (
                df.loc[inverted_mask, ["Ship Date", "Order Date"]].values
            )
        elif inverted_date_strategy == "drop":
            logger.info(
                f"[Anomali Bisnis] Menemukan {inverted_count:,} baris tanggal terbalik. "
                f"Menghapus baris data tersebut (drop corrupt rows)."
            )
            df = df[~inverted_mask].copy()
        else:
            raise ValueError(f"Strategi '{inverted_date_strategy}' tidak valid. Pilih 'swap' atau 'drop'.")

    # 2. Kuantitas Tidak Valid (Quantity <= 0)
    invalid_qty_count = (df["Quantity"] <= 0).sum()
    df = df[df["Quantity"] > 0].copy()
    logger.info(f"[Anomali Bisnis] Memfilter dan membuang {invalid_qty_count:,} baris dengan Quantity <= 0.")

    logger.info(f"Data Cleaning selesai. Total baris: {initial_rows:,} -> {len(df):,} baris bersih.")
    return df


# ==============================================================================
# 3. TAHAP MODEL (TRANSFORM II - INCREMENTAL DIMENSIONAL MODELING)
# ==============================================================================
def get_existing_table(table_name: str, output_dir: str = "dwh_output") -> pd.DataFrame | None:
    """
    Membaca tabel yang sudah ada di DWH (jika tersedia) untuk mendukung incremental load.
    """
    file_path = os.path.join(output_dir, f"{table_name}.csv")
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            logger.warning(f"Gagal membaca tabel eksisting {table_name}: {e}. Memulai dari awal.")
            return None
    return None


def create_dim_customer(df_clean: pd.DataFrame, output_dir: str = "dwh_output") -> pd.DataFrame:
    """
    Membentuk / Memperbarui tabel dimensi Customer (dim_customer) secara incremental.
    Primary Key: customer_key (Surrogate Key, int berurut)
    Unique Key : customer_id
    """
    logger.info("Sinkronisasi dim_customer (Incremental Check)...")
    existing_dim = get_existing_table("dim_customer", output_dir)

    # Ekstrak entitas customer unik dari batch data saat ini
    incoming_cust = (
        df_clean[["Customer ID", "Customer Name", "Segment"]]
        .sort_values(by=["Customer ID", "Customer Name"], key=lambda col: col == "Unknown")
        .drop_duplicates(subset=["Customer ID"], keep="first")
        .rename(columns={
            "Customer ID": "customer_id",
            "Customer Name": "customer_name",
            "Segment": "segment"
        })
    )

    if existing_dim is None or existing_dim.empty:
        # Initial Load: buat key dari 1 s/d N
        incoming_cust.insert(0, "customer_key", np.arange(1, len(incoming_cust) + 1, dtype=int))
        logger.info(f"dim_customer (Initial Load): {len(incoming_cust):,} entitas dibuat.")
        return incoming_cust
    else:
        # Incremental Load: cari customer yang belum ada di DWH
        existing_ids = set(existing_dim["customer_id"].astype(str))
        new_cust = incoming_cust[~incoming_cust["customer_id"].astype(str).isin(existing_ids)].copy()

        if not new_cust.empty:
            max_key = int(existing_dim["customer_key"].max())
            new_cust.insert(0, "customer_key", np.arange(max_key + 1, max_key + 1 + len(new_cust), dtype=int))
            updated_dim = pd.concat([existing_dim, new_cust], ignore_index=True)
            logger.info(f"dim_customer (Incremental): Menambahkan {len(new_cust):,} customer baru. Total: {len(updated_dim):,}.")
            return updated_dim
        else:
            logger.info(f"dim_customer (Incremental): Tidak ada customer baru. Total tetap: {len(existing_dim):,}.")
            return existing_dim


def create_dim_product(df_clean: pd.DataFrame, output_dir: str = "dwh_output") -> pd.DataFrame:
    """
    Membentuk / Memperbarui tabel dimensi Produk (dim_product) secara incremental.
    Primary Key: product_key (Surrogate Key, int berurut)
    Unique Key : product_id
    """
    logger.info("Sinkronisasi dim_product (Incremental Check)...")
    existing_dim = get_existing_table("dim_product", output_dir)

    incoming_prod = (
        df_clean[["Product ID", "Product Name", "Category", "Sub-Category"]]
        .sort_values(by=["Product ID", "Product Name"], key=lambda col: col == "Unknown Product")
        .drop_duplicates(subset=["Product ID"], keep="first")
        .rename(columns={
            "Product ID": "product_id",
            "Product Name": "product_name",
            "Category": "category",
            "Sub-Category": "sub_category"
        })
    )

    if existing_dim is None or existing_dim.empty:
        incoming_prod.insert(0, "product_key", np.arange(1, len(incoming_prod) + 1, dtype=int))
        logger.info(f"dim_product (Initial Load): {len(incoming_prod):,} entitas dibuat.")
        return incoming_prod
    else:
        existing_ids = set(existing_dim["product_id"].astype(str))
        new_prod = incoming_prod[~incoming_prod["product_id"].astype(str).isin(existing_ids)].copy()

        if not new_prod.empty:
            max_key = int(existing_dim["product_key"].max())
            new_prod.insert(0, "product_key", np.arange(max_key + 1, max_key + 1 + len(new_prod), dtype=int))
            updated_dim = pd.concat([existing_dim, new_prod], ignore_index=True)
            logger.info(f"dim_product (Incremental): Menambahkan {len(new_prod):,} produk baru. Total: {len(updated_dim):,}.")
            return updated_dim
        else:
            logger.info(f"dim_product (Incremental): Tidak ada produk baru. Total tetap: {len(existing_dim):,}.")
            return existing_dim


def create_dim_location(df_clean: pd.DataFrame, output_dir: str = "dwh_output") -> pd.DataFrame:
    """
    Membentuk / Memperbarui tabel dimensi Lokasi (dim_location) secara incremental.
    Primary Key: location_key (Surrogate Key, int berurut)
    """
    logger.info("Sinkronisasi dim_location (Incremental Check)...")
    existing_dim = get_existing_table("dim_location", output_dir)

    loc_cols = ["Country", "Region", "State", "City", "Postal Code"]
    incoming_loc = (
        df_clean[loc_cols]
        .drop_duplicates()
        .sort_values(by=loc_cols)
        .rename(columns={
            "Country": "country",
            "Region": "region",
            "State": "state",
            "City": "city",
            "Postal Code": "postal_code"
        })
    )

    if existing_dim is None or existing_dim.empty:
        incoming_loc.insert(0, "location_key", np.arange(1, len(incoming_loc) + 1, dtype=int))
        logger.info(f"dim_location (Initial Load): {len(incoming_loc):,} lokasi dibuat.")
        return incoming_loc
    else:
        check_cols = ["country", "region", "state", "city", "postal_code"]
        existing_dim[check_cols] = existing_dim[check_cols].astype(str)
        incoming_loc[check_cols] = incoming_loc[check_cols].astype(str)

        merged = incoming_loc.merge(existing_dim, on=check_cols, how="left")
        new_loc = merged[merged["location_key"].isna()][check_cols].copy()

        if not new_loc.empty:
            max_key = int(existing_dim["location_key"].max())
            new_loc.insert(0, "location_key", np.arange(max_key + 1, max_key + 1 + len(new_loc), dtype=int))
            updated_dim = pd.concat([existing_dim, new_loc], ignore_index=True)
            logger.info(f"dim_location (Incremental): Menambahkan {len(new_loc):,} lokasi baru. Total: {len(updated_dim):,}.")
            return updated_dim
        else:
            logger.info(f"dim_location (Incremental): Tidak ada lokasi baru. Total tetap: {len(existing_dim):,}.")
            return existing_dim


def create_dim_ship_mode(df_clean: pd.DataFrame, output_dir: str = "dwh_output") -> pd.DataFrame:
    """
    Membentuk / Memperbarui tabel dimensi Moda Pengiriman (dim_ship_mode).
    Primary Key: ship_mode_key (Surrogate Key, int berurut)
    """
    logger.info("Sinkronisasi dim_ship_mode (Incremental Check)...")
    existing_dim = get_existing_table("dim_ship_mode", output_dir)

    incoming_ship = (
        df_clean[["Ship Mode"]]
        .drop_duplicates()
        .sort_values(by="Ship Mode")
        .rename(columns={"Ship Mode": "ship_mode"})
    )

    if existing_dim is None or existing_dim.empty:
        incoming_ship.insert(0, "ship_mode_key", np.arange(1, len(incoming_ship) + 1, dtype=int))
        logger.info(f"dim_ship_mode (Initial Load): {len(incoming_ship):,} jenis pengiriman dibuat.")
        return incoming_ship
    else:
        existing_modes = set(existing_dim["ship_mode"].astype(str))
        new_ship = incoming_ship[~incoming_ship["ship_mode"].astype(str).isin(existing_modes)].copy()

        if not new_ship.empty:
            max_key = int(existing_dim["ship_mode_key"].max())
            new_ship.insert(0, "ship_mode_key", np.arange(max_key + 1, max_key + 1 + len(new_ship), dtype=int))
            updated_dim = pd.concat([existing_dim, new_ship], ignore_index=True)
            logger.info(f"dim_ship_mode (Incremental): Menambahkan {len(new_ship):,} jenis pengiriman baru.")
            return updated_dim
        else:
            logger.info(f"dim_ship_mode (Incremental): Tidak ada jenis pengiriman baru.")
            return existing_dim


def create_dim_date(df_clean: pd.DataFrame, output_dir: str = "dwh_output") -> pd.DataFrame:
    """
    Membentuk / Memperbarui tabel dimensi Waktu/Tanggal (dim_date) secara incremental.
    Primary Key: date_key (format integer: YYYYMMDD)
    """
    logger.info("Sinkronisasi dim_date (Incremental Check)...")
    existing_dim = get_existing_table("dim_date", output_dir)

    all_dates = pd.concat([df_clean["Order Date"], df_clean["Ship Date"]]).dropna().unique()
    unique_dates = pd.Series(all_dates).sort_values().reset_index(drop=True)

    dim_dt = pd.DataFrame({"full_date_dt": unique_dates})
    dim_dt["date_key"] = dim_dt["full_date_dt"].dt.strftime("%Y%m%d").astype(int)
    dim_dt["full_date"] = dim_dt["full_date_dt"].dt.date
    dim_dt["day"] = dim_dt["full_date_dt"].dt.day
    dim_dt["month"] = dim_dt["full_date_dt"].dt.month
    dim_dt["month_name"] = dim_dt["full_date_dt"].dt.month_name()
    dim_dt["quarter"] = dim_dt["full_date_dt"].dt.quarter
    dim_dt["year"] = dim_dt["full_date_dt"].dt.year
    dim_dt = dim_dt[["date_key", "full_date", "day", "month", "month_name", "quarter", "year"]]

    if existing_dim is None or existing_dim.empty:
        logger.info(f"dim_date (Initial Load): {len(dim_dt):,} tanggal kalender dibuat.")
        return dim_dt
    else:
        existing_keys = set(existing_dim["date_key"].astype(int))
        new_dates = dim_dt[~dim_dt["date_key"].astype(int).isin(existing_keys)].copy()

        if not new_dates.empty:
            updated_dim = pd.concat([existing_dim, new_dates], ignore_index=True).sort_values(by="date_key").reset_index(drop=True)
            logger.info(f"dim_date (Incremental): Menambahkan {len(new_dates):,} tanggal baru. Total: {len(updated_dim):,}.")
            return updated_dim
        else:
            logger.info(f"dim_date (Incremental): Tidak ada tanggal baru. Total tetap: {len(existing_dim):,}.")
            return existing_dim


# ==============================================================================
# 4. TAHAP FACT TABLES GENERATION (INCREMENTAL)
# ==============================================================================
def create_fact_sales(
    df_clean: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_ship_mode: pd.DataFrame,
    output_dir: str = "dwh_output"
) -> pd.DataFrame:
    """
    Membentuk / Menambahkan tabel fakta Penjualan (fact_sales) secara incremental.
    Hanya menyertakan baris transaksi yang belum pernah tercatat (berdasarkan Row ID).
    """
    logger.info("Sinkronisasi fact_sales (Incremental Check)...")
    existing_fact = get_existing_table("fact_sales", output_dir)
    
    fact_batch = df_clean.copy()

    # Cek apakah ada data historis untuk memfilter row_id yang sudah ada
    if existing_fact is not None and not existing_fact.empty:
        existing_row_ids = set(existing_fact["row_id"].astype(str))
        fact_batch = fact_batch[~fact_batch["Row ID"].astype(str).isin(existing_row_ids)].copy()
        logger.info(f"fact_sales: Ditemukan {len(fact_batch):,} transaksi baru yang belum tercatat di DWH.")
        if fact_batch.empty:
            logger.info("fact_sales: Tidak ada baris transaksi baru untuk ditambahkan.")
            return existing_fact
        max_sales_key = int(existing_fact["sales_key"].max())
    else:
        max_sales_key = 0
        logger.info(f"fact_sales (Initial Load): Memproses seluruh {len(fact_batch):,} transaksi.")

    # Siapkan atribut asli dan foreign key tanggal (format YYYYMMDD)
    fact_batch["row_id"] = fact_batch["Row ID"].astype(str)
    fact_batch["order_id"] = fact_batch["Order ID"].astype(str)
    fact_batch["order_date_key"] = fact_batch["Order Date"].dt.strftime("%Y%m%d").astype(int)
    fact_batch["ship_date_key"] = fact_batch["Ship Date"].dt.strftime("%Y%m%d").astype(int)
    fact_batch["quantity"] = fact_batch["Quantity"].astype(int)
    fact_batch["sales"] = fact_batch["Sales"].astype(float)
    fact_batch["discount"] = fact_batch["Discount"].astype(float)
    fact_batch["profit"] = fact_batch["Profit"].astype(float)

    # 1. Join ke dim_customer
    fact_batch = fact_batch.merge(
        dim_customer[["customer_id", "customer_key"]].astype({"customer_id": str}),
        left_on="Customer ID",
        right_on="customer_id",
        how="left"
    )

    # 2. Join ke dim_product
    fact_batch = fact_batch.merge(
        dim_product[["product_id", "product_key"]].astype({"product_id": str}),
        left_on="Product ID",
        right_on="product_id",
        how="left"
    )

    # 3. Join ke dim_location
    loc_dim_copy = dim_location[["country", "region", "state", "city", "postal_code", "location_key"]].copy()
    for col in ["country", "region", "state", "city", "postal_code"]:
        loc_dim_copy[col] = loc_dim_copy[col].astype(str)
        fact_batch[col.capitalize() if col != "postal_code" else "Postal Code"] = fact_batch[col.capitalize() if col != "postal_code" else "Postal Code"].astype(str)

    fact_batch = fact_batch.merge(
        loc_dim_copy,
        left_on=["Country", "Region", "State", "City", "Postal Code"],
        right_on=["country", "region", "state", "city", "postal_code"],
        how="left"
    )

    # 4. Join ke dim_ship_mode
    fact_batch = fact_batch.merge(
        dim_ship_mode[["ship_mode", "ship_mode_key"]].astype({"ship_mode": str}),
        left_on="Ship Mode",
        right_on="ship_mode",
        how="left"
    )

    # Generate Primary Key berlanjut dari max_sales_key
    fact_batch.insert(0, "sales_key", np.arange(max_sales_key + 1, max_sales_key + 1 + len(fact_batch), dtype=np.int64))

    # Susun kolom sesuai skema
    fact_columns = [
        "sales_key", "row_id", "order_id", "order_date_key", "ship_date_key",
        "customer_key", "product_key", "location_key", "ship_mode_key",
        "quantity", "sales", "discount", "profit"
    ]
    new_fact_sales = fact_batch[fact_columns].copy()

    # Verifikasi Integritas Relasional
    fk_check = new_fact_sales[[
        "order_date_key", "ship_date_key", "customer_key",
        "product_key", "location_key", "ship_mode_key"
    ]].isna().sum()

    if fk_check.sum() > 0:
        logger.warning(f"Terdeteksi nilai NULL pada Foreign Key fact_sales batch baru:\n{fk_check}")
    else:
        logger.info("Integritas Relasional fact_sales batch baru: 100% Valid (0 NULLs).")

    if existing_fact is not None and not existing_fact.empty:
        combined_fact = pd.concat([existing_fact, new_fact_sales], ignore_index=True)
        logger.info(f"fact_sales (Incremental): Menambahkan {len(new_fact_sales):,} baris. Total: {len(combined_fact):,}.")
        return combined_fact
    else:
        logger.info(f"fact_sales (Initial Load): {len(new_fact_sales):,} baris transaksi terbentuk.")
        return new_fact_sales


def create_fact_customer_order(
    fact_sales: pd.DataFrame,
    dim_location: pd.DataFrame,
    output_dir: str = "dwh_output"
) -> pd.DataFrame:
    """
    Membentuk / Menambahkan tabel fakta Pesanan Pelanggan (fact_customer_order) secara incremental.
    Granularitas: Satu baris per Order ID.
    """
    logger.info("Sinkronisasi fact_customer_order (Incremental Check)...")
    existing_fact_order = get_existing_table("fact_customer_order", output_dir)

    # Jika sudah ada di DWH, ambil order_id yang belum pernah ada
    if existing_fact_order is not None and not existing_fact_order.empty:
        existing_order_ids = set(existing_fact_order["order_id"].astype(str))
        fact_sales_subset = fact_sales[~fact_sales["order_id"].astype(str).isin(existing_order_ids)].copy()
        if fact_sales_subset.empty:
            logger.info("fact_customer_order: Tidak ada pesanan baru untuk ditambahkan.")
            return existing_fact_order
        max_order_key = int(existing_fact_order["customer_order_key"].max())
    else:
        fact_sales_subset = fact_sales.copy()
        max_order_key = 0

    # Integrasikan informasi postal_code untuk memprioritaskan alamat valid
    loc_subset = dim_location[["location_key", "postal_code"]].copy()
    loc_subset["postal_code"] = loc_subset["postal_code"].astype(str)

    fact_with_loc = fact_sales_subset.merge(loc_subset, on="location_key", how="left")
    fact_with_loc["is_unknown_loc"] = fact_with_loc["postal_code"].isin(["00000", "0", "Unknown"])
    fact_sorted = fact_with_loc.sort_values(by=["order_id", "is_unknown_loc"])

    # Agregasi data pada tingkat Order ID
    fact_order = fact_sorted.groupby("order_id", as_index=False).agg(
        customer_key=("customer_key", "first"),
        order_date_key=("order_date_key", lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
        location_key=("location_key", "first"),
        ship_mode_key=("ship_mode_key", "first"),
        total_sales=("sales", lambda x: np.round(x.sum(), 4)),
        total_quantity=("quantity", "sum"),
        total_profit=("profit", lambda x: np.round(x.sum(), 4)),
        total_discount=("discount", lambda x: np.round(x.sum(), 4)),
        product_line_count=("sales_key", "count")
    )

    # Generate Primary Key berlanjut
    fact_order.insert(0, "customer_order_key", np.arange(max_order_key + 1, max_order_key + 1 + len(fact_order), dtype=np.int64))

    fact_order_columns = [
        "customer_order_key", "order_id", "customer_key", "order_date_key",
        "location_key", "ship_mode_key", "total_sales", "total_quantity",
        "total_profit", "total_discount", "product_line_count"
    ]
    new_fact_order = fact_order[fact_order_columns].copy()

    if existing_fact_order is not None and not existing_fact_order.empty:
        combined_fact_order = pd.concat([existing_fact_order, new_fact_order], ignore_index=True)
        logger.info(f"fact_customer_order (Incremental): Menambahkan {len(new_fact_order):,} pesanan. Total: {len(combined_fact_order):,}.")
        return combined_fact_order
    else:
        logger.info(f"fact_customer_order (Initial Load): {len(new_fact_order):,} pesanan unik terbentuk.")
        return new_fact_order


# ==============================================================================
# 5. TAHAP LOAD
# ==============================================================================
def load_to_dwh(tables: dict, output_dir: str = "dwh_output") -> None:
    """
    Menyimpan seluruh DataFrame dimensi dan fakta menjadi file CSV pada direktori output.
    """
    logger.info(f"Memulai tahap Load ke direktori: '{output_dir}/'...")
    os.makedirs(output_dir, exist_ok=True)

    for table_name, df_table in tables.items():
        file_path = os.path.join(output_dir, f"{table_name}.csv")
        df_table.to_csv(file_path, index=False)
        logger.info(f"  [SAVED] {table_name:<20} -> {file_path} ({len(df_table):,} baris, {df_table.shape[1]} kolom)")

    logger.info("Tahap Load ke CSV selesai dengan sukses.")


def load_to_mysql(
    tables: dict,
    engine: Engine | None = None,
    db_connection_string: str | None = None
) -> None:
    """
    Menyimpan seluruh DataFrame dimensi dan fakta LANGSUNG ke database MySQL DWH.
    Menggunakan konfigurasi terpusat atau engine yang disediakan.
    """
    logger.info("Memulai tahap Load langsung ke MySQL Data Warehouse...")

    # 1. Pengecekan driver pymysql
    try:
        import pymysql
    except ImportError:
        logger.error(
            "Driver 'pymysql' belum terpasang. Jalankan 'pip install pymysql cryptography' "
            "untuk mengaktifkan fitur load langsung ke MySQL."
        )
        return

    # 2. Uji koneksi ke database MySQL DWH
    try:
        if engine is None:
            if db_connection_string:
                engine = create_engine(db_connection_string)
            else:
                engine = get_database_engine(prefix="DWH", default_db="superstore_dwh")

        with engine.connect() as conn:
            logger.info(f"Berhasil terhubung ke database MySQL DWH: {engine.url.database}")
    except Exception as e:
        logger.error(f"Gagal terhubung ke database MySQL DWH: {e}")
        logger.warning(
            "Tips: Pastikan MySQL Server sedang aktif dan database tujuan ('superstore_dwh') "
            "sudah dibuat terlebih dahulu (jalankan 'sql/dwh_schema.sql')."
        )
        return

    # 3. Looping untuk mengirim setiap tabel ke MySQL dengan aman
    try:
        with engine.connect() as conn:
            # Nonaktifkan sementara pemeriksaan foreign key agar replace table dimensi & fakta dapat berjalan
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            for table_name, df_table in tables.items():
                try:
                    logger.info(f"Mengirim tabel {table_name} ke MySQL...")
                    df_table.to_sql(
                        name=table_name,
                        con=conn,
                        if_exists="replace",
                        index=False
                    )
                    logger.info(f"  [SUCCESS] {table_name:<20} berhasil dimuat ({len(df_table):,} baris)")
                except Exception as e:
                    logger.error(f"  [FAILED] Gagal memuat tabel {table_name}: {e}")
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            conn.commit()
    except Exception as e:
        logger.error(f"Gagal melakukan proses penulisan ke database MySQL DWH: {e}")

    logger.info("Tahap Load ke MySQL DWH selesai.")


# ==============================================================================
# PIPELINE ORCHESTRATOR
# ==============================================================================
def run_etl_pipeline(
    oltp_table_name: str | None = None,
    output_dir: str = "dwh_output",
    inverted_date_strategy: str = "swap",
    postal_code_fill: str = "00000",
    load_mysql: bool | None = None
):
    """
    Fungsi utama orchestrator yang menjalankan seluruh tahapan ETL dari MySQL OLTP ke DWH.
    Parameter:
        oltp_table_name: Nama tabel sumber di database OLTP (default dari env atau 'superstore_raw').
        output_dir: Direktori tempat menyimpan salinan data warehouse dalam format CSV.
        inverted_date_strategy: Strategi anomali tanggal ('swap' atau 'drop').
        postal_code_fill: Nilai default imputasi untuk postal code yang hilang ('00000').
        load_mysql: Opsi untuk memuat hasil ke MySQL DWH (default membaca dari env LOAD_TO_MYSQL).
    """
    logger.info("=" * 70)
    logger.info("MEMULAI PIPELINE ETL SUPERSTORE: MYSQL OLTP -> DATA WAREHOUSE (STAR SCHEMA)")
    logger.info("=" * 70)

    # 1. EXTRACT (Dari MySQL OLTP)
    df_raw = extract_data(table_name=oltp_table_name)

    # 2. CLEAN (TRANSFORM I)
    df_clean = clean_data(
        df_raw,
        inverted_date_strategy=inverted_date_strategy,
        postal_code_fill=postal_code_fill
    )

    # 3. MODEL (TRANSFORM II - INCREMENTAL STAR SCHEMA DIMENSIONS)
    dim_customer = create_dim_customer(df_clean, output_dir=output_dir)
    dim_product = create_dim_product(df_clean, output_dir=output_dir)
    dim_location = create_dim_location(df_clean, output_dir=output_dir)
    dim_ship_mode = create_dim_ship_mode(df_clean, output_dir=output_dir)
    dim_date = create_dim_date(df_clean, output_dir=output_dir)

    # 4. FACT TABLES GENERATION (INCREMENTAL)
    # 4.1 Tabel Fakta 1: fact_sales (Grain: Item Penjualan / Line Item)
    fact_sales = create_fact_sales(
        df_clean=df_clean,
        dim_customer=dim_customer,
        dim_product=dim_product,
        dim_location=dim_location,
        dim_ship_mode=dim_ship_mode,
        output_dir=output_dir
    )

    # 4.2 Tabel Fakta 2: fact_customer_order (Grain: Order ID Header)
    fact_customer_order = create_fact_customer_order(
        fact_sales=fact_sales,
        dim_location=dim_location,
        output_dir=output_dir
    )

    # 5. LOAD
    dwh_tables = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_location": dim_location,
        "dim_ship_mode": dim_ship_mode,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
        "fact_customer_order": fact_customer_order
    }

    # 5.1 Load ke CSV (DWH Output Directory)
    load_to_dwh(dwh_tables, output_dir=output_dir)

    # 5.2 Load ke MySQL DWH
    if load_mysql is None:
        load_mysql_env = os.getenv("LOAD_TO_MYSQL", "true").lower()
        load_mysql = load_mysql_env in ("true", "1", "yes")

    if load_mysql:
        dwh_engine = get_database_engine(prefix="DWH", default_db="superstore_dwh")
        load_to_mysql(dwh_tables, engine=dwh_engine)

    logger.info("=" * 70)
    logger.info("PIPELINE ETL SELESAI DENGAN SUKSES!")
    logger.info("=" * 70)

    return dwh_tables


if __name__ == "__main__":
    run_etl_pipeline()
