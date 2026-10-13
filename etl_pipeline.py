"""
ETL Pipeline: Superstore Sales Data to Star Schema Data Warehouse
==================================================================
Script ini melakukan proses Extract, Transform, dan Load (ETL) end-to-end:
1. Extract  : Membaca dataset transaksi mentah dari CSV.
2. Clean    : Menangani Missing Values, Duplikasi Data, dan Anomali Logika Bisnis.
3. Model    : Membentuk tabel dimensi (dim_customer, dim_product, dim_location,
              dim_ship_mode, dim_date) dengan Surrogate Keys berurut.
4. Fact Gen : Membentuk dua tabel fakta:
              - fact_sales          : Granularitas per baris item produk (sales line item)
              - fact_customer_order : Granularitas per Order ID (order header summary)
5. Load     : Mengekspor seluruh tabel dimensi dan fakta ke format CSV di dwh_output/.

Author: Data Engineering Expert
"""

import os
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# Konfigurasi logging standar industri untuk memantau siklus hidup ETL
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ETL_Pipeline")


# ==============================================================================
# 1. TAHAP EXTRACT
# ==============================================================================
def extract_data(file_path: str) -> pd.DataFrame:
    """
    Mengekstrak data transaksi mentah dari file CSV sumber.
    """
    logger.info(f"Memulai proses Extract dari: {file_path}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File sumber tidak ditemukan di path: {file_path}")

    df = pd.read_csv(file_path)
    logger.info(f"Extract berhasil. Baris mentah: {len(df):,}, Kolom: {df.shape[1]}")
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
            lambda x: f"{int(x):05d}" if pd.notnull(x) else "00000"
        )
    else:
        df["Postal Code"] = df["Postal Code"].apply(
            lambda x: f"{int(x):05d}" if pd.notnull(x) else str(postal_code_fill)
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
# 3. TAHAP MODEL (TRANSFORM II - STAR SCHEMA DIMENSIONS)
# ==============================================================================
def create_dim_customer(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Membentuk tabel dimensi Customer (dim_customer).
    Primary Key: customer_key (Surrogate Key, int berurut)
    Unique Key : customer_id
    """
    logger.info("Membentuk dim_customer...")
    # Memprioritaskan nama valid jika customer memiliki catatan transaksi lain yang valid
    dim_cust = (
        df_clean[["Customer ID", "Customer Name", "Segment"]]
        .sort_values(
            by=["Customer ID", "Customer Name"],
            key=lambda col: col == "Unknown"
        )
        .drop_duplicates(subset=["Customer ID"], keep="first")
        .reset_index(drop=True)
    )

    dim_cust.rename(columns={
        "Customer ID": "customer_id",
        "Customer Name": "customer_name",
        "Segment": "segment"
    }, inplace=True)

    # Tambahkan Surrogate Key berurut (1..N)
    dim_cust.insert(0, "customer_key", dim_cust.index + 1)
    logger.info(f"dim_customer selesai: {len(dim_cust):,} entitas unik.")
    return dim_cust


def create_dim_product(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Membentuk tabel dimensi Produk (dim_product).
    Primary Key: product_key (Surrogate Key, int berurut)
    Unique Key : product_id
    """
    logger.info("Membentuk dim_product...")
    # Memprioritaskan nama produk yang valid daripada 'Unknown Product'
    dim_prod = (
        df_clean[["Product ID", "Product Name", "Category", "Sub-Category"]]
        .sort_values(
            by=["Product ID", "Product Name"],
            key=lambda col: col == "Unknown Product"
        )
        .drop_duplicates(subset=["Product ID"], keep="first")
        .reset_index(drop=True)
    )

    dim_prod.rename(columns={
        "Product ID": "product_id",
        "Product Name": "product_name",
        "Category": "category",
        "Sub-Category": "sub_category"
    }, inplace=True)

    # Tambahkan Surrogate Key berurut (1..N)
    dim_prod.insert(0, "product_key", dim_prod.index + 1)
    logger.info(f"dim_product selesai: {len(dim_prod):,} entitas unik.")
    return dim_prod


def create_dim_location(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Membentuk tabel dimensi Lokasi (dim_location).
    Primary Key: location_key (Surrogate Key, int berurut)
    Komponen   : country, region, state, city, postal_code
    """
    logger.info("Membentuk dim_location...")
    loc_cols = ["Country", "Region", "State", "City", "Postal Code"]
    dim_loc = (
        df_clean[loc_cols]
        .drop_duplicates()
        .sort_values(by=loc_cols)
        .reset_index(drop=True)
    )

    dim_loc.rename(columns={
        "Country": "country",
        "Region": "region",
        "State": "state",
        "City": "city",
        "Postal Code": "postal_code"
    }, inplace=True)

    # Tambahkan Surrogate Key berurut (1..N)
    dim_loc.insert(0, "location_key", dim_loc.index + 1)
    logger.info(f"dim_location selesai: {len(dim_loc):,} lokasi unik.")
    return dim_loc


def create_dim_ship_mode(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Membentuk tabel dimensi Moda Pengiriman (dim_ship_mode).
    Primary Key: ship_mode_key (Surrogate Key, int berurut)
    Unique Key : ship_mode
    """
    logger.info("Membentuk dim_ship_mode...")
    dim_ship = (
        df_clean[["Ship Mode"]]
        .drop_duplicates()
        .sort_values(by="Ship Mode")
        .reset_index(drop=True)
    )

    dim_ship.rename(columns={"Ship Mode": "ship_mode"}, inplace=True)

    # Tambahkan Surrogate Key berurut (1..N)
    dim_ship.insert(0, "ship_mode_key", dim_ship.index + 1)
    logger.info(f"dim_ship_mode selesai: {len(dim_ship):,} jenis pengiriman unik.")
    return dim_ship


def create_dim_date(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Membentuk tabel dimensi Waktu/Tanggal (dim_date).
    Digenerate dari seluruh tanggal unik pada gabungan Order Date dan Ship Date.
    Primary Key: date_key (format integer: YYYYMMDD)
    """
    logger.info("Membentuk dim_date...")
    # Menggabungkan Order Date dan Ship Date untuk mendapatkan tanggal unik
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

    dim_dt = dim_dt[[
        "date_key", "full_date", "day", "month", "month_name", "quarter", "year"
    ]]

    logger.info(
        f"dim_date selesai: {len(dim_dt):,} tanggal unik "
        f"({dim_dt['full_date'].min()} s/d {dim_dt['full_date'].max()})."
    )
    return dim_dt


# ==============================================================================
# 4. TAHAP FACT TABLES GENERATION
# ==============================================================================
def create_fact_sales(
    df_clean: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_ship_mode: pd.DataFrame
) -> pd.DataFrame:
    """
    Membentuk tabel fakta Penjualan Baris Item (fact_sales).
    Granularitas: Satu baris untuk setiap item penjualan (Row ID).
    Primary Key : sales_key (bigint berurut)
    """
    logger.info("Membentuk fact_sales (Left Join dengan tabel dimensi)...")
    fact = df_clean.copy()

    # Siapkan atribut asli dan foreign key tanggal (format YYYYMMDD)
    fact["row_id"] = fact["Row ID"].astype(str)
    fact["order_id"] = fact["Order ID"].astype(str)
    fact["order_date_key"] = fact["Order Date"].dt.strftime("%Y%m%d").astype(int)
    fact["ship_date_key"] = fact["Ship Date"].dt.strftime("%Y%m%d").astype(int)
    fact["quantity"] = fact["Quantity"].astype(int)
    fact["sales"] = fact["Sales"].astype(float)
    fact["discount"] = fact["Discount"].astype(float)
    fact["profit"] = fact["Profit"].astype(float)

    # 1. Join ke dim_customer
    fact = fact.merge(
        dim_customer[["customer_id", "customer_key"]],
        left_on="Customer ID",
        right_on="customer_id",
        how="left"
    )

    # 2. Join ke dim_product
    fact = fact.merge(
        dim_product[["product_id", "product_key"]],
        left_on="Product ID",
        right_on="product_id",
        how="left"
    )

    # 3. Join ke dim_location
    fact = fact.merge(
        dim_location[["country", "region", "state", "city", "postal_code", "location_key"]],
        left_on=["Country", "Region", "State", "City", "Postal Code"],
        right_on=["country", "region", "state", "city", "postal_code"],
        how="left"
    )

    # 4. Join ke dim_ship_mode
    fact = fact.merge(
        dim_ship_mode[["ship_mode", "ship_mode_key"]],
        left_on="Ship Mode",
        right_on="ship_mode",
        how="left"
    )

    # Generate Primary Key: sales_key (bigint)
    fact.insert(0, "sales_key", np.arange(1, len(fact) + 1, dtype=np.int64))

    # Susun kolom sesuai skema relasional fact_sales
    fact_columns = [
        "sales_key",
        "row_id",
        "order_id",
        "order_date_key",
        "ship_date_key",
        "customer_key",
        "product_key",
        "location_key",
        "ship_mode_key",
        "quantity",
        "sales",
        "discount",
        "profit"
    ]
    fact_sales = fact[fact_columns].copy()

    # Verifikasi Integritas Relasional
    fk_check = fact_sales[[
        "order_date_key", "ship_date_key", "customer_key",
        "product_key", "location_key", "ship_mode_key"
    ]].isna().sum()

    if fk_check.sum() > 0:
        logger.warning(f"Terdeteksi nilai NULL pada Foreign Key fact_sales:\n{fk_check}")
    else:
        logger.info("Integritas Relasional fact_sales: 100% Foreign Key terpetakan sempurna (0 NULLs).")

    logger.info(f"fact_sales selesai: {len(fact_sales):,} baris transaksi.")
    return fact_sales


def create_fact_customer_order(
    fact_sales: pd.DataFrame,
    dim_location: pd.DataFrame
) -> pd.DataFrame:
    """
    Membentuk tabel fakta Pesanan Pelanggan (fact_customer_order).
    Granularitas: Satu baris untuk setiap Order ID.
    Primary Key : customer_order_key (bigint berurut)
    Measures    : total_sales, total_quantity, total_profit, total_discount, product_line_count
    """
    logger.info("Membentuk fact_customer_order (Agregasi pada tingkat grain Order ID)...")

    # Integrasikan informasi postal_code untuk memprioritaskan alamat valid dibandingkan '00000'
    fact_with_loc = fact_sales.merge(
        dim_location[["location_key", "postal_code"]],
        on="location_key",
        how="left"
    )

    # Prioritaskan entri dengan lokasi diketahui (bukan '00000') saat mengambil location_key pesanan
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
        product_line_count=("sales_key", "count")  # Jumlah baris item produk dalam pesanan
    )

    # Generate Primary Key: customer_order_key (bigint berurut)
    fact_order.insert(0, "customer_order_key", np.arange(1, len(fact_order) + 1, dtype=np.int64))

    # Susun kolom secara presisi sesuai spesifikasi skema
    fact_order_columns = [
        "customer_order_key",
        "order_id",
        "customer_key",
        "order_date_key",
        "location_key",
        "ship_mode_key",
        "total_sales",
        "total_quantity",
        "total_profit",
        "total_discount",
        "product_line_count"
    ]
    fact_customer_order = fact_order[fact_order_columns].copy()

    # Verifikasi Integritas Relasional
    fk_check = fact_customer_order[[
        "customer_key", "order_date_key", "location_key", "ship_mode_key"
    ]].isna().sum()

    if fk_check.sum() > 0:
        logger.warning(f"Terdeteksi nilai NULL pada Foreign Key fact_customer_order:\n{fk_check}")
    else:
        logger.info("Integritas Relasional fact_customer_order: 100% Foreign Key terpetakan sempurna (0 NULLs).")

    logger.info(f"fact_customer_order selesai: {len(fact_customer_order):,} pesanan unik.")
    return fact_customer_order


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

    logger.info("Tahap Load selesai dengan sukses.")

def load_to_mysql(tables: dict, db_connection_string: str) -> None:
    """
    Menyimpan seluruh DataFrame dimensi dan fakta LANGSUNG ke database MySQL.
    """
    logger.info("Memulai tahap Load langsung ke MySQL...")
    
    # Membuat koneksi ke database MySQL
    try:
        engine = create_engine(db_connection_string)
        logger.info("Berhasil terhubung ke database.")
    except Exception as e:
        logger.error(f"Gagal terhubung ke database: {e}")
        return

    # Looping untuk mengirim setiap tabel di dalam dictionary ke database
    for table_name, df_table in tables.items():
        logger.info(f"Mengirim tabel {table_name} ke MySQL...")
        
        # 'if_exists="replace"' akan menghapus tabel lama dan membuat yang baru tiap kali jalan.
        # Jika ingin menambah data (incremental), ubah menjadi 'if_exists="append"'.
        df_table.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace", 
            index=False
        )
        logger.info(f"  [SUCCESS] {table_name:<20} berhasil dimuat ({len(df_table):,} baris)")

    logger.info("Tahap Load ke MySQL selesai dengan sukses.")

# ==============================================================================
# PIPELINE ORCHESTRATOR
# ==============================================================================
def run_etl_pipeline(
    source_csv_path: str = "data_source/superstore_dirty.csv",
    output_dir: str = "dwh_output",
    inverted_date_strategy: str = "swap",
    postal_code_fill: str = "00000"
):
    """
    Fungsi utama orchestrator yang menjalankan seluruh tahapan ETL.
    """
    logger.info("=" * 70)
    logger.info("MEMULAI PIPELINE ETL SUPERSTORE KE DATA WAREHOUSE (STAR SCHEMA)")
    logger.info("=" * 70)

    # 1. EXTRACT
    df_raw = extract_data(source_csv_path)

    # 2. CLEAN (TRANSFORM I)
    df_clean = clean_data(
        df_raw,
        inverted_date_strategy=inverted_date_strategy,
        postal_code_fill=postal_code_fill
    )

    # 3. MODEL (TRANSFORM II - STAR SCHEMA DIMENSIONS)
    dim_customer = create_dim_customer(df_clean)
    dim_product = create_dim_product(df_clean)
    dim_location = create_dim_location(df_clean)
    dim_ship_mode = create_dim_ship_mode(df_clean)
    dim_date = create_dim_date(df_clean)

    # 4. FACT TABLES GENERATION
    # 4.1 Tabel Fakta 1: fact_sales (Grain: Item Penjualan / Line Item)
    fact_sales = create_fact_sales(
        df_clean=df_clean,
        dim_customer=dim_customer,
        dim_product=dim_product,
        dim_location=dim_location,
        dim_ship_mode=dim_ship_mode
    )

    # 4.2 Tabel Fakta 2: fact_customer_order (Grain: Order ID Header)
    fact_customer_order = create_fact_customer_order(
        fact_sales=fact_sales,
        dim_location=dim_location
    )

    load_to_mysql(dwh_tables, db_connection_string=mysql_conn_str)

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
    load_to_dwh(dwh_tables, output_dir=output_dir)

    logger.info("=" * 70)
    logger.info("PIPELINE ETL SELESAI DENGAN SUKSES!")
    logger.info("=" * 70)

    return dwh_tables


if __name__ == "__main__":
    run_etl_pipeline()
