"""
Seeder Script: Memuat Dataset CSV ke Database Relasional MySQL OLTP (3NF)
========================================================================
Script utilitas untuk menginisialisasi skema relasional OLTP (8 tabel)
dan memasukkan 10.093 record mentah dari 'data_source/superstore_dirty.csv'
tanpa membersihkan atau memodifikasi data kotor (preservasi anomali).

Tabel Target:
- categories, subcategories, products (Katalog Produk)
- customers (Pelanggan)
- ship_modes (Metode Pengiriman)
- locations (Alamat Geografis)
- orders (Header Pesanan)
- order_items (Detail Item Transaksi)

Author: Senior Data Engineer
"""

import os
import sys
import logging
from urllib.parse import quote_plus
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("OLTP_Seeder")

load_dotenv()


def get_oltp_engine():
    host = os.getenv("OLTP_DB_HOST", "localhost")
    port = os.getenv("OLTP_DB_PORT", "3306")
    user = os.getenv("OLTP_DB_USER", "root")
    password = os.getenv("OLTP_DB_PASSWORD", "")
    database = os.getenv("OLTP_DB_NAME", "superstore_oltp")

    auth = f"{user}:{quote_plus(password)}" if password else f"{user}:"
    server_url = f"mysql+pymysql://{auth}@{host}:{port}"
    db_url = f"mysql+pymysql://{auth}@{host}:{port}/{database}"

    # Pastikan database ada
    server_engine = create_engine(server_url)
    with server_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{database}` DEFAULT CHARACTER SET utf8mb4;"))
        conn.commit()

    return create_engine(db_url)


def seed_relational_oltp(
    csv_path: str = "data_source/superstore_dirty.csv",
    schema_file: str = "sql/oltp_schema.sql"
):
    """
    Mengimpor dataset kotor CSV ke database relasional MySQL OLTP dengan normalisasi 3NF.
    Seluruh nilai NULL, anomali tanggal terbalik, dan duplikat tetap dipertahankan.
    """
    if not os.path.exists(schema_file) and os.path.exists("oltp_schema.sql"):
        schema_file = "oltp_schema.sql"
    if not os.path.exists(csv_path):
        logger.error(f"File CSV sumber tidak ditemukan di: {csv_path}")
        sys.exit(1)

    logger.info("=" * 70)
    logger.info("MEMULAI SEEDING DATABASE RELASIONAL MYSQL OLTP (NORMALISASI 3NF)")
    logger.info("=" * 70)

    engine = get_oltp_engine()

    # 1. Jalankan DDL Skema Relasional
    if os.path.exists(schema_file):
        logger.info(f"Menerapkan skema relasional dari '{schema_file}'...")
        with open(schema_file, "r", encoding="utf-8") as f:
            sql_statements = f.read().split(";")
            with engine.connect() as conn:
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
                for stmt in sql_statements:
                    stmt_clean = stmt.strip()
                    if stmt_clean and not stmt_clean.upper().startswith("CREATE DATABASE") and not stmt_clean.upper().startswith("USE"):
                        conn.execute(text(stmt_clean))
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                conn.commit()
        logger.info("Struktur tabel relasional OLTP berhasil dibuat.")

    # 2. Baca Dataset Kotor dari CSV
    logger.info(f"Membaca dataset mentah: {csv_path}...")
    df = pd.read_csv(csv_path)
    total_raw = len(df)
    logger.info(f"Total baris mentah: {total_raw:,}, Kolom: {df.shape[1]}")

    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        # ----------------------------------------------------------------------
        # A. Tabel: categories
        # ----------------------------------------------------------------------
        unique_cats = sorted(df["Category"].dropna().unique())
        df_categories = pd.DataFrame({
            "category_id": np.arange(1, len(unique_cats) + 1),
            "category_name": unique_cats
        })
        df_categories.to_sql("categories", con=conn, if_exists="append", index=False)
        cat_map = dict(zip(df_categories["category_name"], df_categories["category_id"]))
        logger.info(f"  [SEEDED] categories       : {len(df_categories):,} baris")

        # ----------------------------------------------------------------------
        # B. Tabel: subcategories
        # ----------------------------------------------------------------------
        unique_subcats = (
            df[["Category", "Sub-Category"]]
            .drop_duplicates()
            .sort_values(by="Sub-Category")
            .reset_index(drop=True)
        )
        unique_subcats["category_id"] = unique_subcats["Category"].map(cat_map)
        unique_subcats["subcategory_id"] = np.arange(1, len(unique_subcats) + 1)
        unique_subcats = unique_subcats.rename(columns={"Sub-Category": "subcategory_name"})
        unique_subcats[["subcategory_id", "category_id", "subcategory_name"]].to_sql(
            "subcategories", con=conn, if_exists="append", index=False
        )
        subcat_map = dict(zip(unique_subcats["subcategory_name"], unique_subcats["subcategory_id"]))
        logger.info(f"  [SEEDED] subcategories    : {len(unique_subcats):,} baris")

        # ----------------------------------------------------------------------
        # C. Tabel: products (Master Katalog Produk)
        # ----------------------------------------------------------------------
        # Ambil representasi master produk unik (1 baris per Product ID)
        df_prods = (
            df.sort_values(by=["Product ID", "Product Name"], key=lambda col: col.isna())
            .drop_duplicates(subset=["Product ID"], keep="first")
            .copy()
        )
        df_prods["subcategory_id"] = df_prods["Sub-Category"].map(subcat_map)
        df_prods = df_prods[["Product ID", "subcategory_id", "Product Name"]].rename(columns={
            "Product ID": "product_id",
            "Product Name": "product_name"
        })
        df_prods.to_sql("products", con=conn, if_exists="append", index=False)
        logger.info(f"  [SEEDED] products         : {len(df_prods):,} baris")

        # ----------------------------------------------------------------------
        # D. Tabel: customers (Master Pelanggan)
        # ----------------------------------------------------------------------
        df_custs = (
            df.sort_values(by=["Customer ID", "Customer Name"], key=lambda col: col.isna())
            .drop_duplicates(subset=["Customer ID"], keep="first")
            .copy()
        )
        df_custs = df_custs[["Customer ID", "Customer Name", "Segment"]].rename(columns={
            "Customer ID": "customer_id",
            "Customer Name": "customer_name",
            "Segment": "segment"
        })
        # Isi master customer name yang kebetulan NULL di baris pertama dengan default valid
        df_custs["customer_name"] = df_custs["customer_name"].fillna("Unknown Customer")
        df_custs.to_sql("customers", con=conn, if_exists="append", index=False)
        logger.info(f"  [SEEDED] customers        : {len(df_custs):,} baris")

        # ----------------------------------------------------------------------
        # E. Tabel: ship_modes (Master Moda Pengiriman)
        # ----------------------------------------------------------------------
        unique_modes = sorted(df["Ship Mode"].dropna().unique())
        df_ship = pd.DataFrame({
            "ship_mode_id": np.arange(1, len(unique_modes) + 1),
            "ship_mode_name": unique_modes
        })
        df_ship.to_sql("ship_modes", con=conn, if_exists="append", index=False)
        ship_map = dict(zip(df_ship["ship_mode_name"], df_ship["ship_mode_id"]))
        logger.info(f"  [SEEDED] ship_modes       : {len(df_ship):,} baris")

        # ----------------------------------------------------------------------
        # F. Tabel: locations (Master Alamat & Wilayah Geografis)
        # ----------------------------------------------------------------------
        loc_cols = ["Country", "Region", "State", "City", "Postal Code"]
        df_locs = df[loc_cols].drop_duplicates().reset_index(drop=True)
        df_locs["location_id"] = np.arange(1, len(df_locs) + 1)

        # Ubah nama kolom untuk MySQL
        df_locs_insert = df_locs.rename(columns={
            "Country": "country",
            "Region": "region",
            "State": "state",
            "City": "city",
            "Postal Code": "postal_code"
        })
        df_locs_insert.to_sql("locations", con=conn, if_exists="append", index=False)
        logger.info(f"  [SEEDED] locations        : {len(df_locs_insert):,} baris (termasuk postal_code NULL)")

        # ----------------------------------------------------------------------
        # G. Tabel: orders (Header Pesanan)
        # ----------------------------------------------------------------------
        # Hubungkan location_id ke dataframe mentah berdasarkan loc_cols
        df_mapped = df.merge(df_locs, on=loc_cols, how="left")

        df_orders = (
            df_mapped.drop_duplicates(subset=["Order ID"], keep="first")
            [["Order ID", "Customer ID", "Order Date", "Ship Mode", "location_id"]]
            .copy()
        )
        df_orders["ship_mode_id"] = df_orders["Ship Mode"].map(ship_map)
        df_orders = df_orders.rename(columns={
            "Order ID": "order_id",
            "Customer ID": "customer_id",
            "Order Date": "order_date"
        })[["order_id", "customer_id", "order_date", "ship_mode_id", "location_id"]]

        df_orders.to_sql("orders", con=conn, if_exists="append", index=False)
        logger.info(f"  [SEEDED] orders           : {len(df_orders):,} baris header")

        # ----------------------------------------------------------------------
        # H. Tabel: order_items (Detail Item Transaksi)
        # ----------------------------------------------------------------------
        # Memuat seluruh 10.093 baris transaksi mentah tanpa membuang apapun
        df_items = pd.DataFrame({
            "order_item_id": np.arange(1, len(df_mapped) + 1),
            "row_id": df_mapped["Row ID"],
            "order_id": df_mapped["Order ID"],
            "product_id": df_mapped["Product ID"],
            "location_id": df_mapped["location_id"],
            "ship_date": df_mapped["Ship Date"],
            "customer_name": df_mapped["Customer Name"],
            "product_name": df_mapped["Product Name"],
            "sales": df_mapped["Sales"],
            "quantity": df_mapped["Quantity"],
            "discount": df_mapped["Discount"],
            "profit": df_mapped["Profit"]
        })

        df_items.to_sql("order_items", con=conn, if_exists="append", index=False)
        logger.info(f"  [SEEDED] order_items      : {len(df_items):,} baris transaksi mentah")

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        conn.commit()

    logger.info("=" * 70)
    logger.info("SEEDING DATABASE RELASIONAL MYSQL OLTP BERHASIL 100%!")
    logger.info("=" * 70)


if __name__ == "__main__":
    seed_relational_oltp()
