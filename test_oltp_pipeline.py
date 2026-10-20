"""
Automated Test Suite: Relational MySQL OLTP & ETL Verification
=============================================================
Memverifikasi:
1. Jumlah record di setiap tabel OLTP (8 tabel).
2. Integritas foreign key dan relasi 3NF.
3. Preservasi nilai kotor (NULLs, tanggal terbalik, kuantitas <= 0, duplikat).
4. Ekstraksi relasional (JOIN 8 tabel) melalui MySQL OLTP.
5. Grain hasil JOIN (10.093 baris transaksi tanpa baris hilang atau duplikasi buatan).
6. Kompatibilitas penuh dengan fungsi Transform (clean_data) dan DWH Load.
"""

import sys
import unittest
import pandas as pd
from sqlalchemy import text
from etl_pipeline import get_database_engine, extract_data, clean_data, run_etl_pipeline


class TestRelationalOLTPPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = get_database_engine(prefix="OLTP", default_db="superstore_oltp")
        cls.csv_df = pd.read_csv("data_source/superstore_dirty.csv")

    def test_01_oltp_table_counts(self):
        """Memverifikasi seluruh 8 tabel OLTP terisi dengan jumlah record yang tepat."""
        expected_counts = {
            "categories": 3,
            "subcategories": 17,
            "products": 1862,
            "customers": 793,
            "ship_modes": 4,
            "locations": 733,
            "orders": 5009,
            "order_items": 10093
        }

        with self.engine.connect() as conn:
            for table, expected in expected_counts.items():
                count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
                self.assertEqual(
                    count, expected,
                    f"Jumlah baris pada tabel {table} tidak sesuai. Didapat: {count}, Diharapkan: {expected}"
                )

    def test_02_foreign_key_integrity(self):
        """Memverifikasi integritas referensial: tidak ada orphaned foreign key di OLTP."""
        checks = [
            ("subcategories", "category_id", "categories", "category_id"),
            ("products", "subcategory_id", "subcategories", "subcategory_id"),
            ("orders", "customer_id", "customers", "customer_id"),
            ("orders", "ship_mode_id", "ship_modes", "ship_mode_id"),
            ("orders", "location_id", "locations", "location_id"),
            ("order_items", "order_id", "orders", "order_id"),
            ("order_items", "product_id", "products", "product_id"),
            ("order_items", "location_id", "locations", "location_id")
        ]

        with self.engine.connect() as conn:
            for child_tbl, child_fk, parent_tbl, parent_pk in checks:
                query = text(f"""
                    SELECT COUNT(*)
                    FROM `{child_tbl}` c
                    LEFT JOIN `{parent_tbl}` p ON c.`{child_fk}` = p.`{parent_pk}`
                    WHERE p.`{parent_pk}` IS NULL
                """)
                orphan_count = conn.execute(query).scalar()
                self.assertEqual(
                    orphan_count, 0,
                    f"Ditemukan {orphan_count} orphan FK pada {child_tbl}.{child_fk} -> {parent_tbl}.{parent_pk}"
                )

    def test_03_dirty_data_preservation(self):
        """Memverifikasi bahwa seluruh anomali data mentah tetap tersimpan utuh di OLTP."""
        with self.engine.connect() as conn:
            # A. Null counts di order_items
            null_cust = conn.execute(text("SELECT COUNT(*) FROM order_items WHERE customer_name IS NULL")).scalar()
            null_prod = conn.execute(text("SELECT COUNT(*) FROM order_items WHERE product_name IS NULL")).scalar()
            null_post = conn.execute(text("SELECT COUNT(*) FROM locations WHERE postal_code IS NULL")).scalar()

            self.assertEqual(null_cust, 200, "NULL Customer Name pada order_items harus 200")
            self.assertEqual(null_prod, 200, "NULL Product Name pada order_items harus 200")
            self.assertEqual(null_post, 101, "NULL Postal Code pada master locations harus 101")

            # B. Duplikat Row ID
            dup_row_ids = conn.execute(text("""
                SELECT SUM(c - 1) FROM (
                    SELECT row_id, COUNT(*) as c FROM order_items GROUP BY row_id HAVING c > 1
                ) sub
            """)).scalar()
            self.assertEqual(dup_row_ids, 99, "Duplikat Row ID pada order_items harus 99")

            # C. Anomali kuantitas <= 0
            invalid_qty = conn.execute(text("SELECT COUNT(*) FROM order_items WHERE quantity <= 0")).scalar()
            self.assertEqual(invalid_qty, 100, "Transaksi Quantity <= 0 harus 100")

    def test_04_relational_extract_grain_and_semantics(self):
        """Memverifikasi ekstraksi JOIN dari MySQL OLTP menghasilkan shape, kolom, dan grain yang persis."""
        df_extracted = extract_data()

        # Shape & Grain
        self.assertEqual(df_extracted.shape, (10093, 21))
        self.assertEqual(list(df_extracted.columns), list(self.csv_df.columns))

        # Total NULL cells di hasil ekstrak harus tetap 600
        self.assertEqual(int(df_extracted.isna().sum().sum()), 600)
        self.assertEqual(int(df_extracted["Customer Name"].isna().sum()), 200)
        self.assertEqual(int(df_extracted["Product Name"].isna().sum()), 200)
        self.assertEqual(int(df_extracted["Postal Code"].isna().sum()), 200)

        # Duplikasi
        self.assertEqual(int(df_extracted.duplicated().sum()), 95)
        self.assertEqual(int(df_extracted.duplicated(subset=["Row ID"]).sum()), 99)

    def test_05_clean_and_dwh_compatibility(self):
        """Memverifikasi DataFrame hasil extract dari MySQL OLTP 100% kompatibel dengan Transform DWH."""
        df_extracted = extract_data()
        df_clean = clean_data(df_extracted)

        # Harus menghasilkan 9.894 baris data bersih
        self.assertEqual(len(df_clean), 9894)
        self.assertEqual(df_clean["Customer Name"].isna().sum(), 0)
        self.assertEqual(df_clean["Product Name"].isna().sum(), 0)
        self.assertEqual(df_clean["Postal Code"].isna().sum(), 0)
        self.assertEqual((df_clean["Quantity"] <= 0).sum(), 0)
        self.assertEqual((df_clean["Ship Date"] < df_clean["Order Date"]).sum(), 0)


if __name__ == "__main__":
    unittest.main()
