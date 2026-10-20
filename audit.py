
from pathlib import Path
import pandas as pd
import json

# =====================================================
# 1. KONFIGURASI FOLDER
# =====================================================

BASE_DIR = Path(__file__).resolve().parent

SOURCE_FILE = BASE_DIR / "data_source" / "superstore_dirty.csv"
DWH_DIR = BASE_DIR / "dwh_output"

REPORT_DIR = BASE_DIR / "audit_report"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

DIMENSIONS = {
    "dim_customer.csv": "Customer ID",
    "dim_date.csv": "Date Key",
    "dim_location.csv": "Location Key",
    "dim_product.csv": "Product ID",
    "dim_ship_mode.csv": "Ship Mode Key",
}

FACT_FILE = "fact_sales.csv"


# =====================================================
# 2. MEMBACA CSV
# =====================================================

def read_csv(path):
    if not path.exists():
        print(f"[SKIP] File tidak ditemukan: {path}")
        return None

    df = pd.read_csv(path)

    # Normalisasi string kosong menjadi NULL
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    return df


# =====================================================
# 3. AUDIT NULL
# =====================================================

def audit_null(df, table_name):
    result = []

    for col in df.columns:
        null_count = int(df[col].isna().sum())

        result.append({
            "table": table_name,
            "column": col,
            "row_count": len(df),
            "null_count": null_count,
            "null_percentage": round(
                null_count / max(len(df), 1) * 100, 2
            ),
        })

    return result


# =====================================================
# 4. AUDIT DUPLIKASI
# =====================================================

def audit_duplicates(df, table_name, key_columns=None):
    result = [{
        "table": table_name,
        "check": "Full row duplicates",
        "columns": "*",
        "duplicate_count": int(
            df.duplicated(keep="first").sum()
        ),
    }]

    if key_columns:
        existing_keys = [
            col for col in key_columns if col in df.columns
        ]

        if existing_keys:
            result.append({
                "table": table_name,
                "check": "Duplicate key",
                "columns": ", ".join(existing_keys),
                "duplicate_count": int(
                    df.duplicated(
                        subset=existing_keys,
                        keep="first"
                    ).sum()
                ),
            })

    return result


# =====================================================
# 5. AUDIT BUSINESS LOGIC
# =====================================================

def audit_business_logic(df, table_name):
    result = []

    # Sales harus non-negatif untuk transaksi penjualan biasa.
    # Nilai negatif bisa valid jika ada retur/koreksi.
    if "Sales" in df.columns:
        sales = pd.to_numeric(df["Sales"], errors="coerce")

        invalid = sales.lt(0) & sales.notna()

        result.append({
            "table": table_name,
            "rule": "Sales < 0",
            "invalid_count": int(invalid.sum()),
            "description": (
                "Periksa retur atau nilai penjualan negatif"
            ),
        })

    if "Quantity" in df.columns:
        quantity = pd.to_numeric(
            df["Quantity"], errors="coerce"
        )

        invalid = quantity.le(0) & quantity.notna()

        result.append({
            "table": table_name,
            "rule": "Quantity <= 0",
            "invalid_count": int(invalid.sum()),
            "description": (
                "Periksa kuantitas nol atau negatif"
            ),
        })

    # Validasi tanggal pesanan dan pengiriman.
    if (
        "Order Date" in df.columns
        and "Ship Date" in df.columns
    ):
        order_date = pd.to_datetime(
            df["Order Date"], errors="coerce"
        )

        ship_date = pd.to_datetime(
            df["Ship Date"], errors="coerce"
        )

        comparable = order_date.notna() & ship_date.notna()

        invalid = comparable & (ship_date < order_date)

        result.append({
            "table": table_name,
            "rule": "Ship Date < Order Date",
            "invalid_count": int(invalid.sum()),
            "description": (
                "Tanggal pengiriman mendahului tanggal pesanan"
            ),
        })

    return result


# =====================================================
# 6. AUDIT PRIMARY KEY DAN FOREIGN KEY
# =====================================================

def audit_dwh_keys(tables):
    results = []

    # Kandidat foreign key pada fact_sales.
    # Sesuaikan dengan kolom aktual setelah memeriksa CSV.
    relationships = {
        "Customer ID": ("dim_customer.csv", "Customer ID"),
        "Product ID": ("dim_product.csv", "Product ID"),
        "Location Key": ("dim_location.csv", "Location Key"),
        "Ship Mode Key": ("dim_ship_mode.csv", "Ship Mode Key"),
        "Date Key": ("dim_date.csv", "Date Key"),
    }

    # Periksa primary key dimensi.
    for filename, key in DIMENSIONS.items():
        df = tables.get(filename)

        if df is None:
            continue

        if key not in df.columns:
            results.append({
                "check": "Dimension primary key",
                "table": filename,
                "column": key,
                "issue_count": None,
                "status": "Kolom kunci tidak ditemukan",
            })
            continue

        null_keys = int(df[key].isna().sum())
        duplicate_keys = int(
            df[df[key].notna()].duplicated(
                subset=[key], keep="first"
            ).sum()
        )

        results.append({
            "check": "Dimension primary key NULL",
            "table": filename,
            "column": key,
            "issue_count": null_keys,
            "status": "OK" if null_keys == 0 else "Perlu diperiksa",
        })

        results.append({
            "check": "Dimension primary key duplicate",
            "table": filename,
            "column": key,
            "issue_count": duplicate_keys,
            "status": (
                "OK" if duplicate_keys == 0
                else "Perlu diperiksa"
            ),
        })

    fact = tables.get(FACT_FILE)

    if fact is not None:
        for fact_col, (dim_file, dim_key) in relationships.items():
            if fact_col not in fact.columns:
                continue

            dim = tables.get(dim_file)

            if dim is None or dim_key not in dim.columns:
                results.append({
                    "check": "Foreign key",
                    "table": FACT_FILE,
                    "column": fact_col,
                    "issue_count": None,
                    "status": "Dimensi atau kolom kunci tidak tersedia",
                })
                continue

            valid_keys = set(dim[dim_key].dropna())
            fact_values = fact[fact_col]

            # NULL dihitung terpisah; tidak dianggap orphan.
            non_null = fact_values.dropna()
            orphan_count = int(
                (~non_null.isin(valid_keys)).sum()
            )

            results.append({
                "check": "Foreign key orphan",
                "table": FACT_FILE,
                "column": fact_col,
                "issue_count": orphan_count,
                "status": (
                    "OK" if orphan_count == 0
                    else "Perlu diperiksa"
                ),
            })

    return results


# =====================================================
# 7. JALANKAN SEMUA AUDIT
# =====================================================

def main():
    tables = {}

    # Baca data sumber dari MySQL OLTP jika tersedia, fallback ke CSV
    source = None
    source_name = "superstore_dirty.csv"
    try:
        from etl_pipeline import extract_data
        source = extract_data()
        source_name = "superstore_oltp.superstore_raw"
        print("[INFO] Berhasil membaca data sumber mentah dari MySQL OLTP ('superstore_raw').")
    except Exception as e:
        print(f"[INFO] Koneksi MySQL OLTP tidak aktif/gagal ({e}). Menggunakan fallback CSV: {SOURCE_FILE}")
        source = read_csv(SOURCE_FILE)

    if source is None:
        raise FileNotFoundError(
            f"Data sumber tidak ditemukan di MySQL OLTP maupun CSV: {SOURCE_FILE}"
        )

    # Normalisasi string kosong menjadi NULL pada data sumber
    source = source.replace(r"^\s*$", pd.NA, regex=True)
    tables[source_name] = source

    # Baca seluruh tabel DWH.
    for filename in [*DIMENSIONS.keys(), FACT_FILE]:
        df = read_csv(DWH_DIR / filename)

        if df is not None:
            tables[filename] = df

    all_nulls = []
    all_duplicates = []
    all_logic = []
    summaries = []

    for filename, df in tables.items():
        print(f"\nAudit: {filename}")

        summaries.append({
            "table": filename,
            "row_count": len(df),
            "column_count": len(df.columns),
            "total_null_cells": int(
                df.isna().sum().sum()
            ),
            "full_row_duplicates": int(
                df.duplicated().sum()
            ),
        })

        all_nulls.extend(audit_null(df, filename))

        key_columns = None

        if filename in DIMENSIONS:
            key_columns = [DIMENSIONS[filename]]

        all_duplicates.extend(
            audit_duplicates(df, filename, key_columns)
        )

        all_logic.extend(
            audit_business_logic(df, filename)
        )

    key_results = audit_dwh_keys(tables)

    # Simpan semua hasil.
    pd.DataFrame(summaries).to_csv(
        REPORT_DIR / "table_summary.csv", index=False
    )

    pd.DataFrame(all_nulls).to_csv(
        REPORT_DIR / "null_audit.csv", index=False
    )

    pd.DataFrame(all_duplicates).to_csv(
        REPORT_DIR / "duplicate_audit.csv", index=False
    )

    pd.DataFrame(all_logic).to_csv(
        REPORT_DIR / "business_logic_audit.csv", index=False
    )

    pd.DataFrame(key_results).to_csv(
        REPORT_DIR / "key_integrity_audit.csv", index=False
    )

    report = {
        "tables": summaries,
        "key_integrity": key_results,
    }

    with open(
        REPORT_DIR / "audit_summary.json",
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(report, file, indent=4, ensure_ascii=False)

    print("\n===== RINGKASAN TABEL =====")
    print(pd.DataFrame(summaries).to_string(index=False))

    print("\n===== INTEGRITAS PRIMARY KEY / FOREIGN KEY =====")
    print(pd.DataFrame(key_results).to_string(index=False))

    print(f"\nLaporan audit tersimpan di: {REPORT_DIR}")


if __name__ == "__main__":
    main()
