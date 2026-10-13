# Alur Pengerjaan
data_source -> Proses ETL Cleansing Data (etl_pipeline.py) -> Melakukan verifikasi apakah masih ada NULL atau tidak (audit.py) -> Pembentukan STAR SCHEMA

# Processing ETL to DWH

Hasil Audit:
Data Source:
               table  row_count  column_count  total_null_cells  full_row_duplicates
superstore_dirty.csv      10093            21               600                   95

dwh_output:
               table  row_count  column_count  total_null_cells  full_row_duplicates
    dim_customer.csv        793             4                 0                    0
        dim_date.csv       1434             7                 0                    0
    dim_location.csv        733             6                 0                    0
     dim_product.csv       1861             5                 0                    0
   dim_ship_mode.csv          4             2                 0                    0
      fact_sales.csv       9894            13                 0                    0

# Pembentukan Tabel Fakta

### Fakta 1 — fact_sales
Grain: satu baris per item pesanan (Row ID)
Menyimpan metrik penjualan untuk menganalisis total penjualan, kuantitas, diskon, dan keuntungan berdasarkan pelanggan, produk, wilayah, maupun tanggal.

- Total penjualan dan keuntungan berdasarkan tahun atau bulan.
- Produk dan kategori dengan keuntungan tertinggi.
- Penjualan menurut segmen pelanggan.
- Perbandingan penjualan antarwilayah dan kota.
- Perbandingan penjualan berdasarkan metode pengiriman.

### Fakta 2 - fact_customer_order
Grain: satu baris per pelanggan per pesanan (Customer ID + Order ID). Karena satu pesanan dapat berisi beberapa produk, data dari baris-baris produk harus diagregasi terlebih dahulu. Dataset Superstore menyediakan kolom yang diperlukan, seperti Order ID, Customer ID, Order Date, Sales, Quantity, dan Profit.

- Berapa rata-rata nilai penjualan setiap pesanan?
- Pelanggan mana yang paling sering melakukan pemesanan?
- Berapa rata-rata keuntungan per pesanan?
- Apakah pesanan dengan banyak produk menghasilkan keuntungan lebih tinggi?