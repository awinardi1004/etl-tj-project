# ETL TJ Project (Airflow + Astro)
Pipeline Airflow untuk memproses data transaksi bus & halte dan membuat tabel agregat harian di PostgreSQL DWH. Proyek berjalan dengan Astro (Dockerized Airflow).

## 1. Cara Menjalankan Pipeline
### A. Persiapan
1. Clone & masuk ke folder proyek
```
git clone https://github.com/awinardi1004/etl-tj-project.git
cd etl-tj-project
```

2. Pastikan Docker & Astro CLI terpasang, lalu mulai layanan:
```
astro dev start
```

3. Buka Airflow UI: http://localhost:8080 (login default: admin / admin).

### Konfigurasi Koneksi Airflow
Buat 2 Connections di Airflow (menu: Admin → Connections):
* pg_src → menunjuk database source_db : Host, Port, Database (schema), User, Password sesuai server sumber.
* pg_dwh → menunjuk database dwh : Host, Port, Database (schema), User, Password sesuai server tujuan.

### C. Jalankan Pipeline
1. Load dimensi dari CSV (sekali atau saat CSV berubah)
    * Pastikan file CSV ada di ./data:
        * dummy_routes.csv
        * dummy_shelter_corridor.csv
        * dummy_shelter_corridor.csv
    * Trigger DAG: stage_csv_dims_to_pg_dwh <br>
    (mengisi dw.routes, dw.shelter_corridor, dw.realisasi_bus)
2. ETL harian sumber → DWH (agregat)
    * DAG: dag_datapelangan (otomatis jalan 07:00 WIB, bisa manual trigger).
    * dw.agg_by_route (tanggal, route_code, route_name, gate_in_boo, pelanggan_count,amount_sum)
    * dw.agg_by_tariff (tanggal, tarif, gate_in_boo, pelanggan_count)
## 2. Dependency yang Dibutuhkan
Runtime
    * Docker + Docker Compose
    * Astro CLI
Python (requirements.txt)
```
psycopg2-binary==2.9.*
pendulum>=2.1
```
PostgreSQL
* DB sumber: source_db (schema src, berisi dummy_transaksi_bus, dummy_transaksi_halte)
* DB tujuan: dwh (schema dw)
* Ekstensi di DWH: dblink (dibuat otomatis oleh DAG)

## 3. Struktur Folder
```
.
├─ dags/
│  ├─ stage_csv_dims_to_pg_dwh_noprovider.py   # DAG 1: CSV → DWH (dimensi)
│  └─ dag_datapelangan.py                      # DAG 2: ETL harian sumber → DWH (agregat)
├─ data/                                       # CSV input untuk DAG 1
│  ├─ dummy_routes.csv
│  ├─ dummy_shelter_corridor.csv
│  └─ dummy_realisasi_bus.csv
├─ include/                                    
├─ plugins/
├─ package.txt                                
├─ requirements.txt
├─ Dockerfile
└─ README.md
```