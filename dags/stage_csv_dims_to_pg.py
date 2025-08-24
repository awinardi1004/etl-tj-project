from __future__ import annotations
import os
import pendulum
from pathlib import Path
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import psycopg2

TZ = pendulum.timezone("Asia/Jakarta")
PG_DW = os.getenv("PG_DW_CONN_ID", "pg_dwh") 
CSV_DIR = Path(os.getenv("DATA_DIR", "/usr/local/airflow/data")) 

def _pg_conn(conn_id: str):
    c = BaseHook.get_connection(conn_id)
    dsn = f"host={c.host} port={c.port or 5432} dbname={c.schema} user={c.login} password={c.get_password()}"
    return psycopg2.connect(dsn)

@dag(
    dag_id="stage_csv_dims_to_pg_dwh",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=TZ),
    catchup=False,
    tags=["stage","csv","pg_dwh","elt","no-provider"],
)
def stage_csv_dims_to_pg_dwh():

    @task()
    def create_schema_and_tables():
        sql = """
        SELECT pg_advisory_lock(hashtext('dw_ddl_lock'));

        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='dw') THEN
            EXECUTE 'CREATE SCHEMA dw';
          END IF;
        EXCEPTION WHEN duplicate_schema THEN
          NULL;
        END $$;

        CREATE OR REPLACE FUNCTION dw.norm_body(s text)
        RETURNS text LANGUAGE sql IMMUTABLE AS $FN$
          WITH t AS (SELECT regexp_replace(s, '[^A-Za-z0-9]', '', 'g') AS only)
          SELECT CASE
            WHEN s IS NULL OR t.only = '' THEN NULL
            ELSE upper(substring(t.only from '([A-Za-z]{3})'))
                 || '-' ||
                 lpad(substring(t.only from '([0-9]{1,3})'), 3, '0')
          END
          FROM t;
        $FN$;

        DO $$
        BEGIN
          CREATE TABLE IF NOT EXISTS dw.routes (
            route_code text PRIMARY KEY,
            route_name text
          );
        EXCEPTION WHEN duplicate_table THEN NULL; END $$;

        DO $$
        BEGIN
          CREATE TABLE IF NOT EXISTS dw.shelter_corridor (
            shelter_name_var text PRIMARY KEY,
            corridor_code integer,
            corridor_name text
          );
        EXCEPTION WHEN duplicate_table THEN NULL; END $$;

        DO $$
        BEGIN
          CREATE TABLE IF NOT EXISTS dw.realisasi_bus (
            tanggal_realisasi date,
            bus_body_no_norm  text,
            rute_realisasi    text
          );
        EXCEPTION WHEN duplicate_table THEN NULL; END $$;

        DO $$
        BEGIN
          CREATE TABLE IF NOT EXISTS dw._stg_routes (
            route_code text, route_name text
          );
        EXCEPTION WHEN duplicate_table THEN NULL; END $$;

        DO $$
        BEGIN
          CREATE TABLE IF NOT EXISTS dw._stg_shelter_corridor (
            shelter_name_var text, corridor_code text, corridor_name text
          );
        EXCEPTION WHEN duplicate_table THEN NULL; END $$;

        DO $$
        BEGIN
          CREATE TABLE IF NOT EXISTS dw._stg_realisasi_bus (
            tanggal_realisasi text, bus_body_no text, rute_realisasi text
          );
        EXCEPTION WHEN duplicate_table THEN NULL; END $$;

        SELECT pg_advisory_unlock(hashtext('dw_ddl_lock'));
        """
        with _pg_conn(PG_DW) as conn, conn.cursor() as cur:
            cur.execute(sql); conn.commit()

    @task()
    def copy_csv_to_staging():
        files = {
            "dw._stg_routes": CSV_DIR / "dummy_routes.csv",
            "dw._stg_shelter_corridor": CSV_DIR / "dummy_shelter_corridor.csv",
            "dw._stg_realisasi_bus": CSV_DIR / "dummy_realisasi_bus.csv",
        }
        with _pg_conn(PG_DW) as conn, conn.cursor() as cur:
            # kosongkan staging agar idempotent
            cur.execute("TRUNCATE dw._stg_routes; TRUNCATE dw._stg_shelter_corridor; TRUNCATE dw._stg_realisasi_bus;")
            for table, path in files.items():
                if not path.exists():
                    raise FileNotFoundError(f"CSV tidak ditemukan: {path}")
                with open(path, "r", encoding="utf-8") as f:
                    cur.copy_expert(sql=f"COPY {table} FROM STDIN WITH CSV HEADER", file=f)
            conn.commit()

    @task()
    def transform_and_upsert_to_final():
        sql = """
        INSERT INTO dw.routes(route_code, route_name)
        SELECT trim(route_code), route_name
        FROM dw._stg_routes
        WHERE route_code IS NOT NULL
        ON CONFLICT (route_code) DO UPDATE
          SET route_name = EXCLUDED.route_name;

        INSERT INTO dw.shelter_corridor(shelter_name_var, corridor_code, corridor_name)
        SELECT trim(shelter_name_var),
               NULLIF(trim(corridor_code),'')::integer,
               corridor_name
        FROM dw._stg_shelter_corridor
        WHERE shelter_name_var IS NOT NULL
        ON CONFLICT (shelter_name_var) DO UPDATE
          SET corridor_code = EXCLUDED.corridor_code,
              corridor_name = EXCLUDED.corridor_name;

        TRUNCATE dw.realisasi_bus;
        INSERT INTO dw.realisasi_bus(tanggal_realisasi, bus_body_no_norm, rute_realisasi)
        SELECT
          CASE
            WHEN tanggal_realisasi ~ '^\d{4}-\d{2}-\d{2}$'
              THEN to_date(tanggal_realisasi, 'YYYY-MM-DD')
            WHEN tanggal_realisasi ~ '^\d{2}/\d{2}/\d{4}$'
              THEN to_date(tanggal_realisasi, 'DD/MM/YYYY')
            ELSE NULL
          END AS tanggal_realisasi,
          dw.norm_body(bus_body_no) AS bus_body_no_norm,
          rute_realisasi
        FROM dw._stg_realisasi_bus;

        CREATE INDEX IF NOT EXISTS ix_routes_code    ON dw.routes(route_code);
        CREATE INDEX IF NOT EXISTS ix_shelter_code   ON dw.shelter_corridor(corridor_code);
        CREATE INDEX IF NOT EXISTS ix_real_body_norm ON dw.realisasi_bus(bus_body_no_norm);
        CREATE INDEX IF NOT EXISTS ix_real_tanggal   ON dw.realisasi_bus(tanggal_realisasi);
        ANALYZE dw.routes; ANALYZE dw.shelter_corridor; ANALYZE dw.realisasi_bus;
        """
        with _pg_conn(PG_DW) as conn, conn.cursor() as cur:
            cur.execute(sql); conn.commit()

    create_schema_and_tables() >> copy_csv_to_staging() >> transform_and_upsert_to_final()

stage_csv_dims_to_pg_dwh()
