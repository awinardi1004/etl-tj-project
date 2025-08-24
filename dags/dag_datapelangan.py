
from __future__ import annotations
import os
import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context
import psycopg2


log = logging.getLogger(__name__)

TZ = pendulum.timezone("Asia/Jakarta")
PG_SRC = os.getenv("PG_SRC_CONN_ID", "pg_src") 
PG_DW  = os.getenv("PG_DW_CONN_ID",  "pg_dwh")

def _pg_conn(conn_id: str):
    c = BaseHook.get_connection(conn_id)
    dsn = f"host={c.host} port={c.port or 5432} dbname={c.schema} user={c.login} password={c.get_password()}"
    return psycopg2.connect(dsn)

@dag(
    dag_id="dag_datapelangan",
    schedule="0 7 * * *",   
    start_date=pendulum.datetime(2025, 1, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["extract","transform","load","postgres","no-provider"]
)
def dag_datapelangan():

    @task(task_id="extract_transform_create_views_in_src")
    def create_views_in_src():
        ctx = get_current_context()
        ds  = ctx["ds"]
        log.info("[ET] Start create/update views di SOURCE untuk ds=%s", ds)

        sql = """
        SELECT pg_advisory_lock(hashtext('src_ddl_lock'));
        CREATE SCHEMA IF NOT EXISTS src;

        DROP VIEW IF EXISTS src.vw_trx_bus_raw;
        DROP VIEW IF EXISTS src.vw_trx_halte_raw;

        CREATE OR REPLACE FUNCTION src.norm_body(s text)
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

        CREATE OR REPLACE FUNCTION src.to_bool_safe(x anyelement)
        RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
        DECLARE t text := upper(coalesce(x::text,''));
        BEGIN
          IF t IN ('TRUE','T','1','Y','YES')  THEN RETURN TRUE;  END IF;
          IF t IN ('FALSE','F','0','N','NO') THEN RETURN FALSE; END IF;
          IF t = '' THEN RETURN NULL; END IF;
          BEGIN RETURN x::boolean; EXCEPTION WHEN others THEN RETURN NULL; END;
        END $$;

        CREATE VIEW src.vw_trx_bus_raw AS
        SELECT
          CAST(waktu_transaksi AS timestamp)::date AS tanggal,
          UPPER(card_type_var)                     AS card_type,
          fare_int::numeric(18,2)                  AS amount,
          UPPER(status_var)                        AS status_var,
          src.norm_body(no_body_var)               AS no_body_norm,
          src.to_bool_safe(gate_in_boo)            AS gate_in_boo
        FROM src.dummy_transaksi_bus;

        CREATE VIEW src.vw_trx_halte_raw AS
        SELECT
          CAST(waktu_transaksi AS timestamp)::date AS tanggal,
          UPPER(card_type_var)                     AS card_type,
          fare_int::numeric(18,2)                  AS amount,
          UPPER(status_var)                        AS status_var,
          shelter_name_var,
          src.to_bool_safe(gate_in_boo)            AS gate_in_boo
        FROM src.dummy_transaksi_halte;

        SELECT pg_advisory_unlock(hashtext('src_ddl_lock'));
        """
        with _pg_conn(PG_SRC) as conn, conn.cursor() as cur:
            cur.execute(sql); conn.commit()

            cur.execute(
                "SELECT COUNT(*) FROM src.dummy_transaksi_bus "
                "WHERE UPPER(status_var)='S' AND CAST(waktu_transaksi AS date)=DATE %s",
                (ds,),
            )
            bus_rows = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM src.dummy_transaksi_halte "
                "WHERE UPPER(status_var)='S' AND CAST(waktu_transaksi AS date)=DATE %s",
                (ds,),
            )
            halte_rows = cur.fetchone()[0]

        log.info("[ET] Views siap. Row kandidat (status='S') untuk %s -> bus=%s, halte=%s", ds, bus_rows, halte_rows)
        return {"bus_rows": bus_rows, "halte_rows": halte_rows, "ds": ds}

    @task(task_id="load_to_dwh_prepare_paritions")
    def dw_setup_and_partition():
        ds = get_current_context()["ds"]
        log.info("[LD] Setup DWH & partisi untuk ds=%s", ds)

        sql = f"""
        SELECT pg_advisory_lock(hashtext('dw_ddl_lock'));
        CREATE SCHEMA IF NOT EXISTS dw;

        CREATE TABLE IF NOT EXISTS dw.agg_by_card (
          tanggal date NOT NULL,
          card_type text NOT NULL,
          gate_in_boo boolean,
          pelanggan_count bigint NOT NULL,
          amount_sum numeric(18,2) NOT NULL
        ) PARTITION BY RANGE (tanggal);

        CREATE TABLE IF NOT EXISTS dw.agg_by_route (
          tanggal date NOT NULL,
          route_code text,
          route_name text,
          gate_in_boo boolean,
          pelanggan_count bigint NOT NULL,
          amount_sum numeric(18,2) NOT NULL
        ) PARTITION BY RANGE (tanggal);

        CREATE TABLE IF NOT EXISTS dw.agg_by_tariff (
          tanggal date NOT NULL,
          tarif numeric(18,2) NOT NULL,
          gate_in_boo boolean,
          pelanggan_count bigint NOT NULL
        ) PARTITION BY RANGE (tanggal);

        DO $$
        DECLARE d date := DATE '{ds}';
        BEGIN
          EXECUTE format('CREATE TABLE IF NOT EXISTS dw.agg_by_card_p%s  PARTITION OF dw.agg_by_card  FOR VALUES FROM (%L) TO (%L)',
                         to_char(d,'YYYYMMDD'), d, d+1);
          EXECUTE format('CREATE TABLE IF NOT EXISTS dw.agg_by_route_p%s PARTITION OF dw.agg_by_route FOR VALUES FROM (%L) TO (%L)',
                         to_char(d,'YYYYMMDD'), d, d+1);
          EXECUTE format('CREATE TABLE IF NOT EXISTS dw.agg_by_tariff_p%s PARTITION OF dw.agg_by_tariff FOR VALUES FROM (%L) TO (%L)',
                         to_char(d,'YYYYMMDD'), d, d+1);
        END $$;

        SELECT pg_advisory_unlock(hashtext('dw_ddl_lock'));
        """
        with _pg_conn(PG_DW) as conn, conn.cursor() as cur:
            cur.execute(sql); conn.commit()

        log.info("[LD] Partisi harian dibuat/ada: agg_by_* untuk %s", ds)
        return {"ds": ds}

    @task(task_id="load_to_dwh_aggregate")
    def load_aggs():
        ds  = get_current_context()["ds"]
        src = BaseHook.get_connection(PG_SRC)
        conninfo = f"host={src.host} port={src.port or 5432} dbname={src.schema} user={src.login} password={src.get_password()}"
        log.info("[LD] Mulai agregasi & load ke DWH untuk ds=%s", ds)

        sql = f"""
        SELECT pg_advisory_lock(hashtext('dw_load_lock'));
        CREATE EXTENSION IF NOT EXISTS dblink;

        WITH
        bus AS (
          SELECT * FROM dblink('{conninfo}',
            'SELECT tanggal, card_type, amount, status_var, no_body_norm, gate_in_boo
               FROM src.vw_trx_bus_raw
               WHERE status_var=''S'' AND tanggal = DATE ''{ds}'' ')
          AS t(tanggal date, card_type text, amount numeric, status_var text, no_body_norm text, gate_in_boo boolean)
        ),
        halte AS (
          SELECT * FROM dblink('{conninfo}',
            'SELECT tanggal, card_type, amount, status_var, shelter_name_var, gate_in_boo
               FROM src.vw_trx_halte_raw
               WHERE status_var=''S'' AND tanggal = DATE ''{ds}'' ')
          AS t(tanggal date, card_type text, amount numeric, status_var text, shelter_name_var text, gate_in_boo boolean)
        ),

        ins_card AS (
          INSERT INTO dw.agg_by_card(tanggal, card_type, gate_in_boo, pelanggan_count, amount_sum)
          SELECT tanggal, card_type, gate_in_boo, COUNT(*), SUM(amount)
          FROM (
            SELECT tanggal, card_type, amount, gate_in_boo FROM bus
            UNION ALL
            SELECT tanggal, card_type, amount, gate_in_boo FROM halte
          ) x
          GROUP BY tanggal, card_type, gate_in_boo
          RETURNING 1
        ),

        ins_route AS (
          INSERT INTO dw.agg_by_route(tanggal, route_code, route_name, gate_in_boo, pelanggan_count, amount_sum)
          SELECT tanggal, route_code, route_name, gate_in_boo, COUNT(*), SUM(amount)
          FROM (
            SELECT b.tanggal,
                   rb.rute_realisasi::text AS route_code,
                   r.route_name,
                   b.gate_in_boo,
                   b.amount
            FROM bus b
            JOIN dw.realisasi_bus rb ON rb.bus_body_no_norm = b.no_body_norm
            LEFT JOIN dw.routes r     ON r.route_code = rb.rute_realisasi::text

            UNION ALL
            SELECT h.tanggal,
                   (sc.corridor_code)::text AS route_code,
                   r.route_name,
                   h.gate_in_boo,
                   h.amount
            FROM halte h
            LEFT JOIN dw.shelter_corridor sc ON sc.shelter_name_var = h.shelter_name_var
            LEFT JOIN dw.routes r            ON r.route_code = (sc.corridor_code)::text
          ) y
          GROUP BY tanggal, route_code, route_name, gate_in_boo
          RETURNING 1
        )

        INSERT INTO dw.agg_by_tariff(tanggal, tarif, gate_in_boo, pelanggan_count)
        SELECT tanggal, amount AS tarif, gate_in_boo, COUNT(*)
        FROM (
          SELECT tanggal, amount, gate_in_boo FROM bus
          UNION ALL
          SELECT tanggal, amount, gate_in_boo FROM halte
        ) z
        GROUP BY tanggal, amount, gate_in_boo;

        SELECT
          (SELECT COUNT(*) FROM dw.agg_by_card  WHERE tanggal = DATE '{ds}')    AS card_rows,
          (SELECT COUNT(*) FROM dw.agg_by_route WHERE tanggal = DATE '{ds}')    AS route_rows,
          (SELECT COUNT(*) FROM dw.agg_by_tariff WHERE tanggal = DATE '{ds}')   AS tariff_rows;
        """
        with _pg_conn(PG_DW) as conn, conn.cursor() as cur:
            cur.execute(sql)
            card_rows, route_rows, tariff_rows = cur.fetchone()
            conn.commit()

        log.info("[LD] Selesai. Rows tersimpan untuk %s → agg_by_card=%s, agg_by_route=%s, agg_by_tariff=%s",
                 ds, card_rows, route_rows, tariff_rows)
        return {"ds": ds, "card_rows": card_rows, "route_rows": route_rows, "tariff_rows": tariff_rows}

    create_views_in_src() >> dw_setup_and_partition() >> load_aggs()

dag_datapelangan()
