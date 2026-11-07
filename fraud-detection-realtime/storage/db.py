"""
db.py - akses PostgreSQL (dipakai Spark foreachBatch dan dasbor Streamlit)
=========================================================================

    python storage/db.py --init      # buat tabel (jika belum ada)
    python storage/db.py --reset     # hapus & buat ulang semua tabel (mulai demo dari nol)
    python storage/db.py --status    # jumlah baris per tabel
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

SCHEMA = Path(__file__).with_name("schema.sql")
TABLES = ["alerts", "predictions", "rejected_records", "batch_metrics", "agg_minute", "transactions"]


def connect(retries: int = 30):
    last = None
    for _ in range(retries):
        try:
            return psycopg2.connect(host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
                                    user=config.PG_USER, password=config.PG_PASSWORD, connect_timeout=5)
        except psycopg2.OperationalError as e:      # container postgres mungkin belum siap
            last = e
            time.sleep(2)
    raise RuntimeError(f"PostgreSQL tidak bisa dihubungi di {config.PG_HOST}:{config.PG_PORT}: {last}")


def init_schema():
    with connect() as c, c.cursor() as cur:
        cur.execute(SCHEMA.read_text(encoding="utf-8"))


def reset():
    """Hapus dan buat ulang semua tabel (juga menerapkan perubahan skema terbaru)."""
    with connect() as c, c.cursor() as cur:
        cur.execute("DROP VIEW IF EXISTS v_scored; DROP TABLE IF EXISTS " + ", ".join(TABLES) + " CASCADE")
    init_schema()


def insert_rows(table: str, df: pd.DataFrame, conflict: str = "DO NOTHING", key: str | None = None) -> int:
    """INSERT massal; ON CONFLICT membuat penulisan idempotent (aman jika micro-batch diulang)."""
    if df is None or df.empty:
        return 0
    cols = list(df.columns)
    rows = [tuple(None if (isinstance(v, float) and v != v) else v for v in r)
            for r in df.astype(object).where(pd.notna(df), None).itertuples(index=False, name=None)]
    on = f"ON CONFLICT ({key}) {conflict}" if key else "ON CONFLICT DO NOTHING"
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s {on}"
    with connect() as c, c.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
        return cur.rowcount


def upsert_agg(df: pd.DataFrame) -> int:
    upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in df.columns if c not in ("window_start", "source"))
    return insert_rows("agg_minute", df, conflict=f"DO UPDATE SET {upd}, updated_at = now() AT TIME ZONE 'utc'",
                       key="window_start, source")


def query(sql: str, params=None) -> pd.DataFrame:
    import warnings
    with connect(retries=3) as c, warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)     # pandas menyarankan SQLAlchemy; psycopg2 sudah cukup
        return pd.read_sql_query(sql, c, params=params)


def status() -> dict:
    out = {}
    with connect() as c, c.cursor() as cur:
        for t in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            out[t] = cur.fetchone()[0]
    return out


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true")
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()
    if a.init or a.reset:
        init_schema()
        print("schema OK")
    if a.reset:
        reset()
        print("semua tabel dikosongkan")
    print(status())
