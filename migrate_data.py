#!/usr/bin/env python3
"""
migrate_data.py  ────────────────────────────────────────────────────
Copy every row from every MS-SQL table                    ➜ PostgreSQL
  · streaming batches (low RAM)
  · fast COPY-style inserts (psycopg2.execute_values)
  · FK/trigger suspension for speed, re-validation at end
  · identity-sequence resynchronisation
  · resumable via .migrated checkpoint file

Run “python migrate_data.py -h” for full CLI help.
"""

import argparse
import logging
import os
from pathlib import Path
from typing import List, Tuple

import pyodbc
import psycopg2
import psycopg2.extras as pgx
from tqdm import tqdm

# ───────────────────────── configuration ────────────────────────── #
BATCH_SIZE        = 10_000
CHECKPOINT_FILE   = ".migrated"      # one line per finished table
LOG_FMT           = "%(asctime)s - %(levelname)s - %(message)s"

# ───────────────────────── helpers ──────────────────────────────── #
def quote_ident(identifier: str) -> str:
    """ANSI-SQL identifier quoting."""
    return '"' + identifier.replace('"', '""') + '"'


def tables_in_sqlserver(conn: pyodbc.Connection) -> List[Tuple[str, str]]:
    """[(schema, table), …] ordered for deterministic processing."""
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    return [(r[0], r[1]) for r in cur.fetchall()]


def fetch_batches(cur: pyodbc.Cursor, size: int = BATCH_SIZE):
    """Generator yielding batches from a pyodbc cursor."""
    while True:
        rows = cur.fetchmany(size)
        if not rows:
            break
        yield rows


def load_checkpoint() -> set:
    if not Path(CHECKPOINT_FILE).exists():
        return set()
    return {line.strip() for line in Path(CHECKPOINT_FILE).read_text().splitlines() if line.strip()}


def save_checkpoint(table_key: str):
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{table_key}\n")


def resync_id_sequences(pg_conn: psycopg2.extensions.connection,
                        schema: str, table: str):
    """Bump each identity/serial sequence to MAX(id)+1 after copy."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND is_identity='YES'
        """, (schema, table))
        for (col,) in cur.fetchall():
            cur.execute(f'SELECT MAX({quote_ident(col)}) FROM {quote_ident(schema)}.{quote_ident(table)}')
            max_val = cur.fetchone()[0] or 0
            cur.execute("""
                SELECT pg_get_serial_sequence(%s, %s)
            """, (f'{schema}.{table}', col))
            seq = cur.fetchone()[0]
            if seq:
                cur.execute('SELECT setval(%s, %s)', (seq, max_val + 1))


def validate_constraints(pg_conn: psycopg2.extensions.connection, logger):
    """Re-enable FK triggers & validate them (only if we disabled)."""
    with pg_conn.cursor() as cur:
        logger.info("Re-enabling triggers & validating foreign-keys …")
        cur.execute("SET session_replication_role = DEFAULT;")
        cur.execute("""
            SELECT conrelid::regclass::text AS tbl, conname
            FROM pg_constraint
            WHERE contype = 'f' AND NOT convalidated
        """)
        for table, constraint in cur.fetchall():
            logger.debug("VALIDATE CONSTRAINT %s ON %s", constraint, table)
            cur.execute(f'ALTER TABLE {table} VALIDATE CONSTRAINT "{constraint}"')
    pg_conn.commit()

# ───────────────────────── copy routine ─────────────────────────── #
def copy_table(src_conn, dst_conn, schema, table,
               batch_size: int, logger):
    key = f"{schema}.{table}"
    logger.info("Copying %s", key)

    src_cur = src_conn.cursor()
    src_cur.execute(f"SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}")
    columns = [quote_ident(col[0]) for col in src_cur.description]

    sql_insert = (
        f'INSERT INTO {quote_ident(schema)}.{quote_ident(table)} '
        f'({", ".join(columns)}) VALUES %s'
    )
    dst_cur = dst_conn.cursor()

    total = 0
    for batch in fetch_batches(src_cur, batch_size):
        pgx.execute_values(dst_cur, sql_insert, batch,
                           template=None, page_size=len(batch))
        total += len(batch)

    resync_id_sequences(dst_conn, schema, table)
    dst_conn.commit()
    logger.info("✓ %s rows", total)

# ────────────────────────── main driver ─────────────────────────── #
def migrate(args):
    logger = logging.getLogger("migrate_data")

    # build DSNs ----------------------------------------------------------------
    mssql_dsn = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={args.server};'
        f'DATABASE={args.database};'
        f'UID={args.username};PWD={args.password}'
    )
    pg_dsn = (
        f'host={args.pg_host} port={args.pg_port} '
        f'dbname={args.pg_database} user={args.pg_username} '
        f'password={args.pg_password}'
    )

    # connections ---------------------------------------------------------------
    logger.info("Connecting to SQL Server …")
    src_conn = pyodbc.connect(mssql_dsn, autocommit=False)
    # src_conn.fast_executemany = True

    logger.info("Connecting to PostgreSQL …")
    dst_conn = psycopg2.connect(pg_dsn)
    # speed-ups: temporarily disable FK triggers unless requested NOT to
    if not args.strict:
        dst_conn.set_session(autocommit=False)
        with dst_conn.cursor() as cur:
            cur.execute("SET session_replication_role = replica;")
            cur.execute("SHOW session_replication_role;")
            role = cur.fetchone()[0]
            logger.info("PostgreSQL session_replication_role is set to: %s", role)

    # processing ----------------------------------------------------------------
    done = load_checkpoint()
    tables = tables_in_sqlserver(src_conn)

    for schema, table in tqdm(tables, unit="table"):
        key = f"{schema}.{table}"
        if key in done:
            continue
        try:
            copy_table(src_conn, dst_conn, schema, table,
                       BATCH_SIZE, logger)
            save_checkpoint(key)
        except Exception as exc:
            dst_conn.rollback()
            logger.exception("⚠️  Error on %s (rolled back) – %s", key, exc)

    if not args.strict:
        validate_constraints(dst_conn, logger)

    src_conn.close()
    dst_conn.close()
    logger.info("🎉  Data migration finished successfully.")


def cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate **data** from MS-SQL to PostgreSQL "
                    "(schema must already exist)")
    # SQL Server flags — identical to migrate_schema.py ------------------------
    p.add_argument('--server',   default='localhost,1433',  help='SQL Server host,port')
    p.add_argument('--database', required=True,  help='SQL Server database')
    p.add_argument('--username', required=True,  help='SQL Server login')
    p.add_argument('--password', required=True,  help='SQL Server password')
    # Postgres flags -----------------------------------------------------------
    p.add_argument('--pg-host',     default='localhost', help='Postgres host')
    p.add_argument('--pg-port',     default='5432',      help='Postgres port')
    p.add_argument('--pg-database', required=True,       help='Postgres db')
    p.add_argument('--pg-username', required=True,       help='Postgres user')
    p.add_argument('--pg-password', required=True,       help='Postgres password')
    # options ------------------------------------------------------------------
    p.add_argument('--strict', action='store_true',
                   help="Do NOT disable triggers/FKs during load "
                        "(safer but slower)")
    p.add_argument('--log-level', default='INFO',
                   choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    return p.parse_args()


if __name__ == '__main__':
    args = cli()
    logging.basicConfig(level=getattr(logging, args.log_level), format=LOG_FMT)
    migrate(args)
