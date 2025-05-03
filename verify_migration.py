import pyodbc
import psycopg2
import argparse
import logging
from collections import defaultdict


def setup_logging(level):
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)


def fetch_sqlserver_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    return set((row[0], row[1]) for row in cur.fetchall())


def fetch_postgres_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
    """)
    return set((row[0], row[1]) for row in cur.fetchall())


def fetch_row_count(conn, schema, table, is_pg):
    cur = conn.cursor()
    quoted = f'"{schema}"."{table}"' if is_pg else f'{schema}.{table}'
    try:
        cur.execute(f'SELECT COUNT(*) FROM {quoted}')
        return cur.fetchone()[0]
    except Exception:
        return None


def compare_data_counts(src_conn, dst_conn, tables, logger):
    mismatches = []
    for schema, table in sorted(tables):
        src_count = fetch_row_count(src_conn, schema, table, is_pg=False)
        dst_count = fetch_row_count(dst_conn, schema, table, is_pg=True)
        if src_count != dst_count:
            mismatches.append((schema, table, src_count, dst_count))
    return mismatches


def fetch_foreign_keys_pg(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            tc.table_schema, tc.table_name, kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            tc.constraint_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE constraint_type = 'FOREIGN KEY'
    """)
    return set(
        (s, t, c, rs, rt, rc, cn)
        for s, t, c, rs, rt, rc, cn in cur.fetchall()
    )


def fetch_foreign_keys_sqlserver(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            sch1.name AS table_schema,
            tab1.name AS table_name,
            col1.name AS column_name,
            sch2.name AS foreign_table_schema,
            tab2.name AS foreign_table_name,
            col2.name AS foreign_column_name,
            fk.name AS constraint_name
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id
        INNER JOIN sys.schemas sch1 ON sch1.schema_id = tab1.schema_id
        INNER JOIN sys.columns col1 ON col1.column_id = fkc.parent_column_id AND col1.object_id = tab1.object_id
        INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id
        INNER JOIN sys.schemas sch2 ON sch2.schema_id = tab2.schema_id
        INNER JOIN sys.columns col2 ON col2.column_id = fkc.referenced_column_id AND col2.object_id = tab2.object_id
    """)
    return set(
        (s, t, c, rs, rt, rc, cn)
        for s, t, c, rs, rt, rc, cn in cur.fetchall()
    )


def compare_foreign_keys(src_fk, dst_fk):
    def normalize_fk(fk):
        return (fk[0], fk[1], fk[2], fk[3], fk[4], fk[5])  # exclude constraint name

    src_normalized = {normalize_fk(fk): fk for fk in src_fk}
    dst_normalized = {normalize_fk(fk): fk for fk in dst_fk}

    missing_in_pg = list(src_normalized[k] for k in src_normalized if k not in dst_normalized)
    extra_in_pg = list(dst_normalized[k] for k in dst_normalized if k not in src_normalized)

    return missing_in_pg, extra_in_pg


def prompt_drop_fk(conn, logger, fk_tuple):
    schema, table, column, ref_schema, ref_table, ref_column, constraint = fk_tuple
    logger.warning(f"Foreign key only in PostgreSQL: {fk_tuple}")
    response = input(f"Do you want to drop constraint {constraint} on {schema}.{table}? [y/N]: ").strip().lower()
    if response == 'y':
        cur = conn.cursor()
        cur.execute(f'ALTER TABLE "{schema}"."{table}" DROP CONSTRAINT "{constraint}"')
        conn.commit()
        logger.info(f"Dropped constraint {constraint} on {schema}.{table}")


def add_missing_fk(conn, logger, fk_tuple):
    schema, table, column, ref_schema, ref_table, ref_column, constraint = fk_tuple
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = %s AND table_schema = %s AND table_name = %s
        """, (constraint, schema, table))
        if cur.fetchone():
            logger.info(f"Constraint {constraint} already exists on {schema}.{table}, skipping.")
            return

        cur.execute(
            f'ALTER TABLE "{schema}"."{table}" '
            f'ADD CONSTRAINT "{constraint}" FOREIGN KEY ("{column}") '
            f'REFERENCES "{ref_schema}"."{ref_table}" ("{ref_column}")'
        )
        conn.commit()
        logger.info(f"Added constraint {constraint} on {schema}.{table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to add FK {constraint}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Compare SQL Server and PostgreSQL databases for consistency")
    parser.add_argument('--server', default='localhost,1433')
    parser.add_argument('--database', required=True)
    parser.add_argument('--username', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--pg-host', default='localhost')
    parser.add_argument('--pg-port', default='5432')
    parser.add_argument('--pg-database', required=True)
    parser.add_argument('--pg-username', required=True)
    parser.add_argument('--pg-password', required=True)
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])

    args = parser.parse_args()
    logger = setup_logging(args.log_level)

    src_dsn = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={args.server};"
        f"DATABASE={args.database};UID={args.username};PWD={args.password}"
    )
    pg_dsn = (
        f"host={args.pg_host} port={args.pg_port} dbname={args.pg_database} "
        f"user={args.pg_username} password={args.pg_password}"
    )

    src_conn = pyodbc.connect(src_dsn)
    dst_conn = psycopg2.connect(pg_dsn)

    logger.info("Comparing table lists …")
    src_tables = fetch_sqlserver_tables(src_conn)
    dst_tables = fetch_postgres_tables(dst_conn)

    missing_in_pg = src_tables - dst_tables
    extra_in_pg = dst_tables - src_tables

    if missing_in_pg:
        logger.warning("Tables missing in PostgreSQL:")
        for schema, table in sorted(missing_in_pg):
            logger.warning(f"  - {schema}.{table}")
    if extra_in_pg:
        logger.warning("Extra tables in PostgreSQL:")
        for schema, table in sorted(extra_in_pg):
            logger.warning(f"  - {schema}.{table}")

    common_tables = src_tables & dst_tables
    logger.info("Comparing row counts …")
    data_mismatches = compare_data_counts(src_conn, dst_conn, common_tables, logger)
    if data_mismatches:
        logger.warning("Row count mismatches:")
        for schema, table, src_count, dst_count in data_mismatches:
            logger.warning(f"  - {schema}.{table}: SQL Server = {src_count}, PostgreSQL = {dst_count}")

    logger.info("Comparing foreign key constraints …")
    src_fk = fetch_foreign_keys_sqlserver(src_conn)
    dst_fk = fetch_foreign_keys_pg(dst_conn)

    missing_in_pg, extra_in_pg = compare_foreign_keys(src_fk, dst_fk)

    for fk in missing_in_pg:
        add_missing_fk(dst_conn, logger, fk)

    for fk in extra_in_pg:
        prompt_drop_fk(dst_conn, logger, fk)

    logger.info("All foreign keys are now in sync.")


    logger.info("!!! Comparison completed. !!!") 
    src_conn.close()
    dst_conn.close()


if __name__ == "__main__":
    main()
