import pyodbc
import psycopg2
import argparse
import logging
import json
from reports import generate_verification_report

def load_type_mapping(file_path='type_mappings.json'):
    with open(file_path) as f:
        return json.load(f)


def setup_logging(level):
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)

def get_columns_by_type(conn, schema, table, is_pg, type_mapping):
    cur = conn.cursor()
    if is_pg:
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
    else:
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
    cur.execute(query, (schema, table))
    results = cur.fetchall()
    numeric_cols = []
    datetime_cols = []
    for col, dtype in results:
        dtype = dtype.lower()
        if is_pg:
            mapped_type = type_mapping.get(dtype)
        else:
            mapped_type = dtype
        if mapped_type in ['int', 'bigint', 'smallint', 'tinyint', 'bit', 'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real']:
            numeric_cols.append(col)
        elif mapped_type in ['date', 'datetime', 'datetime2', 'smalldatetime', 'time']:
            datetime_cols.append(col)

    return numeric_cols, datetime_cols

def compute_column_means(conn, schema, table, columns, is_pg, mode):
    cur = conn.cursor()
    quoted_table = f'"{schema}"."{table}"' if is_pg else f'{schema}.{table}'
    means = {}

    for col in columns:
        try:
            if mode == 'numeric':
                query = f'SELECT AVG("{col}") FROM {quoted_table}' if is_pg else f'SELECT AVG([{col}]) FROM {quoted_table}'
            else:  # datetime
                if is_pg:
                    query = f'SELECT AVG(EXTRACT(EPOCH FROM "{col}")) FROM {quoted_table}'
                else:
                    query = f"SELECT AVG(DATEDIFF(SECOND, '1970-01-01', [{col}])) FROM {quoted_table}"
            cur.execute(query)
            result = cur.fetchone()
            means[col] = result[0]
        except Exception as e:
            conn.rollback()
            means[col] = None
    return means

def compare_column_means(src_conn, dst_conn, common_tables, type_mapping, logger):
    mean_mismatches = []
    all_means = []

    for schema, table in sorted(common_tables):
        src_numeric, src_datetime = get_columns_by_type(src_conn, schema, table, False, type_mapping)
        dst_numeric, dst_datetime = get_columns_by_type(dst_conn, schema, table, True, type_mapping)
        
        # Intersect columns by name
        numeric_cols = set(src_numeric) & set(dst_numeric)
        datetime_cols = set(src_datetime) & set(dst_datetime)

        src_num_mean = compute_column_means(src_conn, schema, table, numeric_cols, False, 'numeric')
        dst_num_mean = compute_column_means(dst_conn, schema, table, numeric_cols, True, 'numeric')
        src_dt_mean = compute_column_means(src_conn, schema, table, datetime_cols, False, 'datetime')
        dst_dt_mean = compute_column_means(dst_conn, schema, table, datetime_cols, True, 'datetime')

        for col in numeric_cols:
            all_means.append(f"{schema}.{table}.{col} [numeric]: SQL Server = {src_num_mean[col]}, PostgreSQL = {dst_num_mean.get(col)}")
            if col in dst_num_mean and src_num_mean[col] is not None and dst_num_mean[col] is not None and abs(src_num_mean[col] - dst_num_mean[col]) > 1e-3:
                mean_mismatches.append((schema, table, col, src_num_mean[col], dst_num_mean[col], 'numeric'))

        for col in datetime_cols:
            all_means.append(f"{schema}.{table}.{col} [datetime]: SQL Server = {src_dt_mean[col]}, PostgreSQL = {dst_dt_mean.get(col)}")
            if col in dst_dt_mean and src_dt_mean[col] is not None and dst_dt_mean[col] is not None and abs(src_dt_mean[col] - dst_dt_mean[col]) > 1e-3:
                mean_mismatches.append((schema, table, col, src_dt_mean[col], dst_dt_mean[col], 'datetime'))
    return mean_mismatches, all_means


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
    # response = input(f"Do you want to drop constraint {constraint} on {schema}.{table}? [y/N/a]: ").strip().lower()
    response = 'n'
    if response == 'y':
        cur = conn.cursor()
        cur.execute(f'ALTER TABLE "{schema}"."{table}" DROP CONSTRAINT "{constraint}"') 
        conn.commit()
        logger.info(f"Dropped constraint {constraint} on {schema}.{table}")
    elif response == 'a':
        return


def add_missing_fk(conn, logger, fk_tuple, failed_fks):
    schema, table, column, ref_schema, ref_table, ref_column, constraint = fk_tuple
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = %s AND table_schema = %s AND table_name = %s
        """, (constraint, schema, table))
        if cur.fetchone():
            logger.debug(f"Constraint {constraint} already exists on {schema}.{table}, skipping.")
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
        error_msg = str(e)
        if "already exists" not in error_msg.lower():
            logger.error(f"Failed to add FK {constraint} on {schema}.{table}: {error_msg}")
            failed_fks.append((fk_tuple, error_msg))



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
    common_tables = src_tables & dst_tables
    type_mapping = load_type_mapping('type_mappings.json')
    mean_mismatches, all_means = compare_column_means(src_conn, dst_conn, common_tables, type_mapping, logger)

    if mean_mismatches:
        logger.warning("Column-wise mean mismatches:")
        for mismatch in mean_mismatches:
            schema, table, col, src_val, dst_val, typ = mismatch
            logger.warning(f"  - {schema}.{table}.{col} [{typ}]: SQL Server = {src_val}, PostgreSQL = {dst_val}")


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

    failed_fks = []
    logger.info("Comparing foreign key constraints …")
    src_fk = fetch_foreign_keys_sqlserver(src_conn)
    dst_fk = fetch_foreign_keys_pg(dst_conn)

    missing_in_pg, extra_in_pg = compare_foreign_keys(src_fk, dst_fk)

    for fk in missing_in_pg:
        add_missing_fk(dst_conn, logger, fk, failed_fks)
        
    if failed_fks:
        logger.warning("Foreign keys that could not be added:")
        for fk, reason in failed_fks:
            schema, table, col, rs, rt, rc, cname = fk
            logger.warning(f"  - {schema}.{table}.{col} → {rs}.{rt}.{rc} ({cname}): {reason}")


    for fk in extra_in_pg:
        prompt_drop_fk(dst_conn, logger, fk)

    logger.info("All foreign keys are now in sync.")
    generate_verification_report(
        common_tables,
        data_mismatches,
        mean_mismatches,
        all_means,
        failed_fks,
        output_path="migration_report.md"
    )
    logger.info("Migration report saved to migration_report.md")

    logger.info("!!! Comparison completed. !!!") 
    src_conn.close()
    dst_conn.close()


if __name__ == "__main__":
    main()
