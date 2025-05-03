import pyodbc
import argparse
import logging
import os
from pathlib import Path
from collections import defaultdict
import datetime
import psycopg2

# Track columns needing post-processing
casted_columns_map = defaultdict(list)

def setup_logging(level):
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)

def quote_ident(s):
    return '"' + s.replace('"', '""') + '"'

def quote_literal(val):
    if val is None:
        return 'NULL'
    if isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"
    if isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
        return f"'{val}'"
    if isinstance(val, bytes):
        return f"E'\\\\x{val.hex()}'"
    return str(val)

def fetch_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    return [(row[0], row[1]) for row in cur.fetchall()]

def get_safe_select_clause(cursor, schema, table, logger):
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, schema, table)

    select_clauses = []
    output_columns = []
    for row in cursor.fetchall():
        col, dtype = row[0], row[1].lower()
        output_columns.append(col)

        if dtype == "geography":
            select_clauses.append(f"CAST({quote_ident(col)} AS VARCHAR(2048)) AS {quote_ident(col)}")
            casted_columns_map[f"{schema}.{table}"].append((col, dtype))
        elif dtype in {"hierarchyid", "sql_variant"}:
            select_clauses.append(f"CAST({quote_ident(col)} AS VARCHAR) AS {quote_ident(col)}")
            casted_columns_map[f"{schema}.{table}"].append((col, dtype))
        else:
            select_clauses.append(quote_ident(col))

    return select_clauses, output_columns

def build_foreign_key_usage(pg_conn):
    """
    Build a map of foreign key constraints referencing each (schema, table, column).
    Returns:
        Dict[(schema, table, column)] -> List[Dict with referencing table/column/constraint]
    """
    usage = defaultdict(list)
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT
                c.conname AS constraint_name,
                sch1.nspname AS src_schema,
                tbl1.relname AS src_table,
                col1.attname AS src_column,
                sch2.nspname AS tgt_schema,
                tbl2.relname AS tgt_table,
                col2.attname AS tgt_column
            FROM pg_constraint c
            JOIN pg_class tbl1 ON c.conrelid = tbl1.oid
            JOIN pg_namespace sch1 ON tbl1.relnamespace = sch1.oid
            JOIN unnest(c.conkey) WITH ORDINALITY AS k1(attnum, ord) ON true
            JOIN pg_attribute col1 ON col1.attnum = k1.attnum AND col1.attrelid = tbl1.oid
            JOIN pg_class tbl2 ON c.confrelid = tbl2.oid
            JOIN pg_namespace sch2 ON tbl2.relnamespace = sch2.oid
            JOIN unnest(c.confkey) WITH ORDINALITY AS k2(attnum, ord) ON k1.ord = k2.ord
            JOIN pg_attribute col2 ON col2.attnum = k2.attnum AND col2.attrelid = tbl2.oid
            WHERE c.contype = 'f'
        """)
        for row in cur.fetchall():
            constraint, src_schema, src_table, src_col, tgt_schema, tgt_table, tgt_col = row
            usage[(tgt_schema, tgt_table, tgt_col)].append({
                'schema': src_schema,
                'table': src_table,
                'column': src_col,
                'constraint': constraint
            })
    return usage


def export_table(conn, schema, table, out_dir, logger):
    cur = conn.cursor()
    try:
        select_clauses, output_cols = get_safe_select_clause(cur, schema, table, logger)
        col_list = ', '.join(quote_ident(c) for c in output_cols)
        query = f"SELECT {', '.join(select_clauses)} FROM {quote_ident(schema)}.{quote_ident(table)}"
        cur.execute(query)
    except Exception as e:
        logger.error("Failed to prepare export for %s.%s: %s", schema, table, e)
        return

    output_path = Path(out_dir) / f"{schema}.{table}.sql"
    row_count = 0

    with open(output_path, "w", encoding="utf-8") as f:
        for row in cur:
            try:
                values = ', '.join(quote_literal(v) for v in row)
                insert_stmt = (
                    f'INSERT INTO {quote_ident(schema)}.{quote_ident(table)} '
                    f'({col_list}) VALUES ({values});\n'
                )
                f.write(insert_stmt)
                row_count += 1
            except Exception as err:
                logger.warning("Row error in %s.%s: %s", schema, table, err)

    if row_count == 0:
        output_path.unlink()
        logger.warning("Table %s.%s had no rows. Skipping empty file.", schema, table)
    else:
        logger.info("Exported %d rows to %s", row_count, output_path)

def generate_postprocess_sql(out_dir, logger):
    output_file = Path(out_dir) / "_convert_types.sql"
    with open(output_file, "w", encoding="utf-8") as f:
        extensions = set()

        for full_table, columns in casted_columns_map.items():
            schema, table = full_table.split(".")
            for col, dtype in columns:
                fks = foreign_key_usage.get((schema, table, col), [])

                # Drop constraints first
                for fk in fks:
                    f.write(
                        f"ALTER TABLE {quote_ident(fk['schema'])}.{quote_ident(fk['table'])} "
                        f"DROP CONSTRAINT {quote_ident(fk['constraint'])};\n"
                    )

                # Convert the column in the target table
                if dtype == "hierarchyid":
                    if "ltree" not in extensions:
                        f.write("CREATE EXTENSION IF NOT EXISTS ltree;\n\n")
                        extensions.add("ltree")

                    f.write(
                        f"ALTER TABLE {quote_ident(schema)}.{quote_ident(table)} "
                        f"ALTER COLUMN {quote_ident(col)} TYPE ltree "
                        f"USING regexp_replace(trim(both '/' from {quote_ident(col)}::text), '/', '.', 'g')::ltree;\n\n"
                    )

                    # Convert referencing columns too
                    for fk in fks:
                        f.write(
                            f"ALTER TABLE {quote_ident(fk['schema'])}.{quote_ident(fk['table'])} "
                            f"ALTER COLUMN {quote_ident(fk['column'])} TYPE ltree "
                            f"USING regexp_replace(trim(both '/' from {quote_ident(fk['column'])}::text), '/', '.', 'g')::ltree;\n\n"
                        )

                elif dtype == "geography":
                    if "postgis" not in extensions:
                        f.write("CREATE EXTENSION IF NOT EXISTS postgis;\n\n")
                        extensions.add("postgis")

                    f.write(
                        f"ALTER TABLE {quote_ident(schema)}.{quote_ident(table)} "
                        f"ALTER COLUMN {quote_ident(col)} TYPE geometry "
                        f"USING CASE "
                        f"WHEN {quote_ident(col)} ~ '^\\s*(POINT|LINE|POLYGON|MULTI)' "
                        f"THEN ST_GeomFromText({quote_ident(col)}) "
                        f"ELSE ST_GeomFromText('POINT(0 0)') END;\n\n"
                    )

                    for fk in fks:
                        f.write(
                            f"ALTER TABLE {quote_ident(fk['schema'])}.{quote_ident(fk['table'])} "
                            f"ALTER COLUMN {quote_ident(fk['column'])} TYPE geometry "
                            f"USING CASE "
                            f"WHEN {quote_ident(fk['column'])} ~ '^\\s*(POINT|LINE|POLYGON|MULTI)' "
                            f"THEN ST_GeomFromText({quote_ident(fk['column'])}) "
                            f"ELSE ST_GeomFromText('POINT(0 0)') END;\n\n"
                        )

                elif dtype == "sql_variant":
                    f.write(f"-- Manual review needed for {full_table}.{col} (sql_variant)\n\n")

                # Re-add constraints
                for fk in fks:
                    f.write(
                        f"ALTER TABLE {quote_ident(fk['schema'])}.{quote_ident(fk['table'])} "
                        f"ADD CONSTRAINT {quote_ident(fk['constraint'])} "
                        f"FOREIGN KEY ({quote_ident(fk['column'])}) "
                        f"REFERENCES {quote_ident(schema)}.{quote_ident(table)} ({quote_ident(col)});\n"
                    )

    logger.info("Generated type conversion SQL: %s", output_file)

def main():
    parser = argparse.ArgumentParser(description="Export SQL Server data as INSERT SQL for PostgreSQL")
    parser.add_argument("--server", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--pg-host", required=True)
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", required=True)
    parser.add_argument("--pg-username", required=True)
    parser.add_argument("--pg-password", required=True)
    parser.add_argument("--outdir", default="data_sql")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logger = setup_logging(args.log_level)
    os.makedirs(args.outdir, exist_ok=True)

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={args.server};DATABASE={args.database};"
        f"UID={args.username};PWD={args.password}"
    )

    try:
        conn = pyodbc.connect(conn_str)
    except Exception as e:
        logger.error("Connection failed: %s", e)
        return

    # Global foreign key usage
    global foreign_key_usage
    pg_conn = psycopg2.connect(
    host=args.pg_host,
    port=args.pg_port,
    dbname=args.pg_database,
    user=args.pg_username,
    password=args.pg_password
)
    logger.info("Connecting to PostgreSQL for foreign key introspection...")
    foreign_key_usage = build_foreign_key_usage(pg_conn)

    
    logger.info("Fetching table list …")
    tables = fetch_tables(conn)
    logger.info("Found %d tables.", len(tables))

    for schema, table in tables:
        try:
            export_table(conn, schema, table, args.outdir, logger)
        except Exception as e:
            logger.exception("Error exporting %s.%s: %s", schema, table, e)

    conn.close()
    generate_postprocess_sql(args.outdir, logger)
    logger.info("All export scripts and postprocessing generated in: %s", args.outdir)

if __name__ == "__main__":
    main()
