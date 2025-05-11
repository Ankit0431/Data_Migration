import subprocess
import logging
import argparse
import sys
from typing import List

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("data_migration")

def run_script(script: str, args: List[str], logger: logging.Logger):
    cmd = [sys.executable, script] + args
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        logger.error("Script failed: %s", script)
        sys.exit(result.returncode)

def main():
    logger = setup_logging()

    parser = argparse.ArgumentParser(description="End-to-end data migration pipeline from SQL Server to PostgreSQL")
    parser.add_argument('--sqlserver', required=True, help='SQL Server host (e.g., localhost,1433)')
    parser.add_argument('--sqlserver-db', required=True, help='SQL Server database name')
    parser.add_argument('--sqlserver-user', required=True, help='SQL Server username')
    parser.add_argument('--sqlserver-pass', required=True, help='SQL Server password')
    parser.add_argument('--pg-host', required=True, help='PostgreSQL host')
    parser.add_argument('--pg-port', default='5432', help='PostgreSQL port')
    parser.add_argument('--pg-db', required=True, help='PostgreSQL database name')
    parser.add_argument('--pg-user', required=True, help='PostgreSQL username')
    parser.add_argument('--pg-pass', required=True, help='PostgreSQL password')
    parser.add_argument('--sql-dir', default='data_sql', help='Directory to store generated SQL insert scripts')
    args = parser.parse_args()

    # --- Step 1: Extract schema from SQL Server
    run_script("migrate_schema.py", [
        "--server", args.sqlserver,
        "--database", args.sqlserver_db,
        "--username", args.sqlserver_user,
        "--password", args.sqlserver_pass,
        "--output", "schemas.sql"
    ], logger)

    # --- Step 2: Create target PostgreSQL DB and apply schema
    run_script("postgres_schema.py", [
        "--host", args.pg_host,
        "--port", args.pg_port,
        "--database", args.pg_db,
        "--username", args.pg_user,
        "--password", args.pg_pass,
        "--sql-file", "schemas.sql"
    ], logger)

    # --- Step 3: Export data from SQL Server
    run_script("migrate_script.py", [
        "--server", args.sqlserver,
        "--database", args.sqlserver_db,
        "--username", args.sqlserver_user,
        "--password", args.sqlserver_pass,
        "--pg-host", args.pg_host,
        "--pg-port", args.pg_port,
        "--pg-database", args.pg_db,
        "--pg-username", args.pg_user,
        "--pg-password", args.pg_pass,
        "--outdir", args.sql_dir
    ], logger)

    # --- Step 4: Import data into PostgreSQL
    run_script("migrate_data_1.py", [
        "--host", args.pg_host,
        "--port", args.pg_port,
        "--database", args.pg_db,
        "--username", args.pg_user,
        "--password", args.pg_pass,
        "--sql-dir", args.sql_dir
    ], logger)

    # --- Step 5: Validate migration
    run_script("verify_migration.py", [
        "--server", args.sqlserver,
        "--database", args.sqlserver_db,
        "--username", args.sqlserver_user,
        "--password", args.sqlserver_pass,
        "--pg-host", args.pg_host,
        "--pg-port", args.pg_port,
        "--pg-database", args.pg_db,
        "--pg-username", args.pg_user,
        "--pg-password", args.pg_pass
    ], logger)

    logger.info("Data migration and verification completed successfully.")

if __name__ == "__main__":
    main()
