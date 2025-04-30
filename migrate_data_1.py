import argparse
import logging
import os
import psycopg2
from pathlib import Path

def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)

def run_sql_file(conn, filepath, logger):
    logger.info("📄 Running %s ...", filepath.name)
    with open(filepath, 'r', encoding='utf-8') as f:
        sql = f.read()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("✅ Success: %s", filepath.name)
    except Exception as e:
        conn.rollback()
        logger.error("❌ Error in %s: %s", filepath.name, e)

def set_replication_role(conn, role, logger):
    with conn.cursor() as cur:
        cur.execute(f"SET session_replication_role = {role};")
        cur.execute("SHOW session_replication_role;")
        current = cur.fetchone()[0]
        logger.info("PostgreSQL session_replication_role is now: %s", current)

def main():
    parser = argparse.ArgumentParser(description="Run all .sql files against PostgreSQL in order")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="5432")
    parser.add_argument("--database", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--sql-dir", default="data_sql")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logger = setup_logging(args.log_level)
    sql_dir = Path(args.sql_dir)

    if not sql_dir.exists():
        logger.error("SQL directory not found: %s", sql_dir)
        return

    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.database,
            user=args.username,
            password=args.password
        )
    except Exception as e:
        logger.error("❌ PostgreSQL connection failed: %s", e)
        return

    logger.info("Connected to PostgreSQL at %s:%s", args.host, args.port)

    # 🚫 Disable FK/trigger checks
    set_replication_role(conn, "replica", logger)

    # 🧾 Run data SQL files (except _convert_types.sql)
    sql_files = sorted(p for p in sql_dir.glob("*.sql") if p.name != "_convert_types.sql")
    for file in sql_files:
        run_sql_file(conn, file, logger)

    # 🔁 Run postprocessing (type conversion) last
    convert_file = sql_dir / "_convert_types.sql"
    if convert_file.exists():
        run_sql_file(conn, convert_file, logger)

    # ✅ Re-enable constraints
    set_replication_role(conn, "default", logger)

    conn.close()
    logger.info("🎉 All done. SQL execution complete.")

if __name__ == "__main__":
    main()
