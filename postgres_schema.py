import psycopg2
import argparse
import logging
import os
import re

def setup_logging(log_level):
    """Configure logging with the specified level."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def extract_schemas(sql_content):
    """Extract unique schema names from SQL statements."""
    schema_pattern = re.compile(r'"([^"]+)"\."[^"]+"')
    schemas = set()
    for match in schema_pattern.finditer(sql_content):
        schemas.add(match.group(1))
    return schemas

def preprocess_sql_content(sql_content):
    """Replace SQL Server-specific syntax with PostgreSQL equivalents."""
    # Replace newid() with gen_random_uuid()
    sql_content = re.sub(r'\bnewid\(\)', 'gen_random_uuid()', sql_content, flags=re.IGNORECASE)
    return sql_content

def create_schemas(connection_string, schemas):
    """Create schemas in the PostgreSQL database if they don't exist."""
    try:
        connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        cursor = connection.cursor()
        
        for schema in schemas:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS \"{schema}\"")
            logger.debug(f"Ensured schema '{schema}' exists.")
        
        cursor.close()
        connection.close()
        return True
    except psycopg2.Error as e:
        logger.error(f"Error creating schemas: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while creating schemas: {e}")
        return False

def read_sql_file(file_path):
    """Read SQL statements from a file."""
    if not os.path.exists(file_path):
        logger.error(f"SQL file '{file_path}' does not exist.")
        return None
    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
        return sql_content
    except IOError as e:
        logger.error(f"Error reading SQL file '{file_path}': {e}")
        return None

def execute_sql(connection_string, sql_content):
    """Execute SQL statements in the PostgreSQL database."""
    try:
        connection = psycopg2.connect(connection_string)
        connection.autocommit = False
        cursor = connection.cursor()
        
        cursor.execute(sql_content)
        connection.commit()
        logger.info("SQL statements executed successfully.")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        connection.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    return True

def main():
    """Main function to apply schema to PostgreSQL database."""
    global logger
    
    parser = argparse.ArgumentParser(description='Apply SQL schema to a PostgreSQL database.')
    parser.add_argument('--host', required=True, help='PostgreSQL host (e.g., localhost)')
    parser.add_argument('--port', default='5432', help='PostgreSQL port (default: 5432)')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--username', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--sql-file', default='schemas.sql', help='Path to SQL schema file (default: schemas.sql)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Set logging level')
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    connection_string = (
        f"host={args.host} "
        f"port={args.port} "
        f"dbname={args.database} "
        f"user={args.username} "
        f"password={args.password}"
    )
    
    logger.info(f"Reading SQL file: {args.sql_file}")
    sql_content = read_sql_file(args.sql_file)
    if sql_content is None:
        logger.error("Failed to read SQL file. Exiting.")
        return
    
    logger.info("Preprocessing SQL content for PostgreSQL compatibility.")
    sql_content = preprocess_sql_content(sql_content)
    
    logger.info("Extracting schema names from SQL file.")
    schemas = extract_schemas(sql_content)
    if not schemas:
        logger.warning("No schemas found in SQL file. Proceeding with table creation.")
    else:
        logger.info(f"Found schemas: {', '.join(schemas)}")
        logger.info("Creating schemas in PostgreSQL database.")
        if not create_schemas(connection_string, schemas):
            logger.error("Failed to create schemas. Exiting.")
            return
    
    logger.info("Applying schema to PostgreSQL database.")
    if execute_sql(connection_string, sql_content):
        logger.info("Schema migration completed successfully.")
    else:
        logger.error("Schema migration failed.")

if __name__ == '__main__':
    main()