import pyodbc
import argparse
import logging
import uuid

def setup_logging(log_level):
    """Configure logging with the specified level."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_all_tables(connection_string):
    """Retrieve all table names with their schemas from the MS SQL Server database."""
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        tables = cursor.fetchall()
        connection.close()
        return tables
    except pyodbc.Error as e:
        logger.error(f"Error fetching table list: {e}")
        return []

def get_sqlserver_schema(connection_string, schema_name, table_name):
    """Retrieve column, primary key, foreign key, index, and default value information for a specific table."""
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        # Fetch column details including identity and default values
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                CHARACTER_MAXIMUM_LENGTH, 
                NUMERIC_PRECISION, 
                NUMERIC_SCALE, 
                IS_NULLABLE,
                COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS is_identity,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, schema_name, table_name)
        columns = cursor.fetchall()
        
        if not columns:
            logger.warning(f"Table '{schema_name}.{table_name}' has no columns or does not exist.")
            return None, None, None, None, None
        
        # Fetch primary key columns
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME LIKE 'PK_%'
        """, schema_name, table_name)
        primary_keys = [row[0] for row in cursor.fetchall()]
        
        # Fetch foreign key details
        cursor.execute(f"""
            SELECT 
                KCU1.CONSTRAINT_NAME,
                KCU1.COLUMN_NAME,
                KCU2.TABLE_SCHEMA AS REFERENCED_TABLE_SCHEMA,
                KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME,
                KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
                ON RC.CONSTRAINT_NAME = KCU1.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
                ON RC.UNIQUE_CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME
            WHERE KCU1.TABLE_SCHEMA = ? AND KCU1.TABLE_NAME = ?
        """, schema_name, table_name)
        foreign_keys = cursor.fetchall()
        
        # Fetch index details (excluding primary key indexes)
        cursor.execute(f"""
            SELECT 
                i.name AS index_name,
                ic.key_ordinal,
                c.name AS column_name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ? AND i.is_primary_key = 0
            ORDER BY i.name, ic.key_ordinal
        """, schema_name, table_name)
        indexes = {}
        for row in cursor.fetchall():
            index_name = row[0]
            column_name = row[2]
            if index_name not in indexes:
                indexes[index_name] = []
            indexes[index_name].append(column_name)
        
        connection.close()
        return columns, primary_keys, foreign_keys, indexes
    
    except pyodbc.Error as e:
        logger.error(f"Database error for table '{schema_name}.{table_name}': {e}")
        return None, None, None, None, None

def map_data_type(sqlserver_type, char_max_length, numeric_precision, numeric_scale):
    """Map MS SQL Server data types to PostgreSQL equivalents."""
    type_mapping = {
        'int': 'integer',
        'bigint': 'bigint',
        'smallint': 'smallint',
        'tinyint': 'smallint',
        'bit': 'boolean',
        'decimal': 'numeric',
        'numeric': 'numeric',
        'money': 'numeric(19,4)',
        'smallmoney': 'numeric(10,4)',
        'float': 'double precision',
        'real': 'real',
        'date': 'date',
        'datetime': 'timestamp',
        'datetime2': 'timestamp',
        'smalldatetime': 'timestamp',
        'time': 'time',
        'char': 'character',
        'varchar': 'character varying',
        'text': 'text',
        'nchar': 'character',
        'nvarchar': 'character varying',
        'ntext': 'text',
        'binary': 'bytea',
        'varbinary': 'bytea',
        'image': 'bytea',
        'uniqueidentifier': 'uuid',
        'xml': 'xml'
    }
    
    pg_type = type_mapping.get(sqlserver_type.lower(), f'unknown_type /* TODO: map {sqlserver_type} */')
    
    # Handle max length types (e.g., varchar(max), nvarchar(max))
    if sqlserver_type in ['varchar', 'nvarchar', 'char', 'nchar'] and char_max_length == -1:
        pg_type = 'text'  # Use text for max-length types
    elif sqlserver_type in ['varchar', 'nvarchar', 'char', 'nchar'] and char_max_length and char_max_length > 0:
        pg_type += f'({char_max_length})'
    elif sqlserver_type in ['decimal', 'numeric'] and numeric_precision and numeric_scale:
        pg_type += f'({numeric_precision}, {numeric_scale})'
    
    return pg_type

def generate_postgres_schema(columns, primary_keys, foreign_keys, indexes, schema_name, table_name):
    """Generate PostgreSQL CREATE TABLE, ALTER TABLE for foreign keys, and CREATE INDEX statements."""
    # Create table statement
    column_definitions = []
    for col in columns:
        col_name = f'"{col[0]}"'
        pg_type = map_data_type(col[1], col[2], col[3], col[4])
        
        if col[6]:
            pg_type += ' GENERATED BY DEFAULT AS IDENTITY'
        
        nullable = 'NOT NULL' if col[5] == 'NO' else ''
        default_value = f"DEFAULT {col[7]}" if col[7] else ''
        column_definitions.append(f'{col_name} {pg_type} {nullable} {default_value}'.strip())
    
    if primary_keys:
        pk_columns = ', '.join([f'"{pk}"' for pk in primary_keys])
        pk_constraint = f'PRIMARY KEY ({pk_columns})'
        column_definitions.append(pk_constraint)
    
    create_table_stmt = f'CREATE TABLE "{schema_name}"."{table_name}" (\n    ' + ',\n    '.join(column_definitions) + '\n);\n\n'
    
    # Foreign key statements
    fk_statements = []
    for fk in foreign_keys:
        constraint_name = fk[0]
        column_name = fk[1]
        ref_schema = fk[2]
        ref_table = fk[3]
        ref_column = fk[4]
        fk_stmt = f'ALTER TABLE "{schema_name}"."{table_name}" ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ("{column_name}") REFERENCES "{ref_schema}"."{ref_table}" ("{ref_column}");\n'
        fk_statements.append(fk_stmt)
    
    # Index statements
    index_statements = []
    for index_name, columns in indexes.items():
        column_list = ', '.join([f'"{col}"' for col in columns])
        index_stmt = f'CREATE INDEX "{index_name}" ON "{schema_name}"."{table_name}" ({column_list});\n'
        index_statements.append(index_stmt)
    
    return create_table_stmt, fk_statements, index_statements

def main():
    """Main function to migrate schemas for all tables in all schemas."""
    global logger
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Migrate all MS SQL Server table schemas to PostgreSQL, including multiple schemas.')
    parser.add_argument('--server', required=True, help='SQL Server name (e.g., localhost,1433 for Docker)')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--username', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--output', default='schemas.sql', help='Output file path (default: schemas.sql)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Set logging level')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level)
    
    # Construct connection string for Dockerized SQL Server
    connection_string = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={args.server};'
        f'DATABASE={args.database};'
        f'UID={args.username};'
        f'PWD={args.password}'
    )
    
    # Get all tables with schemas
    logger.info("Fetching list of all tables across all schemas.")
    tables = get_all_tables(connection_string)
    if not tables:
        logger.error("No tables found or error occurred. Exiting.")
        return
    
    logger.info(f"Found {len(tables)} tables.")
    
    # Generate schemas for all tables
    with open(args.output, 'w') as f:
        for schema_name, table_name in tables:
            logger.debug(f"Processing table: {schema_name}.{table_name}")
            columns, primary_keys, foreign_keys, indexes = get_sqlserver_schema(connection_string, schema_name, table_name)
            if columns:
                create_table_stmt, fk_statements, index_statements = generate_postgres_schema(columns, primary_keys, foreign_keys, indexes, schema_name, table_name)
                f.write(create_table_stmt)
                for fk_stmt in fk_statements:
                    f.write(fk_stmt)
                for index_stmt in index_statements:
                    f.write(index_stmt)
            else:
                logger.warning(f"Skipping table '{schema_name}.{table_name}' due to schema retrieval failure.")
    
    logger.info(f"Schemas for all tables written to '{args.output}'.")

if __name__ == '__main__':
    main()