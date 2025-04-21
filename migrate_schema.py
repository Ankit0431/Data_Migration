import pyodbc
import argparse
import logging
import re
from collections import defaultdict, deque

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
        
        # Fetch column details including identity, default values, and data type
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
            return None, None, None, None
        
        # Create a dictionary of column names to data types for index validation
        column_data_types = {col[0]: col[1].lower() for col in columns}
        
        # Fetch primary key columns
        cursor.execute(f"""
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = ? 
                AND tc.TABLE_NAME = ? 
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kcu.ORDINAL_POSITION
        """, schema_name, table_name)
        primary_keys = [row[0] for row in cursor.fetchall()]
        logger.debug(f"Primary keys for {schema_name}.{table_name}: {primary_keys}")
        
        # Fetch foreign key details with multi-column support
        cursor.execute(f"""
            SELECT 
                RC.CONSTRAINT_NAME,
                KCU1.COLUMN_NAME AS FK_COLUMN_NAME,
                KCU2.TABLE_SCHEMA AS REFERENCED_TABLE_SCHEMA,
                KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME,
                KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME,
                KCU1.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
                ON RC.CONSTRAINT_NAME = KCU1.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
                ON RC.UNIQUE_CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME
                AND KCU1.ORDINAL_POSITION = KCU2.ORDINAL_POSITION
            WHERE KCU1.TABLE_SCHEMA = ? AND KCU1.TABLE_NAME = ?
            ORDER BY RC.CONSTRAINT_NAME, KCU1.ORDINAL_POSITION
        """, schema_name, table_name)
        fk_rows = cursor.fetchall()
        
        # Group foreign keys by constraint name
        foreign_keys = defaultdict(list)
        for row in fk_rows:
            constraint_name = row[0]
            foreign_keys[constraint_name].append({
                'fk_column': row[1],
                'ref_schema': row[2],
                'ref_table': row[3],
                'ref_column': row[4]
            })
        foreign_keys_list = []
        for constraint_name, fk_info in foreign_keys.items():
            for fk in fk_info:
                foreign_keys_list.append((
                    constraint_name,
                    fk['fk_column'],
                    fk['ref_schema'],
                    fk['ref_table'],
                    fk['ref_column']
                ))
        logger.debug(f"Foreign keys for {schema_name}.{table_name}: {foreign_keys_list}")
        
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
            # Skip indexes on xml columns
            if column_data_types.get(column_name) == 'xml':
                logger.warning(f"Skipping index '{index_name}' on column '{column_name}' in {schema_name}.{table_name} because xml does not support B-tree indexes.")
                continue
            if index_name not in indexes:
                indexes[index_name] = []
            indexes[index_name].append(column_name)
        logger.debug(f"Indexes for {schema_name}.{table_name}: {indexes}")
        
        connection.close()
        return columns, primary_keys, foreign_keys_list, indexes
    
    except pyodbc.Error as e:
        logger.error(f"Database error for table '{schema_name}.{table_name}': {e}")
        return None, None, None, None

def topological_sort_tables(tables, foreign_keys_map):
    """Sort tables based on foreign key dependencies using topological sort."""
    graph = defaultdict(list)
    in_degree = {f"{schema}.{table}": 0 for schema, table in tables}
    
    for schema, table in tables:
        table_key = f"{schema}.{table}"
        if table_key in foreign_keys_map:
            for fk in foreign_keys_map[table_key]:
                ref_table_key = f"{fk[2]}.{fk[3]}"
                if ref_table_key in in_degree:
                    graph[ref_table_key].append(table_key)
                    in_degree[table_key] += 1
    
    queue = deque([key for key, degree in in_degree.items() if degree == 0])
    sorted_tables = []
    
    while queue:
        current = queue.popleft()
        sorted_tables.append(current)
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    if len(sorted_tables) != len(tables):
        logger.warning("Cycle detected in foreign key dependencies; some tables may not be ordered correctly.")
        remaining = [key for key in in_degree if key not in sorted_tables]
        sorted_tables.extend(remaining)
    
    return [(t.split('.')[0], t.split('.')[1]) for t in sorted_tables]

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
        'xml': 'xml',
        'geography': 'geometry /* requires PostGIS extension */',
        'hierarchyid': 'ltree /* requires ltree extension */'
    }
    
    sqlserver_type = sqlserver_type.lower()
    pg_type = type_mapping.get(sqlserver_type)
    if pg_type is None:
        raise ValueError(f"Unmapped SQL Server data type '{sqlserver_type}' detected. Please add a mapping.")
    
    if sqlserver_type in ['varchar', 'nvarchar', 'char', 'nchar']:
        if char_max_length == -1:
            pg_type = 'text'
        elif char_max_length and char_max_length > 0:
            pg_type += f'({char_max_length})'
    elif sqlserver_type in ['decimal', 'numeric'] and numeric_precision and numeric_scale:
        pg_type += f'({numeric_precision}, {numeric_scale})'
    
    return pg_type

def map_default_value(default_value, sqlserver_type):
    """Map SQL Server default values to PostgreSQL equivalents, considering column type."""
    if not default_value:
        return ''
    
    default_value = default_value.strip()
    while default_value.startswith('(') and default_value.endswith(')'):
        default_value = default_value[1:-1].strip()
    
    default_mapping = {
        r'\bNEWID\(\s*\)': 'gen_random_uuid()',
        r'\bNEWSEQUENTIALID\(\s*\)': 'gen_random_uuid()',
        r'\bGETDATE\(\s*\)': 'CURRENT_TIMESTAMP',
        r'\bSYSDATETIME\(\s*\)': 'CURRENT_TIMESTAMP',
        r'\bSYSUTCDATETIME\(\s*\)': '(CURRENT_TIMESTAMP AT TIME ZONE \'UTC\')',
        r'\bGETUTCDATE\(\s*\)': '(CURRENT_TIMESTAMP AT TIME ZONE \'UTC\')',
        r'\bCURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
        r'\bCURRENT_USER': 'CURRENT_USER',
        r'\bSESSION_USER': 'SESSION_USER',
        r'\bSYSTEM_USER': 'CURRENT_USER',
        r'\bUSER': 'CURRENT_USER',
        r'\bSUSER_SNAME\(\s*\)': 'CURRENT_USER',
        r'\bHOST_NAME\(\s*\)': 'inet_client_addr()',
        r'\bNULL': 'NULL',
        r'\b\'[^\']*\'': lambda m: m.group(0),
        r'\b-?\d+(\.\d+)?\b': lambda m: m.group(0),
        r'\b\'\'': '\'\'' 
    }
    
    if sqlserver_type.lower() == 'bit':
        if re.fullmatch(r'\b0\b', default_value):
            return 'DEFAULT FALSE'
        if re.fullmatch(r'\b1\b', default_value):
            return 'DEFAULT TRUE'
    
    for pattern, replacement in default_mapping.items():
        if re.fullmatch(pattern, default_value, re.IGNORECASE):
            if callable(replacement):
                return f'DEFAULT {replacement(re.fullmatch(pattern, default_value, re.IGNORECASE))}'
            else:
                return f'DEFAULT {replacement}'
    
    raise ValueError(f"Unmapped default value '{default_value}' for type '{sqlserver_type}' detected. Please add a mapping.")

def generate_postgres_schema(columns, primary_keys, foreign_keys, indexes, schema_name, table_name):
    """Generate PostgreSQL CREATE TABLE, ALTER TABLE for foreign keys, and CREATE INDEX statements."""
    column_definitions = []
    for col in columns:
        col_name = f'"{col[0]}"'
        pg_type = map_data_type(col[1], col[2], col[3], col[4])
        
        if col[6]:
            pg_type += ' GENERATED BY DEFAULT AS IDENTITY'
        
        nullable = 'NOT NULL' if col[5] == 'NO' else ''
        default_value = map_default_value(col[7], col[1])
        column_definitions.append(f'{col_name} {pg_type} {nullable} {default_value}'.strip())
    
    if primary_keys:
        pk_columns = ', '.join([f'"{pk}"' for pk in primary_keys])
        pk_constraint = f'PRIMARY KEY ({pk_columns})'
        column_definitions.append(pk_constraint)
    
    create_table_stmt = f'CREATE TABLE "{schema_name}"."{table_name}" (\n    ' + ',\n    '.join(column_definitions) + '\n);\n\n'
    
    fk_statements = []
    fk_by_constraint = defaultdict(list)
    for fk in foreign_keys:
        constraint_name = fk[0]
        fk_by_constraint[constraint_name].append(fk)
    
    for constraint_name, fk_list in fk_by_constraint.items():
        fk_columns = ', '.join([f'"{fk[1]}"' for fk in fk_list])
        ref_schema = fk_list[0][2]
        ref_table = fk_list[0][3]
        ref_columns = ', '.join([f'"{fk[4]}"' for fk in fk_list])
        fk_stmt = (
            f'ALTER TABLE "{schema_name}"."{table_name}" '
            f'ADD CONSTRAINT "{constraint_name}" '
            f'FOREIGN KEY ({fk_columns}) '
            f'REFERENCES "{ref_schema}"."{ref_table}" ({ref_columns});\n'
        )
        fk_statements.append(fk_stmt)
    
    index_statements = []
    for index_name, columns in indexes.items():
        column_list = ', '.join([f'"{col}"' for col in columns])
        index_stmt = f'CREATE INDEX "{index_name}" ON "{schema_name}"."{table_name}" ({column_list});\n'
        index_statements.append(index_stmt)
    
    return create_table_stmt, fk_statements, index_statements

def main():
    """Main function to migrate schemas for all tables in all schemas."""
    global logger
    
    parser = argparse.ArgumentParser(description='Migrate all MS SQL Server table schemas to PostgreSQL, including multiple schemas.')
    parser.add_argument('--server', required=True, help='SQL Server name (e.g., localhost,1433 for Docker)')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--username', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--output', default='schemas.sql', help='Output file path (default: schemas.sql)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Set logging level')
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    connection_string = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={args.server};'
        f'DATABASE={args.database};'
        f'UID={args.username};'
        f'PWD={args.password}'
    )
    
    logger.info("Fetching list of all tables across all schemas.")
    tables = get_all_tables(connection_string)
    if not tables:
        logger.error("No tables found or error occurred. Exiting.")
        return
    
    logger.info(f"Found {len(tables)} tables.")
    
    foreign_keys_map = {}
    for schema_name, table_name in tables:
        table_key = f"{schema_name}.{table_name}"
        columns, primary_keys, foreign_keys, indexes = get_sqlserver_schema(connection_string, schema_name, table_name)
        if columns:
            foreign_keys_map[table_key] = foreign_keys
    
    sorted_tables = topological_sort_tables(tables, foreign_keys_map)
    
    create_table_statements = []
    fk_statements = []
    index_statements = []
    
    extension_comments = (
        "-- Enable PostGIS extension for geometry type\n"
        "CREATE EXTENSION IF NOT EXISTS postgis;\n\n"
        "-- Enable ltree extension for hierarchical data\n"
        "CREATE EXTENSION IF NOT EXISTS ltree;\n\n"
    )
    
    for schema_name, table_name in sorted_tables:
        logger.debug(f"Processing table: {schema_name}.{table_name}")
        columns, primary_keys, foreign_keys, indexes = get_sqlserver_schema(connection_string, schema_name, table_name)
        if columns:
            create_table_stmt, table_fk_statements, table_index_statements = generate_postgres_schema(columns, primary_keys, foreign_keys, indexes, schema_name, table_name)
            create_table_statements.append(create_table_stmt)
            fk_statements.extend(table_fk_statements)
            index_statements.extend(table_index_statements)
        else:
            logger.warning(f"Skipping table '{schema_name}.{table_name}' due to schema retrieval failure.")
    
    try:
        with open(args.output, 'w') as f:
            f.write(extension_comments)
            for stmt in create_table_statements:
                f.write(stmt)
            for stmt in fk_statements:
                f.write(stmt)
            for stmt in index_statements:
                f.write(stmt)
        logger.info(f"Schemas for all tables written to '{args.output}'.")
    except IOError as e:
        logger.error(f"Error writing to file: {e}")

if __name__ == '__main__':
    main()