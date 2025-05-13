def build_column_context_prompt(table_name, column_name, sqlserver_type, char_max_length, numeric_precision, numeric_scale, default_value=None, sample_values=None, custom_description=None):
    prompt = f"""
You are converting a SQL Server column definition to a PostgreSQL-compatible type.

Table: {table_name}
Column: {column_name}
SQL Server Type: {sqlserver_type}
Length: {char_max_length}
Precision: {numeric_precision}
Scale: {numeric_scale}
"""

    if default_value:
        prompt += f"Default Value: {default_value}\n"

    if sample_values:
        sample_str = ", ".join(str(v) for v in sample_values[:5])
        prompt += f"Sample Values: {sample_str}\n"

    if custom_description:
        prompt += f"Description: {custom_description}\n"

    prompt += """

If the data type needs a PostgreSQL extension like PostGIS or ltree, mention the mapped datatype as varchar, and add the required extension in your output. For example:
    input: geography
    output:"varchar /* requires PostGIS extension */"
    input: hierarchyid
    output: "varchar /* requires ltree extension */"
    
Return only the PostgreSQL datatype and the required PostgreSQL extension in comment in a single line.
"""
    return prompt.strip()

def build_default_value_prompt(table_name, column_name, sqlserver_type, default_value, custom_description=None):
    prompt = f"""
You are converting a SQL Server default value to a PostgreSQL-compatible DEFAULT clause.

Table: {table_name}
Column: {column_name}
SQL Server Type: {sqlserver_type}
Default Expression: {default_value}
"""

    if custom_description:
        prompt += f"Description: {custom_description}\n"

    prompt += """
Return only the PostgreSQL DEFAULT expression on a single line.
If this value requires a PostgreSQL extension (e.g., PostGIS for geometry), include a comment like:
    ST_GeomFromText('POINT(0 0)', 4326) /* requires PostGIS */
"""
    return prompt.strip()
