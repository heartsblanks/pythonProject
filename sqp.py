import re
import sqlglot

# Function to extract potential SQL statements from ESQL content
def extract_sql_statements(esql_content):
    # Match statements that start with SQL keywords and end with a semicolon
    sql_pattern = r"\b(SELECT|INSERT|UPDATE|DELETE)\b[\s\S]+?;"
    statements = re.findall(sql_pattern, esql_content, re.IGNORECASE | re.DOTALL)
    return statements

# Function to parse SQL statements and handle custom table expressions with {}
def parse_sql_statements(sql_statements):
    for statement in sql_statements:
        # Check for custom table expressions with {}
        if re.search(r"\{.*?\}", statement):
            print(f"Custom Statement Detected: {statement}")
            
            # Extract operation type
            operation_type = statement.split()[0].upper()
            
            # Extract table name by finding the word after INTO, FROM, or UPDATE
            table_name_pattern = {
                'SELECT': r"FROM\s+([\w.{}() ||',]+)",
                'INSERT': r"INTO\s+([\w.{}() ||',]+)",
                'UPDATE': r"UPDATE\s+([\w.{}() ||',]+)",
                'DELETE': r"FROM\s+([\w.{}() ||',]+)"
            }
            table_match = re.search(table_name_pattern[operation_type], statement, re.IGNORECASE)
            table_name = table_match.group(1) if table_match else None

            print(f"Operation Type: {operation_type}")
            print(f"Table Name: {table_name}\n")
        
        else:
            # For standard statements, use sqlglot
            try:
                parsed = sqlglot.parse_one(statement)
                
                # Identify operation type and extract table name using sqlglot
                operation_type = parsed.find(sqlglot.exp.Select) or \
                                 parsed.find(sqlglot.exp.Insert) or \
                                 parsed.find(sqlglot.exp.Update) or \
                                 parsed.find(sqlglot.exp.Delete)

                if operation_type:
                    operation_name = operation_type.key

                # Extract table name based on operation
                table_name = None
                if operation_name == 'select' or operation_name == 'delete':
                    from_clause = parsed.find(sqlglot.exp.From)
                    if from_clause and from_clause.this:
                        table_name = from_clause.this.sql()
                
                elif operation_name == 'insert':
                    into_clause = parsed.find(sqlglot.exp.Into)
                    if into_clause and into_clause.this:
                        table_name = into_clause.this.sql()
                
                elif operation_name == 'update':
                    table_clause = parsed.find(sqlglot.exp.Table)
                    if table_clause:
                        table_name = table_clause.sql()

                # Output the operation type and table name
                print(f"Operation Type: {operation_name.upper()}")
                print(f"Table Name: {table_name}\n")

            except Exception as e:
                print(f"Error parsing statement with sqlglot: {e}")
                print(f"Statement: {statement}\n")

# Example usage
esql_content = """
SELECT column1, column2 FROM Database.{getSchema(), 'HT'}.tablename WHERE condition = 'value';
INSERT INTO Database.{getSchema() || 'HT'}.tablename (column1, column2) VALUES ('value1', 'value2');
UPDATE Database.{getSchema(), 'HT'}.tablename SET column1 = 'new_value' WHERE condition = 'value';
DELETE FROM Database.{getSchema(), 'HT'}.tablename WHERE condition = 'value';
"""
sql_statements = extract_sql_statements(esql_content)
parse_sql_statements(sql_statements)