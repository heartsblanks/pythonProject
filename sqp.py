import sqlglot

# Function to parse SQL statements and extract operation type and table name
def parse_sql_statements(sql_statements):
    for statement in sql_statements:
        # Parse the SQL statement to get an AST
        parsed = sqlglot.parse_one(statement)

        # Identify the operation type
        operation_type = parsed.find(sqlglot.exp.Select) or \
                         parsed.find(sqlglot.exp.Insert) or \
                         parsed.find(sqlglot.exp.Update) or \
                         parsed.find(sqlglot.exp.Delete)

        # Get the name of the operation
        if operation_type:
            operation_name = operation_type.key

        # Extract table name based on operation
        table_name = None
        if operation_name == 'select' or operation_name == 'delete':
            # Get table name from FROM clause
            from_clause = parsed.find(sqlglot.exp.From)
            if from_clause and from_clause.this:
                table_name = from_clause.this.sql()
        
        elif operation_name == 'insert':
            # Get table name from INTO clause
            into_clause = parsed.find(sqlglot.exp.Into)
            if into_clause and into_clause.this:
                table_name = into_clause.this.sql()
        
        elif operation_name == 'update':
            # Get table name after UPDATE
            table_clause = parsed.find(sqlglot.exp.Table)
            if table_clause:
                table_name = table_clause.sql()

        # Output the operation type and table name
        print(f"Operation Type: {operation_name.upper()}")
        if table_name:
            print(f"Table Name: {table_name}\n")

# Example usage with extracted SQL statements
sql_statements = [
    "SELECT column1, column2 FROM Database.{getSchema(), 'HT'}.tablename WHERE condition = 'value';",
    "INSERT INTO Database.{getSchema() || 'HT'}.tablename (column1, column2) VALUES ('value1', 'value2');",
    "UPDATE Database.{getSchema(), 'HT'}.tablename SET column1 = 'new_value' WHERE condition = 'value';",
    "DELETE FROM Database.{getSchema(), 'HT'}.tablename WHERE condition = 'value';"
]

parse_sql_statements(sql_statements)