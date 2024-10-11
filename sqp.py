import sqlparse

# Function to parse each SQL statement and extract table names for all operations
def parse_sql_statements(sql_statements):
    for statement in sql_statements:
        parsed = sqlparse.parse(statement)[0]
        operation_type = None
        table_name = None  # To hold the table name based on the operation type
        
        # Determine the operation type (e.g., SELECT, INSERT, UPDATE, DELETE)
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.DML:
                operation_type = token.value.upper()
                break
        
        # Based on operation type, extract the table name
        if operation_type == 'SELECT' or operation_type == 'DELETE':
            # Look for the FROM clause to get the table name
            for token in parsed.tokens:
                if token.is_keyword and token.value.upper() == 'FROM':
                    # The next non-whitespace token should be the table name
                    next_token = parsed.token_next(parsed.token_index(token))
                    if next_token:
                        table_name = next_token.get_real_name() or next_token.value
                    break
        
        elif operation_type == 'INSERT':
            # Look for the INTO clause to get the table name
            for token in parsed.tokens:
                if token.is_keyword and token.value.upper() == 'INTO':
                    # The next non-whitespace token should be the table name
                    next_token = parsed.token_next(parsed.token_index(token))
                    if next_token:
                        table_name = next_token.get_real_name() or next_token.value
                    break
        
        elif operation_type == 'UPDATE':
            # The next token after UPDATE should be the table name
            for token in parsed.tokens:
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'UPDATE':
                    # The next non-whitespace token should be the table name
                    next_token = parsed.token_next(parsed.token_index(token))
                    if next_token:
                        table_name = next_token.get_real_name() or next_token.value
                    break

        # Output the operation type and table name
        print(f"Operation Type: {operation_type}")
        if table_name:
            print(f"Table Name: {table_name}\n")

# Example usage with extracted SQL statements
sql_statements = [
    "SELECT column1, column2 FROM my_table WHERE condition = 'value';",
    "INSERT INTO my_table (column1, column2) VALUES ('value1', 'value2');",
    "UPDATE my_table SET column1 = 'new_value' WHERE condition = 'value';",
    "DELETE FROM my_table WHERE condition = 'value';"
]

parse_sql_statements(sql_statements)