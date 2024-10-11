import sqlparse

# Function to parse each SQL statement and extract complex table names for all operations
def parse_sql_statements(sql_statements):
    for statement in sql_statements:
        parsed = sqlparse.parse(statement)[0]
        operation_type = None
        table_name = None  # To hold the complex table name based on the operation type
        
        # Determine the operation type (e.g., SELECT, INSERT, UPDATE, DELETE)
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.DML:
                operation_type = token.value.upper()
                break
        
        # Based on operation type, extract the complex table name
        if operation_type == 'SELECT' or operation_type == 'DELETE':
            # Look for the FROM clause to get the table name
            for token in parsed.tokens:
                if token.is_keyword and token.value.upper() == 'FROM':
                    # Capture complex table name expression
                    table_name = []
                    for next_token in parsed.tokens[parsed.token_index(token)+1:]:
                        if next_token.is_keyword or next_token.value in [";", "("]:
                            break
                        table_name.append(str(next_token))
                    table_name = ''.join(table_name).strip()
                    break
        
        elif operation_type == 'INSERT':
            # Look for the INTO clause to get the table name
            for token in parsed.tokens:
                if token.is_keyword and token.value.upper() == 'INTO':
                    # Capture complex table name expression
                    table_name = []
                    for next_token in parsed.tokens[parsed.token_index(token)+1:]:
                        if next_token.is_keyword or next_token.value in [";", "("]:
                            break
                        table_name.append(str(next_token))
                    table_name = ''.join(table_name).strip()
                    break
        
        elif operation_type == 'UPDATE':
            # Capture the complex table name right after UPDATE
            for token in parsed.tokens:
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'UPDATE':
                    # Capture complex table name expression
                    table_name = []
                    for next_token in parsed.tokens[parsed.token_index(token)+1:]:
                        if next_token.is_keyword or next_token.value in [";", "("]:
                            break
                        table_name.append(str(next_token))
                    table_name = ''.join(table_name).strip()
                    break

        # Output the operation type and complex table name
        print(f"Operation Type: {operation_type}")
        if table_name:
            print(f"Table Name: {table_name}\n")

# Example usage with extracted SQL statements
sql_statements = [
    "SELECT column1, column2 FROM Database.{getSchema(), 'HT'}.my_table WHERE condition = 'value';",
    "INSERT INTO Database.{getSchema(), 'HT'}.my_table (column1, column2) VALUES ('value1', 'value2');",
    "UPDATE Database.{getSchema(), 'HT'}.my_table SET column1 = 'new_value' WHERE condition = 'value';",
    "DELETE FROM Database.{getSchema(), 'HT'}.my_table WHERE condition = 'value';"
]

parse_sql_statements(sql_statements)