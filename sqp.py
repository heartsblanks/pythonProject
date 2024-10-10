import sqlparse

# Sample INSERT SQL statement
sql_insert_example = "INSERT INTO database.{getdb() ,'hr'}.tablename (columns)"

# Parse the SQL statement
parsed = sqlparse.parse(sql_insert_example)[0]  # Parses the statement as a single object

# Initialize variables for operation type and table name
operation_type = None
table_name = None

# Extract the tokens and analyze them
for token in parsed.tokens:
    if token.ttype is sqlparse.tokens.DML:
        operation_type = token.value  # Extracts the operation type (e.g., INSERT)
    elif token.is_keyword and token.value.upper() == 'INTO':
        # Find the next non-whitespace token after 'INTO'
        table_name = parsed.token_next_by_instance(token, sqlparse.sql.Identifier).get_real_name()

print("Operation Type:", operation_type)
print("Table Name:", table_name)