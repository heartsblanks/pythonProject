import sqlparse

# Sample INSERT SQL statement
sql_insert_example = "INSERT INTO database.{getdb() ,'hr'}.tablename (columns)"

# Parse the SQL statement
parsed = sqlparse.parse(sql_insert_example)[0]  # Parses the statement as a single object

# Initialize variables for operation type and table name
operation_type = None
table_name = None

# Iterate over tokens to find operation type and table name after INTO
for idx, token in enumerate(parsed.tokens):
    if token.ttype is sqlparse.tokens.DML:
        operation_type = token.value  # Extracts the operation type (e.g., INSERT)
    elif token.is_keyword and token.value.upper() == 'INTO':
        # Get the next non-whitespace token as the table name
        next_token = parsed.token_next(idx)
        if isinstance(next_token, sqlparse.sql.Identifier):
            table_name = next_token.get_real_name()

print("Operation Type:", operation_type)
print("Table Name:", table_name)