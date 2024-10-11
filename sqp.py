import sqlparse

# Function to extract SELECT statements and get content after FROM
def extract_from_clause(sql_statement):
    # Parse the SQL statement
    parsed = sqlparse.parse(sql_statement)[0]
    
    from_clause = None
    capture = False
    result = []
    
    # Iterate through the tokens in the SQL statement
    for token in parsed.tokens:
        # Identify the 'SELECT' statement
        if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
            operation_type = 'SELECT'
        
        # When we hit 'FROM', start capturing
        if token.is_keyword and token.value.upper() == 'FROM':
            capture = True
            continue  # Skip 'FROM' itself
        
        # Stop capturing at the next SQL keyword or end of statement
        if capture:
            if token.is_keyword or token.value == ";":
                break
            result.append(str(token))
    
    from_clause = ''.join(result).strip()
    return operation_type, from_clause

# Function to process SQL file
def process_sql_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Split the file content into individual SQL statements
    statements = sqlparse.split(content)
    
    # Process each statement
    for statement in statements:
        # Check if it's a SELECT statement and extract FROM clause
        if statement.strip().upper().startswith('SELECT'):
            operation_type, from_clause = extract_from_clause(statement)
            print(f"Operation Type: {operation_type}")
            print(f"From Clause: {from_clause}\n")

# Example usage
process_sql_file("your_sql_file.sql")