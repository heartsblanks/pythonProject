import re
import sqlparse

# Function to extract potential SQL statements from ESQL file content
def extract_sql_statements(esql_content):
    # Regex pattern to capture statements starting with operation type and ending with ;
    sql_pattern = r"\b(SELECT|INSERT|UPDATE|DELETE)\b[\s\S]+?;"
    statements = re.findall(sql_pattern, esql_content, re.IGNORECASE | re.DOTALL)
    
    # Filter statements based on the presence of required keywords
    valid_statements = []
    for statement in statements:
        # Determine operation type by extracting the first word (SELECT, INSERT, UPDATE, DELETE)
        operation_type = statement.split()[0].upper()
        
        # Apply secondary regex based on operation type
        if operation_type == "SELECT":
            if re.search(r"\bFROM\b", statement, re.IGNORECASE):
                valid_statements.append(statement)
        elif operation_type == "INSERT":
            if re.search(r"\bINTO\b", statement, re.IGNORECASE):
                valid_statements.append(statement)
        elif operation_type == "UPDATE":
            if re.search(r"\bSET\b", statement, re.IGNORECASE):
                valid_statements.append(statement)
        elif operation_type == "DELETE":
            if re.search(r"\bFROM\b", statement, re.IGNORECASE):
                valid_statements.append(statement)
    
    return valid_statements

# Function to parse each SQL statement and extract details
def parse_sql_statements(sql_statements):
    for statement in sql_statements:
        parsed = sqlparse.parse(statement)[0]
        operation_type = None
        from_clause = None
        
        # Check the operation type and capture FROM clause for SELECT statements
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.DML:
                operation_type = token.value.upper()
                
            if operation_type == 'SELECT' and token.is_keyword and token.value.upper() == 'FROM':
                # Start capturing everything after FROM until next keyword or end of statement
                from_clause = []
                for next_token in parsed.tokens[parsed.token_index(token)+1:]:
                    if next_token.is_keyword or next_token.value == ";":
                        break
                    from_clause.append(str(next_token))
                
                from_clause = ''.join(from_clause).strip()
                break

        # Output the operation type and FROM clause (if any)
        print(f"Operation Type: {operation_type}")
        if from_clause:
            print(f"From Clause: {from_clause}\n")

# Load the ESQL file content
def process_esql_file(file_path):
    with open(file_path, 'r') as file:
        esql_content = file.read()
    
    # Extract SQL statements from the ESQL content
    sql_statements = extract_sql_statements(esql_content)
    
    # Parse and analyze each SQL statement
    parse_sql_statements(sql_statements)

# Example usage
process_esql_file("your_esql_file.esql")