 import chardet

def get_remote_file_encoding(ssh_executor, file_path):
    """Detect the file encoding on the remote server using `chardet`."""
    # Read the file as binary data to detect encoding
    command = f"cat {file_path}"
    raw_content = ssh_executor.execute_command(command).encode('latin1')
    
    # Detect encoding with chardet
    result = chardet.detect(raw_content)
    encoding = result['encoding']
    confidence = result['confidence']
    print(f"Detected encoding: {encoding} (Confidence: {confidence})")
    
    return encoding

def read_remote_file_with_detected_encoding(ssh_executor, file_path):
    """Read the content of a remote file using detected encoding."""
    # Detect the encoding of the file
    encoding = get_remote_file_encoding(ssh_executor, file_path)
    
    # Retrieve the file content as binary and decode with detected encoding
    command = f"cat {file_path}"
    raw_content = ssh_executor.execute_command(command).encode('latin1')
    content = raw_content.decode(encoding, errors='replace')
    
    return content
    
    
import base64

def get_remote_file_base64(ssh_executor, file_path):
    """Retrieve file content from the remote server as base64 to avoid encoding issues."""
    # Use base64 to safely transfer the file content
    command = f"base64 {file_path}"
    base64_content = ssh_executor.execute_command(command).strip()
    
    # Decode the base64 content to get the original binary data
    binary_content = base64.b64decode(base64_content)
    
    return binary_content

def read_remote_file_with_fallback(ssh_executor, file_path):
    """Read file content using base64 and decode with fallback options."""
    # Retrieve the binary content using base64 encoding
    binary_content = get_remote_file_base64(ssh_executor, file_path)
    
    # Try decoding with UTF-8 first, then Latin-1 with replacement as a fallback
    try:
        return binary_content.decode('utf-8')
    except UnicodeDecodeError:
        return binary_content.decode('latin1', errors='replace')
        
        
        
import time
import sqlite3

def retry_on_lock(func):
    """Retry a function if 'database is locked' error occurs."""
    def wrapper(*args, **kwargs):
        max_retries = 5
        delay = 0.1  # Delay in seconds between retries
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    if attempt < max_retries - 1:
                        time.sleep(delay)  # Wait before retrying
                    else:
                        raise  # Re-raise if maximum retries reached
                else:
                    raise  # Re-raise for any other OperationalError
    return wrapper
    
    
import re

# Define the comprehensive table pattern
table_pattern = re.compile(
    r"""
    \b(?:[a-zA-Z_]+\.){0,2}                       # Optional database and schema (e.g., Database.Schema.)
    (?:
        [a-zA-Z_][\w]*                            # Case 1: Plain table name (e.g., TableName)
        |                                         # OR
        [a-zA-Z_]+\s*\(\s*[^)]+\s*\)              # Case 2: Function with parameters as table name (e.g., somefunction(param))
    )
    \b
    """, 
    re.VERBOSE
)

def get_esql_definitions_and_calls(file_content, conn, file_name, folder_name):
    logging.info(f"Analyzing file: {file_name}")
    
    # Patterns for SQL and function analysis
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    sql_pattern = re.compile(r'\b(PASSTHRU\s*\(\s*)?(SELECT|UPDATE|INSERT|DELETE)\b', re.IGNORECASE)
    call_pattern = re.compile(r'\b(\w+)\s*\(')
    message_tree_pattern = re.compile(r'\bFROM\s+\w+\s*\[.*?\]', re.IGNORECASE)

    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage", "CARDINALITY", "COALESCE"}

    module_ranges = []
    # Process modules in the file content
    for module_match in module_pattern.finditer(file_content):
        module_name = module_match.group(1)
        module_start = module_match.end()
        end_module_match = re.search(r'\bEND\s+MODULE\b', file_content[module_start:], re.IGNORECASE)
        module_end = module_start + end_module_match.start() if end_module_match else len(file_content)
        module_content = file_content[module_start:module_end]
        module_ranges.append((module_start, module_end))

        module_id = insert_module(conn, file_name, module_name, folder_name)

        for func_match in definition_pattern.finditer(module_content):
            func_name = func_match.group(1)
            if func_name not in excluded_procedures:
                func_start = func_match.end()
                begin_match = re.search(r'\bBEGIN\b', module_content[func_start:], re.IGNORECASE)
                func_end = module_content.find('END;', func_start) if begin_match else len(module_content)
                func_body = module_content[func_start:func_end]
                function_id = insert_function(conn, file_name, func_name, folder_name, module_id)

                calls = set(call_pattern.findall(func_body))
                for call in calls:
                    if call != func_name and call not in excluded_procedures:
                        insert_call(conn, function_id, call)

                # Detect and process SQL operations
                for sql_match in sql_pattern.finditer(func_body):
                    sql_type = sql_match.group(2).upper()
                    sql_statement = func_body[sql_match.end():func_body.find(';', sql_match.end())].strip()
                    if not message_tree_pattern.search(sql_statement):
                        # Extract the table name or function as table name
                        table_matches = table_pattern.findall(sql_statement)
                        for table in table_matches:
                            insert_sql_operation(conn, function_id, sql_type, table)

    # Process standalone functions/procedures
    for match in definition_pattern.finditer(file_content):
        func_name = match.group(1)
        func_start = match.start()
        if func_name not in excluded_procedures and not any(start <= func_start < end for start, end in module_ranges):
            function_id = insert_function(conn, file_name, func_name, folder_name)

            body_content = file_content[match.end():]
            calls = set(call_pattern.findall(body_content))
            for call in calls:
                if call != func_name and call not in excluded_procedures:
                    insert_call(conn, function_id, call)

            for sql_match in sql_pattern.finditer(body_content):
                sql_type = sql_match.group(2).upper()
                sql_statement = body_content[sql_match.end():body_content.find(';', sql_match.end())].strip()
                if not message_tree_pattern.search(sql_statement):
                    # Extract the table name or function as table name
                    table_matches = table_pattern.findall(sql_statement)
                    for table in table_matches:
                        insert_sql_operation(conn, function_id, sql_type, table)
                        
                        
@retry_on_lock
def insert_sql_operation(conn, function_id, operation_type, table_name):
    cursor = conn.cursor()
    # Check if the combination of function_id, operation_type, and table_name already exists
    cursor.execute('''
        SELECT sql_id FROM sql_operations
        WHERE function_id = ? AND operation_type = ? AND table_name = ?
    ''', (function_id, operation_type, table_name))
    result = cursor.fetchone()
    
    if result:
        return result[0]  # Return the existing ID if found
    
    # Insert new record if it doesn't already exist
    cursor.execute('''
        INSERT INTO sql_operations (function_id, operation_type, table_name)
        VALUES (?, ?, ?)
    ''', (function_id, operation_type, table_name))
    conn.commit()
    return cursor.lastrowid

@retry_on_lock
def insert_call(conn, function_id, call_name):
    cursor = conn.cursor()
    # Check if the combination of function_id and call_name already exists
    cursor.execute('''
        SELECT call_id FROM calls
        WHERE function_id = ? AND call_name = ?
    ''', (function_id, call_name))
    result = cursor.fetchone()
    
    if result:
        return result[0]  # Return the existing ID if found
    
    # Insert new record if it doesn't already exist
    cursor.execute('''
        INSERT INTO calls (function_id, call_name)
        VALUES (?, ?)
    ''', (function_id, call_name))
    conn.commit()
    return cursor.lastrowid
                        
                        