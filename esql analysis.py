import os
import re
import concurrent.futures
import sqlite3
import logging
from ssh_executor import SSHExecutor  # Assuming SSHExecutor class is in ssh_executor.py

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("esql_analysis.log"),
        logging.StreamHandler()
    ]
)

def create_database():
    conn = sqlite3.connect("esql_analysis.db")
    cursor = conn.cursor()
    
    # Create tables for the analysis if not exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS modules (
            module_id INTEGER PRIMARY KEY AUTOINCREMENT,
            module_name TEXT,
            file_name TEXT,
            folder_name TEXT,
            UNIQUE(module_name, file_name, folder_name)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS functions (
            function_id INTEGER PRIMARY KEY AUTOINCREMENT,
            function_name TEXT,
            file_name TEXT,
            folder_name TEXT,
            module_id INTEGER,
            FOREIGN KEY(module_id) REFERENCES modules(module_id),
            UNIQUE(function_name, file_name, folder_name, module_id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sql_operations (
            sql_id INTEGER PRIMARY KEY AUTOINCREMENT,
            function_id INTEGER,
            operation_type TEXT,
            table_name TEXT,
            FOREIGN KEY(function_id) REFERENCES functions(function_id),
            UNIQUE(function_id, operation_type, table_name)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS calls (
            call_id INTEGER PRIMARY KEY AUTOINCREMENT,
            function_id INTEGER,
            call_name TEXT,
            FOREIGN KEY(function_id) REFERENCES functions(function_id),
            UNIQUE(function_id, call_name)
        )
    ''')
    conn.commit()
    return conn

# Pattern to match table names and function calls in SQL statements
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

def get_esql_definitions_and_calls(file_content, file_name, folder_name):
    """Process the file content to gather calls and SQL operations without inserting into the database."""
    calls_data = []
    sql_operations_data = []
    
    # Patterns for SQL and function analysis
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    sql_pattern = re.compile(r'\b(PASSTHRU\s*\(\s*)?(SELECT|UPDATE|INSERT|DELETE)\b', re.IGNORECASE)
    call_pattern = re.compile(r'\b(\w+)\s*\(')
    message_tree_pattern = re.compile(r'\bFROM\s+\w+\s*\[.*?\]', re.IGNORECASE)

    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage", "CARDINALITY", "COALESCE"}

    # Process functions/procedures
    for func_match in definition_pattern.finditer(file_content):
        func_name = func_match.group(1)
        func_start = func_match.end()
        begin_match = re.search(r'\bBEGIN\b', file_content[func_start:], re.IGNORECASE)
        func_end = file_content.find('END;', func_start) if begin_match else len(file_content)
        func_body = file_content[func_start:func_end]

        # Capture function/procedure calls
        calls = set(call_pattern.findall(func_body))
        for call in calls:
            if call not in excluded_procedures:
                calls_data.append((func_name, call))

        # Capture SQL operations
        for sql_match in sql_pattern.finditer(func_body):
            sql_type = sql_match.group(2).upper()
            sql_statement = func_body[sql_match.end():func_body.find(';', sql_match.end())].strip()
            if not message_tree_pattern.search(sql_statement):
                # Extract the table name or function as table name
                table_matches = table_pattern.findall(sql_statement)
                for table in table_matches:
                    sql_operations_data.append((func_name, sql_type, table))

    return calls_data, sql_operations_data

def analyze_folder(ssh_executor, folder):
    """Collects calls and SQL operations data for each folder without inserting into the database."""
    folder_calls = []
    folder_sql_operations = []

    # List all .esql files and process each
    command = f"find {folder} -type f -name '*.esql'"
    esql_files = ssh_executor.execute_command(command).strip().splitlines()

    for esql_file in esql_files:
        logging.info(f"Fetching file: {esql_file}")
        file_content = read_remote_file_with_fallback(ssh_executor, esql_file)
        calls_data, sql_operations_data = get_esql_definitions_and_calls(file_content, esql_file, folder)
        folder_calls.extend(calls_data)
        folder_sql_operations.extend(sql_operations_data)

    return folder_calls, folder_sql_operations

def main():
    all_calls = []
    all_sql_operations = []

    # Set up SSH connection and thread pool
    with SSHExecutor(hostname="...", private_key_path="...") as ssh_executor:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(analyze_folder, ssh_executor, folder) for folder in folders]
            for future in concurrent.futures.as_completed(futures):
                folder_calls, folder_sql_operations = future.result()
                all_calls.extend(folder_calls)
                all_sql_operations.extend(folder_sql_operations)

    # Bulk insert all collected data after threads finish
    conn = create_database()
    with conn:
        # Insert unique calls
        unique_calls = set((function_id, call_name) for function_id, call_name in all_calls)
        conn.executemany("INSERT OR IGNORE INTO calls (function_id, call_name) VALUES (?, ?)", list(unique_calls))

        # Insert unique SQL operations
        unique_sql_operations = set((function_id, operation_type, table_name) for function_id, operation_type, table_name in all_sql_operations)
        conn.executemany("INSERT OR IGNORE INTO sql_operations (function_id, operation_type, table_name) VALUES (?, ?, ?)", list(unique_sql_operations))

    conn.close()
    logging.info("Esql analysis completed.")

if __name__ == "__main__":
    main()