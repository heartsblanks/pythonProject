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
    conn.execute("PRAGMA foreign_keys = 1")  # Enable foreign key support
    cursor = conn.cursor()
    
    # Create tables with foreign keys
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
            FOREIGN KEY(module_id) REFERENCES modules(module_id) ON DELETE CASCADE,
            UNIQUE(function_name, file_name, folder_name, module_id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sql_operations (
            sql_id INTEGER PRIMARY KEY AUTOINCREMENT,
            function_id INTEGER,
            operation_type TEXT,
            table_name TEXT,
            FOREIGN KEY(function_id) REFERENCES functions(function_id) ON DELETE CASCADE,
            UNIQUE(function_id, operation_type, table_name)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS calls (
            call_id INTEGER PRIMARY KEY AUTOINCREMENT,
            function_id INTEGER,
            call_name TEXT,
            FOREIGN KEY(function_id) REFERENCES functions(function_id) ON DELETE CASCADE,
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
    """Process the file content to gather modules, functions, calls, and SQL operations without inserting into the database."""
    modules_data = []
    functions_data = []
    calls_data = []
    sql_operations_data = []
    
    # Patterns for SQL and function analysis
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    sql_pattern = re.compile(r'\b(PASSTHRU\s*\(\s*)?(SELECT|UPDATE|INSERT|DELETE)\b', re.IGNORECASE)
    call_pattern = re.compile(r'\b(\w+)\s*\(')
    message_tree_pattern = re.compile(r'\bFROM\s+\w+\s*\[.*?\]', re.IGNORECASE)

    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage", "CARDINALITY", "COALESCE"}

    # Process modules in the file content
    for module_match in module_pattern.finditer(file_content):
        module_name = module_match.group(1)
        modules_data.append((module_name, file_name, folder_name))

        module_start = module_match.end()
        end_module_match = re.search(r'\bEND\s+MODULE\b', file_content[module_start:], re.IGNORECASE)
        module_end = module_start + end_module_match.start() if end_module_match else len(file_content)
        module_content = file_content[module_start:module_end]

        # Process functions/procedures within the module
        for func_match in definition_pattern.finditer(module_content):
            func_name = func_match.group(1)
            func_start = func_match.end()
            begin_match = re.search(r'\bBEGIN\b', module_content[func_start:], re.IGNORECASE)
            func_end = module_content.find('END;', func_start) if begin_match else len(module_content)
            func_body = module_content[func_start:func_end]
            functions_data.append((func_name, file_name, folder_name, module_name))

            # Capture calls within the function/procedure
            calls = set(call_pattern.findall(func_body))
            for call in calls:
                if call not in excluded_procedures:
                    calls_data.append((func_name, call))

            # Capture SQL operations
            for sql_match in sql_pattern.finditer(func_body):
                sql_type = sql_match.group(2).upper()
                sql_statement = func_body[sql_match.end():func_body.find(';', sql_match.end())].strip()
                if not message_tree_pattern.search(sql_statement):
                    # Extract table name or function call as table name
                    table_matches = table_pattern.findall(sql_statement)
                    for table in table_matches:
                        sql_operations_data.append((func_name, sql_type, table))

    return modules_data, functions_data, calls_data, sql_operations_data

def analyze_folder(ssh_executor, folder):
    """Collects modules, functions, calls, and SQL operations data for each folder without inserting into the database."""
    folder_modules = []
    folder_functions = []
    folder_calls = []
    folder_sql_operations = []

    # List all .esql files and process each
    command = f"find {folder} -type f -name '*.esql'"
    esql_files = ssh_executor.execute_command(command).strip().splitlines()

    for esql_file in esql_files:
        logging.info(f"Fetching file: {esql_file}")
        file_content = read_remote_file_with_fallback(ssh_executor, esql_file)
        modules_data, functions_data, calls_data, sql_operations_data = get_esql_definitions_and_calls(file_content, esql_file, folder)
        folder_modules.extend(modules_data)
        folder_functions.extend(functions_data)
        folder_calls.extend(calls_data)
        folder_sql_operations.extend(sql_operations_data)

    return folder_modules, folder_functions, folder_calls, folder_sql_operations

def main():
    all_modules = []
    all_functions = []
    all_calls = []
    all_sql_operations = []

    # Set up SSH connection and thread pool
    with SSHExecutor(hostname="...", private_key_path="...") as ssh_executor:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(analyze_folder, ssh_executor, folder) for folder in folders]
            for future in concurrent.futures.as_completed(futures):
                folder_modules, folder_functions, folder_calls, folder_sql_operations = future.result()
                all_modules.extend(folder_modules)
                all_functions.extend(folder_functions)
                all_calls.extend(folder_calls)
                all_sql_operations.extend(folder_sql_operations)

    # Bulk insert all collected data after threads finish
    conn = create_database()
    with conn:
        # Insert unique modules and get IDs
        unique_modules = set((module_name, file_name, folder_name) for module_name, file_name, folder_name in all_modules)
        conn.executemany("INSERT OR IGNORE INTO modules (module_name, file_name, folder_name) VALUES (?, ?, ?)", list(unique_modules))

        # Insert unique functions and retrieve function/module IDs
        unique_functions = set((function_name, file_name, folder_name, module_name) for function_name, file_name, folder_name, module_name in all_functions)
        conn.executemany("INSERT OR IGNORE INTO functions (function_name, file_name, folder_name, module_id) VALUES (?, ?, ?, ?)", list(unique_functions))

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