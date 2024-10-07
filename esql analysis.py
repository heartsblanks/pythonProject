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
            UNIQUE(module_name, file_name)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS functions (
            function_id INTEGER PRIMARY KEY AUTOINCREMENT,
            function_name TEXT,
            file_name TEXT,
            module_id INTEGER,
            FOREIGN KEY(module_id) REFERENCES modules(module_id),
            UNIQUE(function_name, file_name, module_id)
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

def insert_module(conn, file_name, module_name):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO modules (file_name, module_name)
        VALUES (?, ?)
    ''', (file_name, module_name))
    conn.commit()
    return cursor.lastrowid if cursor.lastrowid else cursor.execute(
        "SELECT module_id FROM modules WHERE file_name = ? AND module_name = ?", (file_name, module_name)).fetchone()[0]

def insert_function(conn, file_name, function_name, module_id=None):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO functions (file_name, function_name, module_id)
        VALUES (?, ?, ?)
    ''', (file_name, function_name, module_id))
    conn.commit()
    return cursor.lastrowid if cursor.lastrowid else cursor.execute(
        "SELECT function_id FROM functions WHERE file_name = ? AND function_name = ? AND module_id IS ?", 
        (file_name, function_name, module_id)).fetchone()[0]

def insert_sql_operation(conn, function_id, operation_type, table_name):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO sql_operations (function_id, operation_type, table_name)
        VALUES (?, ?, ?)
    ''', (function_id, operation_type, table_name))
    conn.commit()

def insert_call(conn, function_id, call_name):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO calls (function_id, call_name)
        VALUES (?, ?)
    ''', (function_id, call_name))
    conn.commit()

def get_esql_definitions_and_calls(file_content, conn, file_name):
    logging.info(f"Analyzing file: {file_name}")
    
    # Patterns for SQL and function analysis
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    sql_pattern = re.compile(r'\b(PASSTHRU\s*\(\s*)?(SELECT|UPDATE|INSERT|DELETE)\b', re.IGNORECASE)
    table_pattern = re.compile(r'\b(?:FROM|JOIN)\s+(?:\'?\|\|)?[^\s]*\.(\w+)', re.IGNORECASE)
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

        module_id = insert_module(conn, file_name, module_name)

        for func_match in definition_pattern.finditer(module_content):
            func_name = func_match.group(1)
            if func_name not in excluded_procedures:
                func_start = func_match.end()
                begin_match = re.search(r'\bBEGIN\b', module_content[func_start:], re.IGNORECASE)
                func_end = module_content.find('END;', func_start) if begin_match else len(module_content)
                func_body = module_content[func_start:func_end]
                function_id = insert_function(conn, file_name, func_name, module_id)

                calls = set(call_pattern.findall(func_body))
                for call in calls:
                    if call != func_name and call not in excluded_procedures:
                        insert_call(conn, function_id, call)

                for sql_match in sql_pattern.finditer(func_body):
                    sql_type = sql_match.group(2).upper()
                    sql_statement = func_body[sql_match.end():func_body.find(';', sql_match.end())].strip()
                    if not message_tree_pattern.search(sql_statement):
                        tables = table_pattern.findall(sql_statement)
                        for table in tables:
                            insert_sql_operation(conn, function_id, sql_type, table)

    # Process standalone functions/procedures
    for match in definition_pattern.finditer(file_content):
        func_name = match.group(1)
        func_start = match.start()
        if func_name not in excluded_procedures and not any(start <= func_start < end for start, end in module_ranges):
            function_id = insert_function(conn, file_name, func_name, None)

            body_content = file_content[match.end():]
            calls = set(call_pattern.findall(body_content))
            for call in calls:
                if call != func_name and call not in excluded_procedures:
                    insert_call(conn, function_id, call)

            for sql_match in sql_pattern.finditer(body_content):
                sql_type = sql_match.group(2).upper()
                sql_statement = body_content[sql_match.end():body_content.find(';', sql_match.end())].strip()
                if not message_tree_pattern.search(sql_statement):
                    tables = table_pattern.findall(sql_statement)
                    for table in tables:
                        insert_sql_operation(conn, function_id, sql_type, table)

def analyze_folder(ssh_executor, folder, conn):
    """Analyze all .esql files in a given folder on a remote server."""
    logging.info(f"Starting analysis for folder: {folder}")
    try:
        command = f"find {folder} -type f -name '*.esql'"
        esql_files = ssh_executor.execute_command(command).strip().splitlines()

        for esql_file in esql_files:
            logging.info(f"Fetching file: {esql_file}")
            command = f"cat {esql_file}"
            file_content = ssh_executor.execute_command(command)
            get_esql_definitions_and_calls(file_content, conn, esql_file)

        logging.info(f"Completed analysis for folder: {folder}")

    except Exception as e:
        logging.error(f"Error processing folder {folder}: {e}")

def main():
    conn = create_database()
    hostname = "remote_server_hostname"
    private_key_path = "/path/to/private/key"
    username = "remote_user"

    with SSHExecutor(hostname, private_key_path, username=username) as ssh_executor:
        main_directory = "/path/to/main/directory"
        logging.info("Retrieving folder list...")
        folders = ssh_executor.execute_command(f"find {main_directory} -mindepth 1 -type d").strip().splitlines()
        logging.info(f"Found {len(folders)} folders to analyze.")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(analyze_folder, ssh_executor, folder, conn) for folder in folders]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in folder analysis task: {e}")

    conn.close()
    logging.info("Esql analysis completed.")

if __name__ == "__main__":
    main()