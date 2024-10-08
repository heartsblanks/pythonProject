import os
import re
import concurrent.futures
import sqlite3
import time
import logging
import base64
import queue
import threading
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
    """Initialize the SQLite database and tables with foreign keys enabled."""
    conn = sqlite3.connect("esql_analysis.db")
    conn.execute("PRAGMA foreign_keys = 1")  # Enable foreign key support
    conn.execute("PRAGMA journal_mode=WAL;")  # Enable Write-Ahead Logging mode for concurrent writes
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
    conn.close()

def db_writer(queue, conn):
    """Single-threaded function to write database operations from the queue."""
    while True:
        item = queue.get()
        if item is None:  # End signal
            break
        try:
            func, args = item
            func(conn, *args)
        except Exception as e:
            logging.error(f"Error in db_writer: {e}")
        queue.task_done()

def insert_module(conn, file_name, module_name, folder_name):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO modules (file_name, module_name, folder_name)
        VALUES (?, ?, ?)
    ''', (file_name, module_name, folder_name))
    conn.commit()
    return cursor.lastrowid if cursor.lastrowid else cursor.execute(
        "SELECT module_id FROM modules WHERE file_name = ? AND module_name = ? AND folder_name = ?", 
        (file_name, module_name, folder_name)).fetchone()[0]

def insert_function(conn, file_name, function_name, folder_name, module_id=None):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO functions (file_name, function_name, folder_name, module_id)
        VALUES (?, ?, ?, ?)
    ''', (file_name, function_name, folder_name, module_id))
    conn.commit()
    return cursor.lastrowid if cursor.lastrowid else cursor.execute(
        "SELECT function_id FROM functions WHERE file_name = ? AND function_name = ? AND folder_name = ? AND module_id IS ?", 
        (file_name, function_name, folder_name, module_id)).fetchone()[0]

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

def get_remote_file_base64(ssh_executor, file_path):
    """Retrieve file content from the remote server as base64 to avoid encoding issues."""
    command = f"base64 {file_path}"
    base64_content = ssh_executor.execute_command(command).strip()
    
    # Add padding if necessary
    padding_needed = len(base64_content) % 4
    if padding_needed:
        base64_content += "=" * (4 - padding_needed)
    
    # Decode the base64 content
    binary_content = base64.b64decode(base64_content)
    
    return binary_content.decode('utf-8', errors='replace')

def get_esql_definitions_and_calls(file_content, db_queue, file_name, folder_name):
    """Process the file content and add database operations to the queue."""
    # Define patterns for modules, functions, SQL operations, and function calls
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    function_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    sql_pattern = re.compile(r'\b(PASSTHRU\s*\(\s*)?(SELECT|UPDATE|INSERT|DELETE)\b', re.IGNORECASE)
    call_pattern = re.compile(r'\b(\w+)\s*\(')
    message_tree_pattern = re.compile(r'\bFROM\s+\w+\s*\[.*?\]', re.IGNORECASE)
    table_pattern = re.compile(r'\b(?:[a-zA-Z_]+\.){0,2}(?:[a-zA-Z_][\w]*|[a-zA-Z_]+\s*\(\s*[^)]+\s*\))\b', re.VERBOSE)

    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage", "CARDINALITY", "COALESCE"}

    # Process modules in the file content
    for module_match in module_pattern.finditer(file_content):
        module_name = module_match.group(1)
        db_queue.put((insert_module, (file_name, module_name, folder_name)))
        module_id = db_queue.put((insert_module, (file_name, module_name, folder_name)))

        module_start = module_match.end()
        end_module_match = re.search(r'\bEND\s+MODULE\b', file_content[module_start:], re.IGNORECASE)
        module_end = module_start + end_module_match.start() if end_module_match else len(file_content)
        module_content = file_content[module_start:module_end]

        # Process functions within the module
        for func_match in function_pattern.finditer(module_content):
            func_name = func_match.group(1)
            function_id = db_queue.put((insert_function, (file_name, func_name, folder_name, module_id)))

            func_body = module_content[func_match.end():module_content.find('END;', func_match.end())]
            calls = set(call_pattern.findall(func_body))
            for call in calls:
                if call not in excluded_procedures:
                    db_queue.put((insert_call, (function_id, call)))

            for sql_match in sql_pattern.finditer(func_body):
                sql_type = sql_match.group(2).upper()
                sql_statement = func_body[sql_match.end():func_body.find(';', sql_match.end())].strip()
                if not message_tree_pattern.search(sql_statement):
                    tables = table_pattern.findall(sql_statement)
                    for table in tables:
                        db_queue.put((insert_sql_operation, (function_id, sql_type, table)))

def analyze_folder(ssh_executor, folder, db_queue):
    """Analyze each file in the folder and queue data for insertion into the database."""
    command = f"find {folder} -type f -name '*.esql'"
    esql_files = ssh_executor.execute_command(command).strip().splitlines()

    for esql_file in esql_files:
        logging.info(f"Processing file: {esql_file}")
        file_content = get_remote_file_base64(ssh_executor, esql_file)
        get_esql_definitions_and_calls(file_content, db_queue, esql_file, folder)

def main():
    # Initialize database
    create_database()

    db_queue = queue.Queue()
    conn = sqlite3.connect("esql_analysis.db")

    # Start the db_writer thread
    writer_thread = threading.Thread(target=db_writer, args=(db_queue, conn))
    writer_thread.start()

    # Process each folder in a separate thread
    with SSHExecutor(hostname="...", private_key_path="...") as ssh_executor:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(analyze_folder, ssh_executor, folder, db_queue) for folder in folders]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # Ensure any exceptions are raised
                except Exception as e:
                    logging.error(f"Error in thread: {e}")

    # Signal the db_writer thread to stop
    db_queue.put(None)
    writer_thread.join()
    conn.close()

if __name__ == "__main__":
    main()