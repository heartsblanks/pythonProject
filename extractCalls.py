import os
import re
import sqlite3

def create_database():
    conn = sqlite3.connect("esql_analysis.db")
    cursor = conn.cursor()
    
    # Create Modules table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS modules (
            module_id INTEGER PRIMARY KEY AUTOINCREMENT,
            module_name TEXT,
            file_name TEXT,
            UNIQUE(module_name, file_name)
        )
    ''')

    # Create Functions table
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

    # Create SQL Operations table
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

    # Create Calls table
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

def get_esql_definitions_and_calls(directory_path, conn):
    # Patterns to detect SQL components and calls
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    next_block_pattern = re.compile(r'\b(?:CREATE\s+(?:FUNCTION|PROCEDURE)|END\s+MODULE)\b', re.IGNORECASE)
    call_pattern = re.compile(r'\b(\w+)\s*\(')
    sql_pattern = re.compile(r'\b(PASSTHRU\s*\(\s*)?(SELECT|UPDATE|INSERT|DELETE)\b', re.IGNORECASE)
    table_pattern = re.compile(r'\b(?:FROM|JOIN)\s+(?:\'?\|\|)?[^\s]*\.(\w+)', re.IGNORECASE)
    message_tree_pattern = re.compile(r'\bFROM\s+\w+\s*\[.*?\]', re.IGNORECASE)

    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage", "CARDINALITY", "COALESCE"}

    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.esql'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    content = f.read()

                    # Track module ranges to avoid duplicate entries for standalone functions
                    module_ranges = []

                    # Process modules in the file
                    for module_match in module_pattern.finditer(content):
                        module_name = module_match.group(1)
                        module_start = module_match.end()
                        end_module_match = re.search(r'\bEND\s+MODULE\b', content[module_start:], re.IGNORECASE)
                        module_end = module_start + end_module_match.start() if end_module_match else len(content)
                        module_content = content[module_start:module_end]
                        module_ranges.append((module_start, module_end))

                        module_id = insert_module(conn, file, module_name)

                        # Find all function/procedure definitions within the module
                        for func_match in definition_pattern.finditer(module_content):
                            func_name = func_match.group(1)
                            if func_name not in excluded_procedures:
                                func_start = func_match.end()
                                begin_match = re.search(r'\bBEGIN\b', module_content[func_start:], re.IGNORECASE)

                                if begin_match:
                                    begin_pos = func_start + begin_match.start()
                                    end_match = re.search(r'\bEND\s*;\b', module_content[begin_pos:], re.IGNORECASE)
                                    func_end = begin_pos + end_match.end() if end_match else len(content)
                                else:
                                    next_create = next_block_pattern.search(module_content, func_start)
                                    func_end = next_create.start() if next_create else len(content)

                                func_body = module_content[func_start:func_end]
                                function_id = insert_function(conn, file, func_name, module_id)

                                # Insert calls
                                calls = set(call_pattern.findall(func_body))
                                for call in calls:
                                    if call != func_name and call not in excluded_procedures:
                                        insert_call(conn, function_id, call)

                                # Insert SQL operations
                                for sql_match in sql_pattern.finditer(func_body):
                                    sql_type = sql_match.group(2).upper()
                                    sql_start = sql_match.end()
                                    sql_end = func_body.find(';', sql_start) + 1
                                    sql_statement = func_body[sql_start:sql_end].strip()

                                    if not message_tree_pattern.search(sql_statement):
                                        tables = table_pattern.findall(sql_statement)
                                        for table in tables:
                                            insert_sql_operation(conn, function_id, sql_type, table)

                    # Process standalone functions/procedures
                    for match in definition_pattern.finditer(content):
                        func_name = match.group(1)
                        func_start = match.start()
                        if func_name not in excluded_procedures and not any(start <= func_start < end for start, end in module_ranges):
                            start_pos = match.end()
                            begin_match = re.search(r'\bBEGIN\b', content[start_pos:], re.IGNORECASE)

                            if begin_match:
                                begin_pos = start_pos + begin_match.start()
                                end_match = re.search(r'\bEND\s*;\b', content[begin_pos:], re.IGNORECASE)
                                end_pos = begin_pos + end_match.end() if end_match else len(content)
                            else:
                                next_match = next_block_pattern.search(content, start_pos)
                                end_pos = next_match.start() if next_match else len(content)

                            body_content = content[start_pos:end_pos]
                            function_id = insert_function(conn, file, func_name, None)

                            calls = set(call_pattern.findall(body_content))
                            for call in calls:
                                if call != func_name and call not in excluded_procedures:
                                    insert_call(conn, function_id, call)

                            for sql_match in sql_pattern.finditer(body_content):
                                sql_type = sql_match.group(2).upper()
                                sql_start = sql_match.end()
                                sql_end = body_content.find(';', sql_start) + 1
                                sql_statement = body_content[sql_start:sql_end].strip()

                                if not message_tree_pattern.search(sql_statement):
                                    tables = table_pattern.findall(sql_statement)
                                    for table in tables:
                                        insert_sql_operation(conn, function_id, sql_type, table)

# Example usage
directory_path = '/path/to/your/project'
conn = create_database()
get_esql_definitions_and_calls(directory_path, conn)
conn.close()

CREATE VIEW IF NOT EXISTS esql_details AS
SELECT 
    f.file_name AS File,
    m.module_name AS Module,
    f.function_name AS Function,
    o.operation_type AS OperationType,
    o.table_name AS TableName,
    GROUP_CONCAT(c.call_name, ', ') AS Calls
FROM 
    functions f
LEFT JOIN 
    modules m ON f.module_id = m.module_id
LEFT JOIN 
    sql_operations o ON f.function_id = o.function_id
LEFT JOIN 
    calls c ON f.function_id = c.function_id
GROUP BY 
    f.file_name, m.module_name, f.function_name, o.operation_type, o.table_name;