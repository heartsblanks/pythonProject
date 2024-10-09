import os
import re
import concurrent.futures
import sqlite3
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

class DatabaseManager:
    """Manages database setup, table creation, and data insertion."""
    
    def __init__(self, db_path="esql_analysis.db"):
        self.db_path = db_path
        self.create_database()

    def create_database(self):
        """Initialize the SQLite database, tables, and views."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = 1")
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Tables for modules, functions, SQL operations, and calls
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
        # View to summarize data
        cursor.execute('''
            CREATE VIEW function_summary AS
SELECT
    f.function_name,
    GROUP_CONCAT(DISTINCT op.operation_type || ' on ' || op.table_name, ', ') AS operations,
    GROUP_CONCAT(DISTINCT c.call_name, ', ') AS calls
FROM
    functions f
LEFT JOIN
    sql_operations op ON f.function_id = op.function_id
LEFT JOIN
    calls c ON f.function_id = c.function_id
GROUP BY
    f.function_name''')
        conn.commit()
        conn.close()

    def insert_module(self, conn, file_name, module_name, folder_name):
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO modules (file_name, module_name, folder_name)
            VALUES (?, ?, ?)
        ''', (file_name, module_name, folder_name))
        conn.commit()
        return cursor.lastrowid if cursor.lastrowid else cursor.execute(
            "SELECT module_id FROM modules WHERE file_name = ? AND module_name = ? AND folder_name = ?", 
            (file_name, module_name, folder_name)).fetchone()[0]

    def insert_function(self, conn, file_name, function_name, folder_name, module_id=None):
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO functions (file_name, function_name, folder_name, module_id)
            VALUES (?, ?, ?, ?)
        ''', (file_name, function_name, folder_name, module_id))
        conn.commit()
        return cursor.lastrowid if cursor.lastrowid else cursor.execute(
            "SELECT function_id FROM functions WHERE file_name = ? AND function_name = ? AND folder_name = ? AND module_id IS ?", 
            (file_name, function_name, folder_name, module_id)).fetchone()[0]

    def insert_sql_operation(self, conn, function_id, operation_type, table_name):
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO sql_operations (function_id, operation_type, table_name)
            VALUES (?, ?, ?)
        ''', (function_id, operation_type, table_name))
        conn.commit()

    def insert_call(self, conn, function_id, call_name):
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO calls (function_id, call_name)
            VALUES (?, ?)
        ''', (function_id, call_name))
        conn.commit()


class RemoteFileHandler:
    """Handles remote file retrieval, ensuring only the latest version from CVS is fetched in base64."""

    @staticmethod
    def get_latest_file_version_base64(ssh_executor, cvsroot, file_path, repo_directory):
        """Retrieve the latest version of a versioned file from CVS as base64 encoded content."""
        # Remove the ',v' suffix from file_path
        file_path = file_path.rstrip(',v')
        command = f"CVSROOT={cvsroot} cvs checkout -p {file_path} | base64"
        
        # Execute command and get base64 encoded content
        result = ssh_executor.execute_command(command).strip()
        return result

    @staticmethod
    def read_latest_file_content(ssh_executor, cvsroot, file_path, repo_directory):
        """Retrieve the latest version of a file in base64, then decode and return as UTF-8 content."""
        try:
            base64_content = RemoteFileHandler.get_latest_file_version_base64(ssh_executor, cvsroot, file_path, repo_directory)
            # Decode base64 to get file content as UTF-8 string
            decoded_content = base64.b64decode(base64_content).decode('utf-8', errors='ignore')
            return decoded_content
        except Exception as e:
            logging.error(f"Error retrieving or decoding latest version of {file_path}: {e}")
            return ""
    @staticmethod
    def find_esql_files(ssh_executor, folder):
        """Find all .esql files in the specified folder on the remote server."""
        command = f"find {folder} -type f -name '*.esql'"
        esql_files = ssh_executor.execute_command(command).strip().splitlines()
        logging.info(f"Found {len(esql_files)} .esql files in {folder}")
        return esql_files
        
class ESQLProcessor:
    """Parses .esql files to extract modules, functions, SQL operations, and function calls."""

    def __init__(self, db_queue, db_manager):
        self.db_queue = db_queue
        self.db_manager = db_manager

    def process_file(self, file_content, file_name, folder_name):
        module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
        
        # Updated function pattern without case constraints, to handle multi-line function or procedure definitions
        function_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+([A-Za-z0-9_]+)\s*\(\s*', re.DOTALL)
        
        # Final call pattern to exclude purely uppercase names without underscores, support uppercase with underscores
        call_pattern = re.compile(r'([A-Z]*[a-z_]+[A-Za-z0-9_]*)\(\s*', re.DOTALL)
        
        # Updated SQL pattern to capture complex table names for INSERT, SELECT, UPDATE, DELETE
        sql_pattern = re.compile(
    r'''
    ^(?!.*(\*|--|/\*)).*?       # Exclude lines with *, --, or /* before the operation
    (
        \bINSERT\s+INTO\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)\s+  # Match INSERT INTO with table name followed by whitespace
        | \bSELECT\b.*?\bFROM\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)(?=\s|\)|;|,)  # Match SELECT ... FROM with table name until space, ) , or ;
        | \bUPDATE\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)\s+.*?\bSET\b  # Match UPDATE with table name followed by whitespace and SET keyword
        | \bDELETE\s+FROM\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)\s+  # Match DELETE FROM with table name followed by whitespace
    )
    .*?;[\s]*\n                 # Match the rest of the statement until the end with a semicolon and optional spaces/newline
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)
        for module_match in module_pattern.finditer(file_content):
            module_name = module_match.group(1)
            module_id = self._queue_insert_module(file_name, module_name, folder_name)
            
            module_start = module_match.end()
            end_module_match = re.search(r'\bEND\s+MODULE\b', file_content[module_start:], re.IGNORECASE)
            module_end = module_start + end_module_match.start() if end_module_match else len(file_content)
            module_content = file_content[module_start:module_end]

            # Iterate over each function in the module content
            for func_match in function_pattern.finditer(module_content):
                func_name = func_match.group(1)
                function_id = self._queue_insert_function(file_name, func_name, folder_name, module_id)

                # Determine the end of the function body
                func_start = func_match.end()
                next_create_match = function_pattern.search(module_content, func_start)
                func_end = next_create_match.start() if next_create_match else len(module_content)

                func_body = module_content[func_start:func_end]
                
                # Extract SQL operations within the function body
                for sql_match in sql_pattern.finditer(func_body):
                    sql_type = (
                        "INSERT" if sql_match.group(1) else
                        "SELECT" if sql_match.group(2) else
                        "UPDATE" if sql_match.group(3) else
                        "DELETE"
                    )
                    table_name = sql_match.group(1) or sql_match.group(2) or sql_match.group(3) or sql_match.group(4)
                    self._queue_insert_sql_operation(function_id, sql_type, table_name)

                # Extract function calls within the function body
                calls = set(call_pattern.findall(func_body))
                for call in calls:
                    self._queue_insert_call(function_id, call)
                    
    def _queue_insert_module(self, file_name, module_name, folder_name):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_module, (file_name, module_name, folder_name), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_function(self, file_name, function_name, folder_name, module_id):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_function, (file_name, function_name, folder_name, module_id), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_sql_operation(self, function_id, operation_type, table_name):
        self.db_queue.put((self.db_manager.insert_sql_operation, (function_id, operation_type, table_name), None, {}))

    def _queue_insert_call(self, function_id, call_name):
        self.db_queue.put((self.db_manager.insert_call, (function_id, call_name), None, {}))


class DBWriterThread(threading.Thread):
    """Manages a single writer thread that processes database operations from the queue."""

    def __init__(self, db_queue, db_manager):
        super().__init__()
        self.db_queue = db_queue
        self.db_manager = db_manager

    def run(self):
        conn = sqlite3.connect(self.db_manager.db_path)
        conn.execute("PRAGMA foreign_keys = 1")
        while True:
            item = self.db_queue.get()
            if item is None:  # Exit signal
                break
            func, args, callback_event, result_container = item
            try:
                result = func(conn, *args)
                if result_container is not None:
                    result_container["result"] = result
                if callback_event is not None:
                    callback_event.set()
            except Exception as e:
                logging.error(f"Error in db_writer: {e}")
            self.db_queue.task_done()
        conn.close()


# Usage example
def main():
    db_queue = queue.Queue()
    db_manager = DatabaseManager()
    db_writer = DBWriterThread(db_queue, db_manager)
    db_writer.start()

    with SSHExecutor(hostname="...", private_key_path="...") as ssh_executor:
        file_handler = RemoteFileHandler()
        esql_processor = ESQLProcessor(db_queue, db_manager)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for folder in folders:  # 'folders' is a list of target directories on the remote server
                esql_files = file_handler.find_esql_files(ssh_executor, folder)
                for esql_file in esql_files:
                    file_content = file_handler.read_remote_file_with_fallback(ssh_executor, esql_file)
                    executor.submit(esql_processor.process_file, file_content, esql_file, folder)

    db_queue.put(None)  # Signal db_writer to stop
    db_writer.join()

if __name__ == "__main__":
    main()