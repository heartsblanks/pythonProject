import re
import threading
import logging

class ESQLProcessor:
    """Parses .esql files to extract modules, functions, SQL operations, and function calls."""

    def __init__(self, db_queue, db_manager):
        self.db_queue = db_queue
        self.db_manager = db_manager

    def process_file(self, file_content, file_name, folder_name):
        module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+\w+\b.*?\bEND\s+MODULE\b', re.IGNORECASE | re.DOTALL)

        # Process modules and standalone functions separately
        for module_match in re.finditer(module_pattern, file_content):
            module_content = module_match.group(0)
            module_name_match = re.search(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', module_content, re.IGNORECASE)
            module_name = module_name_match.group(1) if module_name_match else None
            module_id = self._queue_insert_module(file_name, module_name, folder_name) if module_name else None

            # Process each function within the module content
            self._process_functions(module_content, file_name, folder_name, module_id)

        # Remove module blocks to get standalone functions
        standalone_content = re.sub(module_pattern, "", file_content).strip()
        
        if standalone_content:
            self._process_functions(standalone_content, file_name, folder_name, None)

    def _process_functions(self, content, file_name, folder_name, module_id):
        """Helper method to process functions, either within a module or standalone."""
        function_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+([A-Za-z0-9_]+)\s*\(\s*', re.DOTALL)

        for func_match in function_pattern.finditer(content):
            func_name = func_match.group(1)
            function_id = self._queue_insert_function(file_name, func_name, folder_name, module_id)

            func_start = func_match.end()
            next_create_match = function_pattern.search(content, func_start)
            func_end = next_create_match.start() if next_create_match else len(content)

            func_body = content[func_start:func_end]
            
            # Extract SQL operations within the function body
            self._process_sql_operations(func_body, function_id)

            # Extract function calls within the function body
            self._process_function_calls(func_body, function_id)

    def _process_sql_operations(self, func_body, function_id):
        """Extract and process SQL operations in a function body."""
        sql_pattern = re.compile(
            r'''
            \bSELECT\b.*?\bFROM\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+) 
            (?=\s|WHERE|;|\)|,|\()

            | \bINSERT\s+INTO\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+) 
            (?=\s|\(|;|,)
            ''', 
            re.IGNORECASE | re.VERBOSE | re.DOTALL
        )

        for sql_match in sql_pattern.finditer(func_body):
            sql_type = "SELECT" if sql_match.group(1) else "INSERT"
            table_name = sql_match.group(1) or sql_match.group(2)
            self._queue_insert_sql_operation(function_id, sql_type, table_name)

    def _process_function_calls(self, func_body, function_id):
        """Extract and process function calls in a function body."""
        call_pattern = re.compile(r'([A-Z]*[a-z_]+[A-Za-z0-9_]*)\(\s*', re.DOTALL)
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