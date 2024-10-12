import re
import threading
import logging

class MsgFlowProcessor:
    """Parses .msgflow files to extract nodes, subflows, expressions, and user-defined properties."""

    def __init__(self, db_queue, db_manager):
        self.db_queue = db_queue
        self.db_manager = db_manager

    def process_file(self, file_content, file_name, project_id):
        """Process the .msgflow file content with an existing project ID."""
        # Insert the msgflow using the given project_id
        msgflow_id = self._queue_insert_msgflow(file_name, project_id)
        
        # Process nodes within the msgflow
        self._process_nodes(file_content, msgflow_id)

        # Process user-defined properties
        self._process_user_defined_properties(file_content, msgflow_id)

    def _process_nodes(self, content, msgflow_id):
        """Identify and process each node in the .msgflow."""
        node_pattern = re.compile(r'<Node .*?name="([^"]+)".*?>(.*?)</Node>', re.DOTALL)
        
        for match in node_pattern.finditer(content):
            node_name = match.group(1)
            node_content = match.group(2)
            
            is_subflow = '<Subflow' in node_content
            subflow_id = self._queue_insert_subflow(node_name) if is_subflow else None
            node_id = self._queue_insert_node(node_name, msgflow_id, subflow_id)

            # If the node is not a subflow, check for compute expressions
            if not is_subflow:
                self._process_compute_expressions(node_content, node_id)

    def _process_compute_expressions(self, node_content, node_id):
        """Extract compute expressions (code type, module, function, and datasource) within a node."""
        expression_pattern = re.compile(
            r'<Compute.*?codeType="([^"]+)"'
            r'.*?moduleName="([^"]+)"'
            r'.*?functionName="([^"]+)"'
            r'(?:.*?dataSource="([^"]+)")?',
            re.DOTALL
        )
        
        match = expression_pattern.search(node_content)
        if match:
            code_type = match.group(1)
            module_name = match.group(2)
            function_name = match.group(3)
            datasource = match.group(4)
            
            module_id = self._queue_insert_module(module_name)
            function_id = self._queue_insert_function(function_name, module_id)
            self._queue_insert_expression(node_id, module_id, function_id, code_type, datasource)

    def _process_user_defined_properties(self, content, msgflow_id):
        """Extract and store user-defined properties in the .msgflow."""
        properties_pattern = re.compile(r'<UserDefinedProperty .*?name="([^"]+)" value="([^"]+)"', re.DOTALL)
        
        for match in properties_pattern.finditer(content):
            property_name = match.group(1)
            property_value = match.group(2)
            self._queue_insert_user_defined_property(msgflow_id, property_name, property_value)

    # Queue methods to insert data using the db_queue
    def _queue_insert_msgflow(self, msgflow_name, project_id):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_msgflow, (msgflow_name, project_id), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_node(self, node_name, msgflow_id, subflow_id=None):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_node, (node_name, msgflow_id, subflow_id), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_subflow(self, subflow_name):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_subflow, (subflow_name,), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_expression(self, node_id, module_id, function_id, code_type, datasource):
        self.db_queue.put((self.db_manager.insert_expression, (node_id, module_id, function_id, code_type, datasource), None, {}))

    def _queue_insert_module(self, module_name):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_module, (module_name,), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_function(self, function_name, module_id):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_function, (function_name, module_id), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_user_defined_property(self, msgflow_id, property_name, property_value):
        self.db_queue.put((self.db_manager.insert_user_defined_property, (msgflow_id, property_name, property_value), None, {}))