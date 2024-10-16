import re
import threading
import concurrent.futures
from database_manager import DatabaseManager

class MQDataLoader:
    def __init__(self, db_queue, db_manager, file_content, max_workers=5):
        """
        Initialize with a database queue, DatabaseManager, file content, and thread pool settings.
        
        Parameters:
        - db_queue (queue.Queue): The queue to manage database operations.
        - db_manager (DatabaseManager): An instance of DatabaseManager.
        - file_content (str): The content of the .mqsc file.
        - max_workers (int): Maximum number of threads for concurrent processing.
        """
        self.db_queue = db_queue
        self.db_manager = db_manager
        self.file_content = file_content
        self.max_workers = max_workers

    def parse_and_load(self):
        """Parses the file content and loads data into the database using concurrent futures."""
        # Regex pattern to find each definition and its attributes
        definition_pattern = re.compile(r"^DEFINE\s+(\w+)\s+([^\(]+)(?:\((.+?)\))?", re.MULTILINE)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            # Process each match in the file content
            for match in definition_pattern.finditer(self.file_content):
                definition_type = match.group(1)
                definition_name = match.group(2).strip()
                attributes_str = match.group(3)

                # Submit each definition processing to the executor
                futures.append(executor.submit(self._process_definition, definition_type, definition_name, attributes_str))

            # Wait for all futures to complete
            for future in concurrent.futures.as_completed(futures):
                future.result()  # To catch exceptions, if any

    def _process_definition(self, definition_type, definition_name, attributes_str):
        """Processes a single definition and its attributes."""
        # Queue the insertion of the main definition
        definition_id = self._queue_insert_definition(definition_type, definition_name)

        # If attributes are found, parse and queue their insertion
        if attributes_str:
            attributes = self._parse_attributes(attributes_str)
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit each attribute insertion as a separate task
                futures = [
                    executor.submit(self._queue_insert_attribute, definition_id, key, value)
                    for key, value in attributes.items()
                ]
                for future in concurrent.futures.as_completed(futures):
                    future.result()  # Catch exceptions if any

    def _parse_attributes(self, attributes_str):
        """Parses attributes from a definition string and returns them as a dictionary."""
        attributes = {}
        attribute_pattern = re.compile(r"(\w+)\(([^)]+)\)")
        
        for attr_match in attribute_pattern.finditer(attributes_str):
            key = attr_match.group(1)
            value = attr_match.group(2)
            attributes[key] = value
        
        return attributes

    def _queue_insert_definition(self, definition_type, definition_name):
        """Queues the insertion of a definition into the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((
            self.db_manager.insert_definition,
            (definition_type, definition_name),
            callback_event,
            result_container
        ))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_attribute(self, definition_id, attribute_key, attribute_value):
        """Queues the insertion of an attribute for a definition into the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((
            self.db_manager.insert_attribute,
            (definition_id, attribute_key, attribute_value),
            callback_event,
            result_container
        ))
        callback_event.wait()