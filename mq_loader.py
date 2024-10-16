import re
import threading
from database_manager import DatabaseManager

class MQDataLoader:
    def __init__(self, db_queue, db_manager, file_content):
        """
        Initialize with a database queue, DatabaseManager, and file content.
        
        Parameters:
        - db_queue (queue.Queue): The queue to manage database operations.
        - db_manager (DatabaseManager): An instance of DatabaseManager.
        - file_content (str): The content of the .mqsc file.
        """
        self.db_queue = db_queue
        self.db_manager = db_manager
        self.file_content = file_content

    def parse_and_load(self):
        """Parses the file content and loads data into the database."""
        # Regex pattern to find each definition and its attributes
        definition_pattern = re.compile(r"^DEFINE\s+(\w+)\s+([^\(]+)(?:\((.+?)\))?", re.MULTILINE)

        # Process each match in the file content
        for match in definition_pattern.finditer(self.file_content):
            definition_type = match.group(1)  # e.g., QLOCAL, SERVICE
            definition_name = match.group(2).strip()  # e.g., MY_QUEUE
            attributes_str = match.group(3)  # e.g., attribute list

            # Queue the insertion of the main definition
            definition_id = self._queue_insert_definition(definition_type, definition_name)

            # If attributes are found, parse and queue their insertion
            if attributes_str:
                attributes = self._parse_attributes(attributes_str)
                for key, value in attributes.items():
                    self._queue_insert_attribute(definition_id, key, value)

    def _parse_attributes(self, attributes_str):
        """
        Parses attributes from a definition string and returns them as a dictionary.
        
        Parameters:
        - attributes_str (str): A string of attributes, e.g., "MAXDEPTH(5000) DEFPSIST(YES)"
        
        Returns:
        - dict: A dictionary with attribute keys and values.
        """
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