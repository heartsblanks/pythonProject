import re
import threading

class PropertiesProcessor:
    """Parses property files to extract configuration details for execution groups, queues, data sources, and web service URLs."""

    def __init__(self, db_queue, db_manager):
        self.db_queue = db_queue
        self.db_manager = db_manager

    def process_file(self, file_content, pap_id, pf_id, common_queues, properties_by_env):
        """Processes a property file to extract and categorize configuration details."""
        # Insert or get the property file ID
        property_file_id = self._queue_insert_property_file(pap_id, pf_id, file_content)

        # Insert common queues (shared across environments)
        self._process_queues(property_file_id, common_queues)

        # Process and insert properties for each environment (data sources and web services)
        for env_name, env_properties in properties_by_env.items():
            self._process_environment_properties(property_file_id, env_name, env_properties)

    def _queue_insert_property_file(self, pap_id, pf_id, file_name):
        """Inserts or retrieves a property file entry in the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((
            self.db_manager.insert_property_file, 
            (pap_id, pf_id, file_name), 
            callback_event, 
            result_container
        ))
        callback_event.wait()
        return result_container["result"]

    def _process_queues(self, property_file_id, queues):
        """Processes common queues and inserts them into the database."""
        for queue_name, queue_type in queues.items():
            queue_id = self._queue_insert_queue(queue_name, queue_type)
            self._queue_insert_pap_queue(property_file_id, queue_id)

    def _process_environment_properties(self, property_file_id, env_name, env_properties):
        """Processes environment-specific properties, including databases and web services."""
        # Insert data sources (database names)
        for db_name, db_value in env_properties.get("databases", {}).items():
            self._queue_insert_database(property_file_id, db_name, env_name, db_value)

        # Insert web service URLs
        for ws_name, ws_url in env_properties.get("web_services", {}).items():
            self._queue_insert_web_service(property_file_id, ws_name, env_name, ws_url)

    def _queue_insert_queue(self, queue_name, queue_type):
        """Inserts or retrieves a queue entry in the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_queue, (queue_name, queue_type), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_pap_queue(self, property_file_id, queue_id):
        """Inserts or retrieves an association between a PAP and a queue."""
        self.db_queue.put((self.db_manager.insert_pap_queue, (property_file_id, queue_id), None, {}))

    def _queue_insert_database(self, property_file_id, db_name, environment, value):
        """Inserts a database configuration into the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_database_property, (property_file_id, db_name, environment, value), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_web_service(self, property_file_id, ws_name, environment, url):
        """Inserts a web service URL configuration into the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_web_service, (property_file_id, ws_name, environment, url), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]
