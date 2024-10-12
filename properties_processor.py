import re
import threading

class PropertiesProcessor:
    """Parses property files to extract configuration details for execution groups, queues, data sources, and web service URLs."""

    def __init__(self, db_queue, db_manager):
        self.db_queue = db_queue
        self.db_manager = db_manager

    def process_file(self, file_content, pf_id, mandant_id, common_queues, environment_configs):
        """Processes a property file to extract configuration details."""
        # Insert or get the property file ID
        property_file_id = self._queue_insert_property_file(pf_id, mandant_id, 
                                                            common_queues['execution_group'], 
                                                            common_queues['event_queue'], 
                                                            common_queues['output_queue'], 
                                                            common_queues['copy_queue'])

        # Process each environment-specific configuration
        for env_name, env_data in environment_configs.items():
            environment_id = self._queue_insert_environment(env_name)
            self._queue_insert_environment_property(property_file_id, environment_id, 
                                                    env_data['data_source_name'], 
                                                    env_data['web_service_url'])

    def _queue_insert_property_file(self, pf_id, mandant_id, execution_group, event_queue, output_queue, copy_queue):
        """Inserts or retrieves a property file entry in the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((
            self.db_manager.insert_property_file, 
            (pf_id, mandant_id, execution_group, event_queue, output_queue, copy_queue), 
            callback_event, 
            result_container
        ))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_environment(self, environment_name):
        """Inserts or retrieves an environment entry in the database."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_environment, (environment_name,), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_environment_property(self, property_file_id, environment_id, data_source_name, web_service_url):
        """Inserts or updates an environment-specific property configuration in the database."""
        self.db_queue.put((self.db_manager.insert_environment_property, (property_file_id, environment_id, data_source_name, web_service_url), None, {}))