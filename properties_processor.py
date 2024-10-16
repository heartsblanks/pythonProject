import re
import threading
from collections import defaultdict

class PropertiesProcessor:
    """Parses property files to extract configuration details for execution groups, queues, data sources, web service URLs, and integration servers."""

    def __init__(self, db_queue, db_manager):
        self.db_queue = db_queue
        self.db_manager = db_manager

    def process_file(self, file_content, pap_id, pf_id):
        """Processes a property file to extract and categorize configuration details."""
        # Insert or get the property file ID
        property_file_id = self._queue_insert_property_file(pap_id, pf_id, file_content)

        # Parse the properties from the file content
        parsed_properties = self._parse_env_properties(file_content)

        # Insert integration server
        if parsed_properties["integration_server"]:
            integration_servers = [parsed_properties["integration_server"]]
            self._process_integration_servers(pap_id, integration_servers)

        # Insert queues
        self._process_queues(property_file_id, parsed_properties["queues"])

        # Insert databases and web services for each environment
        for env_name, env_properties in parsed_properties["database_names"].items():
            for db_name, db_value in env_properties.items():
                self._queue_insert_database(property_file_id, db_name, env_name, db_value)

        for env_name, ws_properties in parsed_properties["webservices"].items():
            for ws_name, ws_url in ws_properties.items():
                self._queue_insert_web_service(property_file_id, ws_name, env_name, ws_url)

        # Insert other properties
        for property_name, env_data in parsed_properties["other_properties"].items():
            for env_name, value in env_data.items():
                self._queue_insert_other_property(property_file_id, property_name, env_name, value)

    def _parse_env_properties(self, content):
        """Parses property file content into categorized properties."""

        # Dictionary to store different categories of properties
        properties = {
            "integration_server": None,
            "queues": set(),  # Store unique queue names ending with _EVT, _ERR, or _CPY
            "database_names": defaultdict(dict),  # Store {property_name: {environment: value}}
            "webservices": defaultdict(dict),  # Store web service URLs {property_name: {environment: value}}
            "other_properties": defaultdict(dict)  # Store other properties not in recognized categories
        }

        # Define regex patterns
        integration_server_pattern = re.compile(r"^prod\.broker\.eg=(.+)$", re.MULTILINE)
        queue_pattern = re.compile(r"=(.+?(_EVT|_ERR|_CPY))$", re.MULTILINE)
        db_property_pattern = re.compile(r"^(?P<env>\w+)\.replace\.replacement\.17=(?P<value>.+)$", re.MULTILINE)
        dynamic_property_pattern = re.compile(r"^(?P<env>\w+)?\.?replace\.replacement\.(?P<prop_num>\d+)=(?P<value>.+)$", re.MULTILINE)
        prop_value_pattern = re.compile(r"^replace\.value\.(?P<num>\d+)=(?P<property_name>.+)$", re.MULTILINE)

        # 1. Integration Server (prod.broker.eg)
        match = integration_server_pattern.search(content)
        if match:
            properties["integration_server"] = match.group(1).strip()

        # 2. Queue Names (_EVT, _ERR, _CPY)
        for match in queue_pattern.finditer(content):
            queue_name = match.group(1).strip()
            properties["queues"].add(queue_name)

        # 3. Specific Database Property ({RPL_DB00}) for `replace.replacement.17`
        db_property_name = "{RPL_DB00}"
        for match in db_property_pattern.finditer(content):
            env = match.group("env")
            properties["database_names"][db_property_name][env] = match.group("value").strip()

        # 4. Dynamic Properties (replace.value.<num> for each property, environment-specific values)
        for match in prop_value_pattern.finditer(content):
            num = match.group("num")  # Number, like "23" in replace.value.23
            property_name = match.group("property_name").strip()

            # Find all env-specific or common values for this property
            for env_match in dynamic_property_pattern.finditer(content):
                env = env_match.group("env") or "common"  # Use "common" if no environment prefix
                prop_num = env_match.group("prop_num")
                value = env_match.group("value").strip()

                # Only process if it matches the current property number
                if prop_num == num:
                    # Check if the property is a database, web service, or queue
                    if "RPL_DB" in property_name:
                        properties["database_names"][property_name][env] = value
                    elif "RPL_" in property_name and "URL" in property_name:
                        properties["webservices"][property_name][env] = value
                    elif any(value in prop_set for prop_set in [properties["queues"], properties["database_names"], properties["webservices"]]):
                        # Skip if the value is already part of queues, database_names, or webservices
                        continue
                    else:
                        properties["other_properties"][property_name][env] = value

        return properties

    # Insert methods for property file, queues, databases, and other properties

    def _queue_insert_property_file(self, pap_id, pf_id, file_name):
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
        for queue_name in queues:
            queue_type = queue_name.split('_')[-1]  # Assume queue type is the suffix like EVT, ERR, CPY
            queue_id = self._queue_insert_queue(queue_name, queue_type)
            self._queue_insert_pap_queue(property_file_id, queue_id)

    def _process_integration_servers(self, pap_id, integration_servers):
        """Processes and inserts integration servers for a specific PAP."""
        for server_name in integration_servers:
            server_id = self._queue_insert_integration_server(server_name)
            self._queue_insert_pap_integration_server(pap_id, server_id)

    def _queue_insert_queue(self, queue_name, queue_type):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_queue, (queue_name, queue_type), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_pap_queue(self, property_file_id, queue_id):
        self.db_queue.put((self.db_manager.insert_pap_queue, (property_file_id, queue_id), None, {}))

    def _queue_insert_database(self, property_file_id, db_name, environment, value):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_database_property, (property_file_id, db_name, environment, value), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_web_service(self, property_file_id, ws_name, environment, url):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_web_service, (property_file_id, ws_name, environment, url), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_integration_server(self, server_name):
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_integration_server, (server_name,), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def _queue_insert_pap_integration_server(self, pap_id, server_id):
        self.db_queue.put((self.db_manager.insert_pap_integration_server, (pap_id, server_id), None, {}))

    def _queue_insert_other_property(self, property_file_id, property_name, environment, value):
        """Inserts non-standard properties that do not fit into the predefined categories."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.insert_other_property, (property_file_id, property_name, environment, value), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]
