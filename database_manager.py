import sqlite3

class DatabaseManager:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = 1")  # Enable foreign keys

    def create_database(self):
        """Creates all tables and views in the database."""
        cursor = self.conn.cursor()
        
        # Projects Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Projects (
                project_id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_name TEXT UNIQUE NOT NULL
            )
        """)

        # PFNumbers Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PFNumbers (
                pf_id INTEGER PRIMARY KEY AUTOINCREMENT,
                pf_number TEXT NOT NULL,
                project_id INTEGER NOT NULL,
                FOREIGN KEY (project_id) REFERENCES Projects(project_id)
            )
        """)

        # Mandants Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Mandants (
                mandant_id INTEGER PRIMARY KEY AUTOINCREMENT,
                mandant_name TEXT NOT NULL,
                pf_id INTEGER NOT NULL,
                project_id INTEGER NOT NULL,
                FOREIGN KEY (pf_id) REFERENCES PFNumbers(pf_id),
                FOREIGN KEY (project_id) REFERENCES Projects(project_id)
            )
        """)

        # PropertyFiles Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PropertyFiles (
                property_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                pf_id INTEGER NOT NULL,
                mandant_id INTEGER NOT NULL,
                execution_group TEXT,
                event_queue TEXT,
                output_queue TEXT,
                copy_queue TEXT,
                FOREIGN KEY (pf_id) REFERENCES PFNumbers(pf_id),
                FOREIGN KEY (mandant_id) REFERENCES Mandants(mandant_id)
            )
        """)

        # Environments Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Environments (
                environment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                environment_name TEXT UNIQUE NOT NULL
            )
        """)

        # EnvironmentProperties Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS EnvironmentProperties (
                env_property_id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_file_id INTEGER NOT NULL,
                environment_id INTEGER NOT NULL,
                data_source_name TEXT,
                web_service_url TEXT,
                FOREIGN KEY (property_file_id) REFERENCES PropertyFiles(property_file_id),
                FOREIGN KEY (environment_id) REFERENCES Environments(environment_id)
            )
        """)

        # MsgFlows Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MsgFlows (
                msgflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
                msgflow_name TEXT NOT NULL,
                project_id INTEGER NOT NULL,
                FOREIGN KEY (project_id) REFERENCES Projects(project_id)
            )
        """)

        # Nodes Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Nodes (
                node_id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_name TEXT NOT NULL,
                msgflow_id INTEGER NOT NULL,
                subflow_id INTEGER,
                FOREIGN KEY (msgflow_id) REFERENCES MsgFlows(msgflow_id),
                FOREIGN KEY (subflow_id) REFERENCES Subflows(subflow_id)
            )
        """)

        # Subflows Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Subflows (
                subflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
                subflow_name TEXT UNIQUE NOT NULL
            )
        """)

        # Expressions Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Expressions (
                expression_id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                module_id INTEGER NOT NULL,
                function_id INTEGER NOT NULL,
                code_type TEXT,
                datasource TEXT,
                FOREIGN KEY (node_id) REFERENCES Nodes(node_id),
                FOREIGN KEY (module_id) REFERENCES Modules(module_id),
                FOREIGN KEY (function_id) REFERENCES Functions(function_id)
            )
        """)

        # UserDefinedProperties Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS UserDefinedProperties (
                property_id INTEGER PRIMARY KEY AUTOINCREMENT,
                msgflow_id INTEGER NOT NULL,
                property_name TEXT NOT NULL,
                property_value TEXT,
                FOREIGN KEY (msgflow_id) REFERENCES MsgFlows(msgflow_id)
            )
        """)

        # EsqlFiles Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS EsqlFiles (
                esql_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                esql_file_name TEXT NOT NULL,
                project_id INTEGER NOT NULL,
                FOREIGN KEY (project_id) REFERENCES Projects(project_id)
            )
        """)

        # Functions Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Functions (
                function_id INTEGER PRIMARY KEY AUTOINCREMENT,
                function_name TEXT NOT NULL,
                esql_file_id INTEGER NOT NULL,
                FOREIGN KEY (esql_file_id) REFERENCES EsqlFiles(esql_file_id)
            )
        """)

        # Modules Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Modules (
                module_id INTEGER PRIMARY KEY AUTOINCREMENT,
                module_name TEXT NOT NULL,
                esql_file_id INTEGER NOT NULL,
                FOREIGN KEY (esql_file_id) REFERENCES EsqlFiles(esql_file_id)
            )
        """)

        # SQL_Operations Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SQL_Operations (
                operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                function_id INTEGER NOT NULL,
                operation_type TEXT NOT NULL,
                table_name TEXT NOT NULL,
                FOREIGN KEY (function_id) REFERENCES Functions(function_id)
            )
        """)

        # Calls Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Calls (
                call_id INTEGER PRIMARY KEY AUTOINCREMENT,
                function_id INTEGER NOT NULL,
                call_name TEXT NOT NULL,
                FOREIGN KEY (function_id) REFERENCES Functions(function_id)
            )
        """)

        # Create View
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS summary_view AS
            SELECT 
                p.project_name,
                mf.msgflow_name,
                f.function_name,
                m.module_name,
                GROUP_CONCAT(DISTINCT op.operation_type || ' on ' || op.table_name, ', ') AS operations,
                GROUP_CONCAT(DISTINCT c.call_name, ', ') AS calls
            FROM 
                Projects p
                JOIN MsgFlows mf ON p.project_id = mf.project_id
                LEFT JOIN Nodes n ON mf.msgflow_id = n.msgflow_id
                LEFT JOIN Expressions e ON n.node_id = e.node_id
                LEFT JOIN Functions f ON e.function_id = f.function_id
                LEFT JOIN Modules m ON e.module_id = m.module_id
                LEFT JOIN SQL_Operations op ON f.function_id = op.function_id
                LEFT JOIN Calls c ON f.function_id = c.function_id
            GROUP BY 
                p.project_name, mf.msgflow_name, f.function_name, m.module_name
        """)

        # Create PropertyFiles Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PropertyFiles (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                pap_id INTEGER NOT NULL,
                pf_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                FOREIGN KEY (pap_id) REFERENCES PAP(pap_id)
            )
        """)

        # Create Queues Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Queues (
                queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                queue_type TEXT NOT NULL CHECK (queue_type IN ('EVT', 'ERR', 'CPY'))
            )
        """)

        # Create PAP_Queues Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PAP_Queues (
    pap_queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pap_id INTEGER NOT NULL,
    pf_id INTEGER NOT NULL,
    queue_id INTEGER NOT NULL,
    FOREIGN KEY (pap_id) REFERENCES PAP(pap_id),
    FOREIGN KEY (pf_id) REFERENCES PropertyFiles(pf_id),
    FOREIGN KEY (queue_id) REFERENCES Queues(queue_id),
    UNIQUE (pap_id, pf_id, queue_id)  -- Ensures no duplicate entries for the same PAP-PF-Queue combination
)
        """)

        # Create DatabaseProperties Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DatabaseProperties (
                db_property_id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                db_name TEXT NOT NULL,
                environment TEXT NOT NULL,
                value TEXT NOT NULL,
                FOREIGN KEY (file_id) REFERENCES PropertyFiles(file_id)
            )
        """)

        # Create WebServices Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WebServices (
                ws_property_id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                ws_name TEXT NOT NULL,
                environment TEXT NOT NULL,
                url TEXT NOT NULL,
                FOREIGN KEY (file_id) REFERENCES PropertyFiles(file_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IntegrationServers (
    server_id INTEGER PRIMARY KEY AUTOINCREMENT,
    server_name TEXT NOT NULL UNIQUE
)
        """)
        cursor.execute("""
            CREATE TABLE PAP_IntegrationServers (
    pap_server_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pap_id INTEGER NOT NULL,
    server_id INTEGER NOT NULL,
    FOREIGN KEY (pap_id) REFERENCES PAP(pap_id),
    FOREIGN KEY (server_id) REFERENCES IntegrationServers(server_id),
    UNIQUE (pap_id, server_id)  -- Ensures no duplicate entries for the same PAP-Server pair
)
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS OtherProperties (
    other_property_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_file_id INTEGER NOT NULL,
    property_name TEXT NOT NULL,
    environment TEXT,
    value TEXT,
    FOREIGN KEY (property_file_id) REFERENCES PropertyFiles(pf_id),
    UNIQUE (property_file_id, property_name, environment)
)
        """)
        # Definitions Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Definitions (
                definition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Attributes Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Attributes (
                attribute_id INTEGER PRIMARY KEY AUTOINCREMENT,
                definition_id INTEGER NOT NULL,
                attribute_key TEXT NOT NULL,
                attribute_value TEXT,
                FOREIGN KEY (definition_id) REFERENCES Definitions(definition_id),
                UNIQUE (definition_id, attribute_key)
            );
        """)

        # Relationships Table (optional, in case we need relationships between definitions)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Relationships (
                relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
                definition_id INTEGER NOT NULL,
                related_definition_id INTEGER NOT NULL,
                relationship_type TEXT,
                FOREIGN KEY (definition_id) REFERENCES Definitions(definition_id),
                FOREIGN KEY (related_definition_id) REFERENCES Definitions(definition_id)
            );
        """)
        
        conn.commit()
        conn.close()
        
        self.conn.commit()
    def insert_project(self, project_name):
        cursor = self.conn.cursor()
        cursor.execute("SELECT project_id FROM Projects WHERE project_name = ?", (project_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]  # Return existing ID
        else:
            cursor.execute("INSERT INTO Projects (project_name) VALUES (?)", (project_name,))
            self.conn.commit()
            return cursor.lastrowid

    def insert_pf_number(self, pf_number, project_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT pf_id FROM PFNumbers WHERE pf_number = ? AND project_id = ?", (pf_number, project_id))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO PFNumbers (pf_number, project_id) VALUES (?, ?)", (pf_number, project_id))
            self.conn.commit()
            return cursor.lastrowid

    def insert_mandant(self, mandant_name, pf_id, project_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT mandant_id FROM Mandants WHERE mandant_name = ? AND pf_id = ? AND project_id = ?", (mandant_name, pf_id, project_id))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO Mandants (mandant_name, pf_id, project_id) VALUES (?, ?, ?)", (mandant_name, pf_id, project_id))
            self.conn.commit()
            return cursor.lastrowid

    def insert_property_file(self, pf_id, mandant_id, execution_group, event_queue, output_queue, copy_queue):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT property_file_id FROM PropertyFiles 
            WHERE pf_id = ? AND mandant_id = ? AND execution_group = ? AND event_queue = ? AND output_queue = ? AND copy_queue = ?
        """, (pf_id, mandant_id, execution_group, event_queue, output_queue, copy_queue))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("""
                INSERT INTO PropertyFiles (pf_id, mandant_id, execution_group, event_queue, output_queue, copy_queue)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (pf_id, mandant_id, execution_group, event_queue, output_queue, copy_queue))
            self.conn.commit()
            return cursor.lastrowid

    def insert_environment(self, environment_name):
        cursor = self.conn.cursor()
        cursor.execute("SELECT environment_id FROM Environments WHERE environment_name = ?", (environment_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO Environments (environment_name) VALUES (?)", (environment_name,))
            self.conn.commit()
            return cursor.lastrowid

    def insert_environment_property(self, property_file_id, environment_id, data_source_name, web_service_url):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT env_property_id FROM EnvironmentProperties 
            WHERE property_file_id = ? AND environment_id = ? AND data_source_name = ? AND web_service_url = ?
        """, (property_file_id, environment_id, data_source_name, web_service_url))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("""
                INSERT INTO EnvironmentProperties (property_file_id, environment_id, data_source_name, web_service_url)
                VALUES (?, ?, ?, ?)
            """, (property_file_id, environment_id, data_source_name, web_service_url))
            self.conn.commit()
            return cursor.lastrowid

    def insert_msgflow(self, msgflow_name, project_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT msgflow_id FROM MsgFlows WHERE msgflow_name = ? AND project_id = ?", (msgflow_name, project_id))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO MsgFlows (msgflow_name, project_id) VALUES (?, ?)", (msgflow_name, project_id))
            self.conn.commit()
            return cursor.lastrowid

    def insert_node(self, node_name, msgflow_id, subflow_id=None):
        cursor = self.conn.cursor()
        cursor.execute("SELECT node_id FROM Nodes WHERE node_name = ? AND msgflow_id = ?", (node_name, msgflow_id))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO Nodes (node_name, msgflow_id, subflow_id) VALUES (?, ?, ?)", (node_name, msgflow_id, subflow_id))
            self.conn.commit()
            return cursor.lastrowid

    def insert_subflow(self, subflow_name):
        cursor = self.conn.cursor()
        cursor.execute("SELECT subflow_id FROM Subflows WHERE subflow_name = ?", (subflow_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO Subflows (subflow_name) VALUES (?)", (subflow_name,))
            self.conn.commit()
            return cursor.lastrowid

    def insert_expression(self, node_id, module_id, function_id, code_type, datasource):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT expression_id FROM Expressions 
            WHERE node_id = ? AND module_id = ? AND function_id = ? AND code_type = ? AND datasource IS ?
        """, (node_id, module_id, function_id, code_type, datasource))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("""
                INSERT INTO Expressions (node_id, module_id, function_id, code_type, datasource)
                VALUES (?, ?, ?, ?, ?)
            """, (node_id, module_id, function_id, code_type, datasource))
            self.conn.commit()
            return cursor.lastrowid

    def insert_user_defined_property(self, msgflow_id, property_name, property_value):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT property_id FROM UserDefinedProperties WHERE msgflow_id = ? AND property_name = ? AND property_value = ?
        """, (msgflow_id, property_name, property_value))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute("""
                INSERT INTO UserDefinedProperties (msgflow_id, property_name, property_value)
                VALUES (?, ?, ?)
            """, (msgflow_id, property_name, property_value))
            self.conn.commit()
            return cursor.lastrowid
    
    def get_primary_key_columns(self, conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()
    return [info[1] for info in table_info if info[5] > 0]
    def upsert_data(self, conn, base_insert_query, query_values):
    cursor = conn.cursor()
    cursor.execute(base_insert_query, query_values)
    conn.commit()
    # Insert methods with existing record checks

    def insert_pap(self, pap_name, description=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT pap_id FROM PAP WHERE pap_name = ?", (pap_name,))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO PAP (pap_name, description) VALUES (?, ?)
        """, (pap_name, description))
        conn.commit()
        pap_id = cursor.lastrowid
        conn.close()
        return pap_id

    def insert_property_file(self, pap_id, pf_id, file_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT file_id FROM PropertyFiles WHERE pap_id = ? AND pf_id = ? AND file_name = ?", (pap_id, pf_id, file_name))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO PropertyFiles (pap_id, pf_id, file_name) VALUES (?, ?, ?)
        """, (pap_id, pf_id, file_name))
        conn.commit()
        file_id = cursor.lastrowid
        conn.close()
        return file_id

    def insert_queue(self, queue_name, queue_type):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT queue_id FROM Queues WHERE queue_name = ? AND queue_type = ?", (queue_name, queue_type))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO Queues (queue_name, queue_type) VALUES (?, ?)
        """, (queue_name, queue_type))
        conn.commit()
        queue_id = cursor.lastrowid
        conn.close()
        return queue_id

    def insert_pap_queue(self, pap_id, queue_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT pap_queue_id FROM PAP_Queues WHERE pap_id = ? AND pf_id = ? AND queue_id = ?", (pap_id, pf_id, queue_id))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO PAP_Queues (pap_id, pf_id, queue_id) VALUES (?, ?, ?)
        """, (pap_id, pf_id, queue_id))
        conn.commit()
        pap_queue_id = cursor.lastrowid
        conn.close()
        return pap_queue_id

    def insert_database_property(self, file_id, db_name, environment, value):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT db_property_id FROM DatabaseProperties WHERE file_id = ? AND db_name = ? AND environment = ?", (file_id, db_name, environment))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO DatabaseProperties (file_id, db_name, environment, value) VALUES (?, ?, ?, ?)
        """, (file_id, db_name, environment, value))
        conn.commit()
        db_property_id = cursor.lastrowid
        conn.close()
        return db_property_id

    def insert_web_service(self, file_id, ws_name, environment, url):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT ws_property_id FROM WebServices WHERE file_id = ? AND ws_name = ? AND environment = ?", (file_id, ws_name, environment))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO WebServices (file_id, ws_name, environment, url) VALUES (?, ?, ?, ?)
        """, (file_id, ws_name, environment, url))
        conn.commit()
        ws_property_id = cursor.lastrowid
        conn.close()
        return ws_property_id
    def insert_integration_server(self, server_name):
        """Inserts or retrieves an integration server entry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT server_id FROM IntegrationServers WHERE server_name = ?", (server_name,))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO IntegrationServers (server_name) VALUES (?)
        """, (server_name,))
        conn.commit()
        server_id = cursor.lastrowid
        conn.close()
        return server_id

    def insert_pap_integration_server(self, pap_id, server_id):
        """Links a PAP with an integration server, if not already linked."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT pap_server_id FROM PAP_IntegrationServers WHERE pap_id = ? AND server_id = ?", (pap_id, server_id))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        cursor.execute("""
            INSERT INTO PAP_IntegrationServers (pap_id, server_id) VALUES (?, ?)
        """, (pap_id, server_id))
        conn.commit()
        pap_server_id = cursor.lastrowid
        conn.close()
        return pap_server_id
    def insert_other_property(self, property_file_id, property_name, environment, value):
        """Inserts or retrieves an 'other' property entry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if the property already exists
        cursor.execute("""
            SELECT other_property_id 
            FROM OtherProperties 
            WHERE property_file_id = ? AND property_name = ? AND environment = ?
        """, (property_file_id, property_name, environment))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]

        # Insert the property if it doesn't already exist
        cursor.execute("""
            INSERT INTO OtherProperties (property_file_id, property_name, environment, value)
            VALUES (?, ?, ?, ?)
        """, (property_file_id, property_name, environment, value))
        conn.commit()
        other_property_id = cursor.lastrowid
        conn.close()
        return other_property_id
        
    

    def insert_definition(self, type, name):
        """Inserts a new definition entry or retrieves the existing definition_id."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if the definition already exists
        cursor.execute("SELECT definition_id FROM Definitions WHERE type = ? AND name = ?", (type, name))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]  # Return existing definition_id

        # Insert new definition
        cursor.execute("INSERT INTO Definitions (type, name) VALUES (?, ?)", (type, name))
        conn.commit()
        definition_id = cursor.lastrowid
        conn.close()
        return definition_id

    def insert_attribute(self, definition_id, attribute_key, attribute_value):
        """Inserts a new attribute for a definition or updates it if it exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if the attribute already exists for this definition
        cursor.execute("""
            SELECT attribute_id FROM Attributes 
            WHERE definition_id = ? AND attribute_key = ?
        """, (definition_id, attribute_key))
        result = cursor.fetchone()
        
        if result:
            # Update existing attribute
            cursor.execute("""
                UPDATE Attributes SET attribute_value = ? 
                WHERE attribute_id = ?
            """, (attribute_value, result[0]))
        else:
            # Insert new attribute
            cursor.execute("""
                INSERT INTO Attributes (definition_id, attribute_key, attribute_value)
                VALUES (?, ?, ?)
            """, (definition_id, attribute_key, attribute_value))
        
        conn.commit()
        conn.close()

    def insert_relationship(self, definition_id, related_definition_id, relationship_type):
        """Inserts a new relationship between definitions."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR IGNORE INTO Relationships (definition_id, related_definition_id, relationship_type)
            VALUES (?, ?, ?)
        """, (definition_id, related_definition_id, relationship_type))
        
        conn.commit()
        conn.close()
