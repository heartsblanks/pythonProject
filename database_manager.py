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