import pandas as pd
from datetime import datetime
import threading

class ExcelToDatabaseProcessor:
    """Processes data from an Excel file and queues it for insertion or update in the database."""

    def __init__(self, db_queue, db_manager, table_name, file_path, column_mapping):
        self.db_queue = db_queue
        self.db_manager = db_manager
        self.table_name = table_name
        self.file_path = file_path
        self.column_mapping = column_mapping
        self.primary_key_columns = self.get_primary_key_columns()

    def get_primary_key_columns(self):
        """Get primary key columns from the table schema."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((self.db_manager.get_primary_key_columns, (self.table_name,), callback_event, result_container))
        callback_event.wait()
        return result_container["result"]

    def process_excel_file(self):
        """Processes the Excel file and queues upsert operations for each row."""
        df = pd.read_excel(self.file_path, sheet_name='export')

        columns = ', '.join(self.column_mapping.keys())
        placeholders = ', '.join(['?'] * len(self.column_mapping))
        update_columns = ', '.join([f"{key} = ?" for key in self.column_mapping.keys() if key != 'UPDATED_TIMESTAMP'])
        conflict_columns = ', '.join(self.primary_key_columns)

        where_clause = ' OR '.join([f"{key} != excluded.{key}" for key in self.column_mapping.keys() if key != 'UPDATED_TIMESTAMP'])
        
        base_insert_query = f"""
        INSERT INTO {self.table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT({conflict_columns})
        DO UPDATE SET {update_columns}, UPDATED_TIMESTAMP = ?
        WHERE {where_clause};
        """

        for index, row in df.iterrows():
            properties_dict = {
                table_col: row[excel_col].strip() if isinstance(row[excel_col], str) else row[excel_col]
                for table_col, excel_col in self.column_mapping.items() if excel_col != "Current Timestamp"
            }

            if all(properties_dict[key] is None or properties_dict[key] == '' for key in self.primary_key_columns):
                print(f"Skipping row {index + 2}: Primary key columns are empty or null.")
                continue

            properties_dict["UPDATED_TIMESTAMP"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            query_values = tuple(properties_dict.values()) * 2

            # Queue the database operation
            self.db_queue.put((self.db_manager.upsert_data, (base_insert_query, query_values), None, {}))