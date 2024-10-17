import csv
import threading
import queue

class DatabaseProcessor:
    def __init__(self, db_queue, db_manager, delimiter=';'):
        """
        Initialize DatabaseProcessor with a database queue, DatabaseManager, and CSV delimiter.
        
        Parameters:
        - db_queue (queue.Queue): The queue for managing database operations.
        - db_manager (DatabaseManager): An instance of DatabaseManager.
        - delimiter (str): The delimiter used in the CSV file.
        """
        self.db_queue = db_queue
        self.db_manager = db_manager
        self.delimiter = delimiter

    def clean_csv(self, input_file, output_file):
        """Cleans a CSV file by handling rows with too few or too many columns."""
        # Read the header row to get the column names
        with open(input_file, 'r') as infile:
            reader = csv.reader(infile, delimiter=self.delimiter)
            header = next(reader)  # Read the header row
            num_columns = len(header)

        # Process rows and write them to a cleaned output file
        with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
            reader = csv.reader(infile, delimiter=self.delimiter)
            writer = csv.writer(outfile, delimiter=self.delimiter)
            writer.writerow(header)  # Write header to output file

            for row in reader:
                if len(row) == num_columns:
                    writer.writerow(row)  # Correct row
                elif len(row) > num_columns:
                    # Too many columns, merge extra columns
                    corrected_row = row[:num_columns-1] + [self.delimiter.join(row[num_columns-1:])]
                    writer.writerow(corrected_row)
                elif len(row) < num_columns:
                    # Too few columns, pad with empty strings
                    row += [''] * (num_columns - len(row))
                    writer.writerow(row)

        return header  # Return the cleaned column names

    def load_data_into_table(self, table_name, cleaned_file, columns):
        """Queues data loading from the cleaned CSV file into the specified table."""
        with open(cleaned_file, 'r') as file:
            reader = csv.reader(file, delimiter=self.delimiter)
            next(reader)  # Skip header
            for row in reader:
                # Queue each row for insertion
                self._queue_insert_data(table_name, columns, row)

    def _queue_insert_data(self, table_name, columns, row):
        """Queues a database insert operation."""
        callback_event = threading.Event()
        result_container = {}
        self.db_queue.put((
            self.db_manager.insert_data,
            (table_name, columns, row),
            callback_event,
            result_container
        ))
        callback_event.wait()

# Example usage in main
def main():
    db_queue = queue.Queue()
    db_manager = DatabaseManager()  # Assume DatabaseManager has insert_data method
    db_processor = DatabaseProcessor(db_queue, db_manager)

    # Clean the CSV
    input_file = 'input.csv'
    cleaned_file = 'output_cleaned.csv'
    columns = db_processor.clean_csv(input_file, cleaned_file)

    # Load data into database table
    table_name = 'your_table_name'
    db_processor.load_data_into_table(table_name, cleaned_file, columns)

    # Signal end of processing
    db_queue.put(None)

# To run
if __name__ == "__main__":
    main()