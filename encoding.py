import threading
import queue
import sqlite3

# Primary Queue for DB operations
db_queue = queue.Queue()

def db_writer():
    """Single-threaded function to write database operations from the queue."""
    conn = sqlite3.connect("esql_analysis.db")  # Create a connection within the writer thread
    conn.execute("PRAGMA foreign_keys = 1")  # Enable foreign keys
    while True:
        item = db_queue.get()
        if item is None:  # Exit signal
            break
        func, args, callback_event, result_container = item
        try:
            # Perform the database operation and store the result
            result = func(conn, *args)
            result_container["result"] = result  # Store result in the container
            callback_event.set()  # Signal that the result is ready
        except Exception as e:
            print(f"Error in db_writer: {e}")
        db_queue.task_done()
    conn.close()

def insert_function(file_name, function_name, folder_name, module_id=None):
    """Queue the function insertion and wait for the result."""
    def _insert_function(conn, file_name, function_name, folder_name, module_id=None):
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO functions (file_name, function_name, folder_name, module_id)
            VALUES (?, ?, ?, ?)
        ''', (file_name, function_name, folder_name, module_id))
        conn.commit()
        return cursor.lastrowid if cursor.lastrowid else cursor.execute(
            "SELECT function_id FROM functions WHERE file_name = ? AND function_name = ? AND folder_name = ? AND module_id IS ?", 
            (file_name, function_name, folder_name, module_id)).fetchone()[0]

    # Set up the callback mechanism
    callback_event = threading.Event()  # Event to signal when result is ready
    result_container = {}  # Container to hold the result (since we can't directly return from db_writer)
    db_queue.put((_insert_function, (file_name, function_name, folder_name, module_id), callback_event, result_container))
    
    callback_event.wait()  # Wait for the result to be available
    return result_container["result"]  # Return the result after callback_event is set

# Start the db_writer thread
writer_thread = threading.Thread(target=db_writer)
writer_thread.start()

# Usage example
function_id = insert_function("file1.esql", "function1", "folder1")
print(f"Inserted function with ID: {function_id}")

# Stop the writer thread
db_queue.put(None)
writer_thread.join()