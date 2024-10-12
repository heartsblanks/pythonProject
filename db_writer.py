class DBWriterThread(threading.Thread):
    """Manages a single writer thread that processes database operations from the queue."""

    def __init__(self, db_queue, db_manager):
        super().__init__()
        self.db_queue = db_queue
        self.db_manager = db_manager

    def run(self):
        conn = sqlite3.connect(self.db_manager.db_path)
        conn.execute("PRAGMA foreign_keys = 1")
        while True:
            item = self.db_queue.get()
            if item is None:  # Exit signal
                break
            func, args, callback_event, result_container = item
            try:
                result = func(conn, *args)
                if result_container is not None:
                    result_container["result"] = result
                if callback_event is not None:
                    callback_event.set()
            except Exception as e:
                logging.error(f"Error in db_writer: {e}")
            self.db_queue.task_done()
        conn.close()