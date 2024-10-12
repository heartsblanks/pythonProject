import logging
import time

class RemoteFileHandler:
    def __init__(self, ssh_executor):
        self.ssh_executor = ssh_executor

    def find_files(self, folder, file_type):
        """Find specific file types (.esql or .msgflow) in the specified folder on the remote server."""
        if file_type == 'esql':
            command = f"find {folder} -type f -name '*.esql' -not -path '*/Attic/*'"
        elif file_type == 'msgflow':
            command = f"find {folder} -type f -name '*.msgflow' -not -path '*/Attic/*'"
        else:
            raise ValueError("Invalid file type specified. Use 'esql' or 'msgflow'.")
        
        logging.info(f"Executing command to find {file_type} files: {command}")
        files = self.ssh_executor.execute_command(command).strip().splitlines()
        logging.info(f"Found {len(files)} {file_type} files in {folder}")
        return files

    def get_file_content_base64(self, cvsroot, file_path, retries=3, delay=1):
        """Retrieve the latest version of a versioned file from CVS as base64 encoded content with retries."""
        file_path = file_path.rstrip(',v')  # Remove the ',v' suffix from file_path
        command = f"CVSROOT={cvsroot} cvs checkout -p {file_path} | base64"
        
        for attempt in range(1, retries + 1):
            result = self.ssh_executor.execute_command(command).strip()
            
            if result:
                logging.info(f"Successfully retrieved base64 content for {file_path} on attempt {attempt}")
                return result
            
            logging.warning(f"Attempt {attempt} to retrieve {file_path} failed. Retrying in {delay} second(s)...")
            time.sleep(delay)
        
        logging.error(f"Failed to retrieve base64 content for {file_path} after {retries} attempts")
        return None