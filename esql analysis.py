import os
import concurrent.futures
from ssh_executor import SSHExecutor  # Assuming SSHExecutor class is in ssh_executor.py
import sqlite3

# Adjust the `get_esql_definitions_and_calls` function to work with a remote file content string
def get_esql_definitions_and_calls(file_content, conn, file_name):
    # Add your existing esql analysis logic here, working with `file_content`
    # and inserting analysis results into `conn` database
    pass

def analyze_folder(ssh_executor, folder, conn):
    """Analyze all .esql files in a given folder on a remote server."""
    # Retrieve list of .esql files in the remote folder
    command = f"find {folder} -type f -name '*.esql'"
    esql_files = ssh_executor.execute_command(command).strip().splitlines()

    # Process each .esql file in the folder
    for esql_file in esql_files:
        # Retrieve the file content
        command = f"cat {esql_file}"
        file_content = ssh_executor.execute_command(command)
        
        # Analyze the content and insert results into SQLite3 database
        get_esql_definitions_and_calls(file_content, conn, esql_file)

def main():
    # Database connection
    conn = sqlite3.connect("esql_analysis.db")
    create_database()  # Ensure the database structure is created

    # SSH connection details
    hostname = "remote_server_hostname"
    private_key_path = "/path/to/private/key"
    username = "remote_user"

    # Establish SSH connection and list folders
    with SSHExecutor(hostname, private_key_path, username=username) as ssh_executor:
        # Retrieve all folders under the main directory
        main_directory = "/path/to/main/directory"
        folders = ssh_executor.execute_command(f"find {main_directory} -mindepth 1 -type d").strip().splitlines()

        # Process each folder concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(analyze_folder, ssh_executor, folder, conn)
                for folder in folders
            ]
            # Wait for all futures to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing folder: {e}")

    conn.close()

if __name__ == "__main__":
    main()