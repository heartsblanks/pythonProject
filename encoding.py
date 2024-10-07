import chardet

def get_remote_file_encoding(ssh_executor, file_path):
    """Detect the file encoding on the remote server using `chardet`."""
    # Read the file as binary data to detect encoding
    command = f"cat {file_path}"
    raw_content = ssh_executor.execute_command(command).encode('latin1')
    
    # Detect encoding with chardet
    result = chardet.detect(raw_content)
    encoding = result['encoding']
    confidence = result['confidence']
    print(f"Detected encoding: {encoding} (Confidence: {confidence})")
    
    return encoding

def read_remote_file_with_detected_encoding(ssh_executor, file_path):
    """Read the content of a remote file using detected encoding."""
    # Detect the encoding of the file
    encoding = get_remote_file_encoding(ssh_executor, file_path)
    
    # Retrieve the file content as binary and decode with detected encoding
    command = f"cat {file_path}"
    raw_content = ssh_executor.execute_command(command).encode('latin1')
    content = raw_content.decode(encoding, errors='replace')
    
    return content
    
    
import base64

def get_remote_file_base64(ssh_executor, file_path):
    """Retrieve file content from the remote server as base64 to avoid encoding issues."""
    # Use base64 to safely transfer the file content
    command = f"base64 {file_path}"
    base64_content = ssh_executor.execute_command(command).strip()
    
    # Decode the base64 content to get the original binary data
    binary_content = base64.b64decode(base64_content)
    
    return binary_content

def read_remote_file_with_fallback(ssh_executor, file_path):
    """Read file content using base64 and decode with fallback options."""
    # Retrieve the binary content using base64 encoding
    binary_content = get_remote_file_base64(ssh_executor, file_path)
    
    # Try decoding with UTF-8 first, then Latin-1 with replacement as a fallback
    try:
        return binary_content.decode('utf-8')
    except UnicodeDecodeError:
        return binary_content.decode('latin1', errors='replace')
        
        
        
import time
import sqlite3

def retry_on_lock(func):
    """Retry a function if 'database is locked' error occurs."""
    def wrapper(*args, **kwargs):
        max_retries = 5
        delay = 0.1  # Delay in seconds between retries
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    if attempt < max_retries - 1:
                        time.sleep(delay)  # Wait before retrying
                    else:
                        raise  # Re-raise if maximum retries reached
                else:
                    raise  # Re-raise for any other OperationalError
    return wrapper