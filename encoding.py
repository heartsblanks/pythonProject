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