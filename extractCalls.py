import os
import re

def get_esql_definitions_and_calls(directory_path):
    # Pattern to identify function and procedure definitions
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\bBEGIN\b(.*?)\bEND\b', re.DOTALL | re.IGNORECASE)
    # Pattern to identify function/procedure calls within BEGIN-END blocks
    call_pattern = re.compile(r'\b(\w+)\s*\(')
    
    # Set of names to exclude
    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage"}

    # Dictionary to store definitions and calls in each file
    esql_data = {}

    # Traverse the directory for .esql files
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.esql'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    content = f.read()
                    # Find all function/procedure definitions
                    definitions = {}
                    for match in definition_pattern.finditer(content):
                        func_name = match.group(1)
                        if func_name not in excluded_procedures:
                            block_content = match.group(2)
                            # Find all function/procedure calls within the block
                            calls = set(call_pattern.findall(block_content))
                            # Exclude self-calls and known exclusions
                            calls = [call for call in calls if call != func_name and call not in excluded_procedures]
                            definitions[func_name] = calls
                    esql_data[file] = definitions
    
    return esql_data

# Example usage
directory_path = '/path/to/your/project'
esql_data = get_esql_definitions_and_calls(directory_path)
for file, funcs in esql_data.items():
    print(f"{file}:")
    for func, calls in funcs.items():
        print(f"  {func} calls: {calls}")