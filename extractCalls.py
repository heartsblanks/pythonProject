import os
import re

def get_esql_definitions_and_calls(directory_path):
    # Pattern to identify function and procedure definitions
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    # Pattern to identify the next CREATE or EOF
    next_definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\b', re.IGNORECASE)
    # Pattern to identify function/procedure calls within a block
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
                    esql_data[file] = {}
                    
                    # Find all function/procedure definitions
                    for match in definition_pattern.finditer(content):
                        func_name = match.group(1)
                        if func_name not in excluded_procedures:
                            # Determine start and end of the function/procedure body
                            start_pos = match.end()
                            next_match = next_definition_pattern.search(content, start_pos)
                            end_pos = next_match.start() if next_match else len(content)
                            body_content = content[start_pos:end_pos]
                            
                            # Find all function/procedure calls within the body
                            calls = set(call_pattern.findall(body_content))
                            # Exclude self-calls and known exclusions
                            calls = [call for call in calls if call != func_name and call not in excluded_procedures]
                            esql_data[file][func_name] = calls
    
    return esql_data

# Example usage
directory_path = '/path/to/your/project'
esql_data = get_esql_definitions_and_calls(directory_path)
for file, funcs in esql_data.items():
    print(f"{file}:")
    for func, calls in funcs.items():
        print(f"  {func} calls: {calls}")