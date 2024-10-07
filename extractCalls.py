import os
import re

def get_esql_definitions_and_calls(directory_path):
    # Pattern to identify module definitions
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    # Pattern to identify function and procedure definitions
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    # Pattern to find the next CREATE, END MODULE, or EOF
    next_block_pattern = re.compile(r'\b(?:CREATE|END\s+MODULE)\b', re.IGNORECASE)
    # Pattern to identify function/procedure calls within a block
    call_pattern = re.compile(r'\b(\w+)\s*\(')
    
    # Set of names to exclude
    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage"}

    # Dictionary to store module definitions and calls in each file
    esql_data = {}

    # Traverse the directory for .esql files
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.esql'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    content = f.read()
                    esql_data[file] = {}

                    # Check if there are modules in the file
                    module_matches = list(module_pattern.finditer(content))
                    if module_matches:
                        # Process each module
                        for module_match in module_matches:
                            module_name = module_match.group(1)
                            esql_data[file][module_name] = {}
                            
                            # Determine start and end of the module
                            module_start = module_match.end()
                            next_block = next_block_pattern.search(content, module_start)
                            module_end = next_block.start() if next_block and "END MODULE" in next_block.group(0) else len(content)
                            module_content = content[module_start:module_end]

                            # Find all function/procedure definitions within the module
                            for func_match in definition_pattern.finditer(module_content):
                                func_name = func_match.group(1)
                                if func_name not in excluded_procedures:
                                    # Get the body of the function/procedure
                                    func_start = func_match.end()
                                    next_create = next_block_pattern.search(module_content, func_start)
                                    func_end = next_create.start() if next_create else len(module_content)
                                    func_body = module_content[func_start:func_end]

                                    # Find all function/procedure calls within the body
                                    calls = set(call_pattern.findall(func_body))
                                    calls = [call for call in calls if call != func_name and call not in excluded_procedures]
                                    esql_data[file][module_name][func_name] = calls
                    else:
                        # Process standalone functions/procedures if no modules are present
                        esql_data[file]["Standalone"] = {}
                        for match in definition_pattern.finditer(content):
                            func_name = match.group(1)
                            if func_name not in excluded_procedures:
                                # Extract function/procedure body
                                start_pos = match.end()
                                next_match = next_block_pattern.search(content, start_pos)
                                end_pos = next_match.start() if next_match else len(content)
                                body_content = content[start_pos:end_pos]

                                # Identify calls within the body
                                calls = set(call_pattern.findall(body_content))
                                calls = [call for call in calls if call != func_name and call not in excluded_procedures]
                                esql_data[file]["Standalone"][func_name] = calls
    
    return esql_data

# Example usage
directory_path = '/path/to/your/project'
esql_data = get_esql_definitions_and_calls(directory_path)
for file, modules in esql_data.items():
    print(f"{file}:")
    for module, funcs in modules.items():
        print(f"  Module: {module}")
        for func, calls in funcs.items():
            print(f"    {func} calls: {calls}")