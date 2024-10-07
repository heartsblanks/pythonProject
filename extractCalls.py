import os
import re

def get_esql_definitions_and_calls(directory_path):
    # Pattern to identify module definitions
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    # Pattern to identify function and procedure definitions within a module
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    # Pattern to identify the next CREATE or END MODULE or EOF within the module
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

                    # Find all modules in the file
                    for module_match in module_pattern.finditer(content):
                        module_name = module_match.group(1)
                        
                        # Determine start and end of the module
                        module_start = module_match.end()
                        end_module_match = re.search(r'\bEND\s+MODULE\b', content[module_start:], re.IGNORECASE)
                        module_end = module_start + end_module_match.start() if end_module_match else len(content)
                        module_content = content[module_start:module_end]

                        # Initialize dictionary for the current module
                        esql_data[file][module_name] = {}

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
                    
                    # If no modules are present, process standalone functions/procedures
                    if not esql_data[file]:
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