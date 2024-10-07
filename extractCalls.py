import os
import re

def get_esql_definitions_and_calls(directory_path):
    # Pattern to identify module definitions
    module_pattern = re.compile(r'\bCREATE\s+.*?\bMODULE\s+(\w+)\b', re.IGNORECASE)
    # Pattern to identify function and procedure definitions within a module
    definition_pattern = re.compile(r'\bCREATE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)\s*\(.*?\)', re.IGNORECASE)
    # Pattern to identify the next CREATE or END MODULE or EOF within the module
    next_block_pattern = re.compile(r'\b(?:CREATE\s+(?:FUNCTION|PROCEDURE)|END\s+MODULE)\b', re.IGNORECASE)
    # Pattern to identify function/procedure calls within a block
    call_pattern = re.compile(r'\b(\w+)\s*\(')

    # Pattern to detect SQL operations (select, update, insert, delete)
    sql_pattern = re.compile(r'\b(PASSTHRU\s*\(\s*)?(SELECT|UPDATE|INSERT|DELETE)\b', re.IGNORECASE)
    # Pattern to exclude message tree SQL operations and non-database FROM statements
    message_tree_pattern = re.compile(r'\bFROM\s+\w+\s*\[.*?\]', re.IGNORECASE)
    database_sql_pattern = re.compile(r'\bFROM\s+\w+\.\w+', re.IGNORECASE)  # Matches `schema.table` format

    # Set of names to exclude
    excluded_procedures = {"CopyMessageHeaders", "CopyEntireMessage", "CARDINALITY", "COALESCE"}

    # Dictionary to store module definitions, calls, and SQL statements in each file
    esql_data = {}

    # Traverse the directory for .esql files
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.esql'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    content = f.read()
                    esql_data[file] = {}
                    
                    # Track module ranges to exclude their functions from standalone
                    module_ranges = []

                    # Process modules in the file
                    for module_match in module_pattern.finditer(content):
                        module_name = module_match.group(1)
                        
                        # Determine start and end of the module
                        module_start = module_match.end()
                        end_module_match = re.search(r'\bEND\s+MODULE\b', content[module_start:], re.IGNORECASE)
                        module_end = module_start + end_module_match.start() if end_module_match else len(content)
                        module_content = content[module_start:module_end]
                        module_ranges.append((module_start, module_end))

                        # Initialize dictionary for the current module
                        esql_data[file][module_name] = {"functions": {}, "sql_statements": []}

                        # Find all function/procedure definitions within the module
                        for func_match in definition_pattern.finditer(module_content):
                            func_name = func_match.group(1)
                            if func_name not in excluded_procedures:
                                # Get the start position of the function/procedure body
                                func_start = func_match.end()
                                # Check if 'BEGIN' is in the body
                                begin_match = re.search(r'\bBEGIN\b', module_content[func_start:], re.IGNORECASE)

                                if begin_match:
                                    # If 'BEGIN' exists, find the matching 'END;' with optional whitespace
                                    begin_pos = func_start + begin_match.start()
                                    end_match = re.search(r'\bEND\s*;\b', module_content[begin_pos:], re.IGNORECASE)
                                    func_end = begin_pos + end_match.end() if end_match else len(content)
                                else:
                                    # If no 'BEGIN', find the next 'CREATE FUNCTION/PROCEDURE' or 'END MODULE'
                                    next_create = next_block_pattern.search(module_content, func_start)
                                    func_end = next_create.start() if next_create else len(content)

                                # Extract the function/procedure body
                                func_body = module_content[func_start:func_end]

                                # Find all function/procedure calls within the body
                                calls = set(call_pattern.findall(func_body))
                                unique_calls = [call for call in calls if call != func_name and call not in excluded_procedures]
                                esql_data[file][module_name]["functions"][func_name] = {"calls": unique_calls, "sql_statements": []}

                                # Detect SQL statements within the function/procedure body
                                for sql_match in sql_pattern.finditer(func_body):
                                    sql_start = sql_match.start()
                                    # Find end of statement (up to semicolon)
                                    sql_end = func_body.find(';', sql_start) + 1
                                    sql_statement = func_body[sql_start:sql_end].strip()

                                    # Exclude message tree SQL statements and non-database SQL
                                    if not message_tree_pattern.search(sql_statement) and database_sql_pattern.search(sql_statement):
                                        esql_data[file][module_name]["functions"][func_name]["sql_statements"].append(sql_statement)
                    
                    # Process standalone functions/procedures outside module ranges
                    if "Standalone" not in esql_data[file]:
                        esql_data[file]["Standalone"] = {"functions": {}, "sql_statements": []}
                    for match in definition_pattern.finditer(content):
                        func_name = match.group(1)
                        func_start = match.start()
                        if func_name not in excluded_procedures and not any(start <= func_start < end for start, end in module_ranges):
                            # Start of the function/procedure body
                            start_pos = match.end()
                            # Check for 'BEGIN'
                            begin_match = re.search(r'\bBEGIN\b', content[start_pos:], re.IGNORECASE)

                            if begin_match:
                                # Find matching 'END;' if 'BEGIN' exists, fallback to next block or EOF
                                begin_pos = start_pos + begin_match.start()
                                end_match = re.search(r'\bEND\s*;\b', content[begin_pos:], re.IGNORECASE)
                                if end_match:
                                    end_pos = begin_pos + end_match.end()
                                else:
                                    next_match = next_block_pattern.search(content, start_pos)
                                    end_pos = next_match.start() if next_match else len(content)
                            else:
                                # No 'BEGIN', find next 'CREATE FUNCTION/PROCEDURE' or EOF
                                next_match = next_block_pattern.search(content, start_pos)
                                end_pos = next_match.start() if next_match else len(content)

                            # Extract the function/procedure body
                            body_content = content[start_pos:end_pos]

                            # Identify unique calls within the body
                            calls = set(call_pattern.findall(body_content))
                            unique_calls = [call for call in calls if call != func_name and call not in excluded_procedures]
                            esql_data[file]["Standalone"]["functions"][func_name] = {"calls": unique_calls, "sql_statements": []}

                            # Detect SQL statements
                            for sql_match in sql_pattern.finditer(body_content):
                                sql_start = sql_match.start()
                                # Find end of statement (up to semicolon)
                                sql_end = body_content.find(';', sql_start) + 1
                                sql_statement = body_content[sql_start:sql_end].strip()

                                # Exclude message tree SQL statements and non-database SQL
                                if not message_tree_pattern.search(sql_statement) and database_sql_pattern.search(sql_statement):
                                    esql_data[file]["Standalone"]["functions"][func_name]["sql_statements"].append(sql_statement)
    
    return esql_data

# Example usage
directory_path = '/path/to/your/project'
esql_data = get_esql_definitions_and_calls(directory_path)
for file, modules in esql_data.items():
    print(f"{file}:")
    for module, data in modules.items():
        print(f"  Module: {module}")
        for func, func_data in data["functions"].items():
            print(f"    Function/Procedure: {func}")
            print(f"      Calls: {func_data['calls']}")
            print(f"      SQL Statements: {func_data['sql_statements']}")
        print(f"  SQL Statements outside functions: {data['sql_statements']}")