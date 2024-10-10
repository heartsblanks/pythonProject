select_insert_pattern = re.compile(
    r'''
    # Capture SELECT statements with FROM
    \bSELECT\b.*?\bFROM\s+([^\s;,()]+)  # Capture everything after FROM until whitespace, ;, ), or ,

    |                                   # OR

    # Capture INSERT statements with INTO
    \bINSERT\s+INTO\s+([^\s;,()]+)      # Capture everything after INSERT INTO until whitespace, ;, ), or ,
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)

import re

# Revised regex to capture complex structures, ensuring it includes full expressions after FROM
sql_pattern = r"\b(SELECT|INSERT|UPDATE|DELETE)\b.*?\bFROM\b\s+((?:[^\s]+?))(?:\s+AS\s+\w+)?(.*?)(?=\b(WHERE|GROUP BY|ORDER BY|LIMIT|JOIN|;|$))"

# Sample file content with complex expressions in the table name
file_content = """
SELECT column1, column2 
FROM Database.{get() || 'HT'}.table AS tbl_alias
WHERE condition = 'some value';
Another example:
SELECT * 
FROM SchemaName.tableName another_alias
ORDER BY column3;
"""

# Extracting SQL operation type and complex table name or expressions after FROM
matches = re.findall(sql_pattern, file_content, re.IGNORECASE | re.DOTALL)

# Print extracted information
for match in matches:
    operation_type = match[0]
    table_expression = match[1].strip()
    content_after_from = match[2].strip()
    print(f"Operation Type: {operation_type}")
    print(f"Table Expression: {table_expression}")
    print(f"Content After FROM: {content_after_from}\n")