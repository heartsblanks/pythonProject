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

# Updated regex to capture table names or subqueries after FROM
sql_pattern = r"\b(SELECT|INSERT|UPDATE|DELETE)\b.*?\bFROM\b\s+((?:\w+|\((?:[^()]*|\([^()]*\))*\)))(.*?)(?=\b(WHERE|GROUP BY|ORDER BY|LIMIT|JOIN|;|$))"

# Sample file content with a subquery
file_content = """
SELECT column1 
FROM (SELECT column2 FROM another_table WHERE condition = 'some value') AS subquery
WHERE column1 > 10;
"""

# Extracting SQL operation type and content after FROM
matches = re.findall(sql_pattern, file_content, re.IGNORECASE | re.DOTALL)

# Print extracted information
for match in matches:
    operation_type = match[0]
    table_or_subquery = match[1].strip()
    content_after_from = match[2].strip()
    print(f"Operation Type: {operation_type}")
    print(f"Table or Subquery: {table_or_subquery}")
    print(f"Content After FROM: {content_after_from}\n")
