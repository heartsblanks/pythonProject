import sqlglot

# Complex SQL statement with nested query and joins
sql = """
SELECT a.column1, b.column2 
FROM (SELECT column1 FROM database1.table1) AS a 
JOIN database2.table2 AS b ON a.column1 = b.column1 
WHERE b.column2 IN (SELECT column2 FROM database3.table3 WHERE condition = 'value');
"""

# Parse the SQL statement
parsed = sqlglot.parse_one(sql)

# Extract the operation type
operation_type = parsed.find("select")  # For other types, use "insert", "update", etc.
if operation_type:
    operation_type = "SELECT"  # We assume SELECT as an example here

# Function to extract table names from joins and subqueries
def extract_table_names(parsed_node):
    tables = []
    # Find all FROM and JOIN clauses, which hold the tables
    for node in parsed_node.find_all("from") + parsed_node.find_all("join"):
        if node.args.get("this"):
            tables.append(node.args["this"].sql())
    return tables

# Extract table names from the main query and subqueries
table_names = extract_table_names(parsed)

print("Operation Type:", operation_type)
print("Table Names:", table_names)