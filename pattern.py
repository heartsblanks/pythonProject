select_insert_pattern = re.compile(
    r'''
    # Capture SELECT statements
    \bSELECT\b.*?\bFROM\s+([A-Za-z0-9_\{\}\(\)\|\.\'\"]+?(\.[A-Za-z0-9_]+))  # Capture complex structure up to .table name
    (?=\s|WHERE|;|\)|,|$)                                                     # Stop at space, WHERE, ), ;, ,, or end of line

    |                                                                         # OR

    # Capture INSERT statements
    \bINSERT\s+INTO\s+([A-Za-z0-9_\{\}\(\)\|\.\'\"]+?(\.[A-Za-z0-9_]+))       # Capture complex structure up to .table name
    (?=\s|\(|;|,|$)                                                           # Stop at space, (, ;, ,, or end of line
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)