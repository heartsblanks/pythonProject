select_insert_pattern = re.compile(
    r'''
    # Capture SELECT statements
    \bSELECT\b.*?\bFROM\s+([\w\{\}\(\)\|\.\'\"]+\.[A-Za-z0-9_]+)   # Capture full database.table name
    (?=\s|WHERE|;|\)|,|\()                                         # Stop at space, WHERE, ), ;, ,, or (

    |                                                              # OR

    # Capture INSERT statements
    \bINSERT\s+INTO\s+([\w\{\}\(\)\|\.\'\"]+\.[A-Za-z0-9_]+)       # Capture full database.table name
    (?=\s|\(|;|,)                                                  # Stop at space, (, ;, or ,
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)