select_insert_pattern = re.compile(
    r'''
    # Match SELECT statements
    \bSELECT\b.*?\bFROM\s+([A-Za-z0-9_\{\}\(\)\|\.\'\"]+\.[A-Za-z0-9_]+)   # Capture complex database.table pattern
    (?=\s|WHERE|;|\)|,|\()                                                  # Stop at space, WHERE, ), ;, ,, or (

    |                                                                       # OR

    # Match INSERT statements
    \bINSERT\s+INTO\s+([A-Za-z0-9_\{\}\(\)\|\.\'\"]+\.[A-Za-z0-9_]+)        # Capture complex database.table pattern
    (?=\s|\(|;|,)                                                           # Stop at space, (, ;, or ,
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)