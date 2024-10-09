select_insert_pattern = re.compile(
    r'''
    # Match SELECT statements
    \bSELECT\b.*?\bFROM\s+([\w\{\}\(\)\|\.\'\"]+.*?)    # Capture dynamic content up to .table name or space/terminator
    (?=\s|WHERE|;|\)|,|$)                               # Stop at space, WHERE, ), ;, ,, or end of line

    |                                                   # OR

    # Match INSERT statements
    \bINSERT\s+INTO\s+([\w\{\}\(\)\|\.\'\"]+.*?)        # Capture dynamic content up to .table name or space/terminator
    (?=\s|\(|;|,|$)                                     # Stop at space, (, ;, ,, or end of line
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)