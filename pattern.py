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