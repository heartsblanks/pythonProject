select_insert_pattern = re.compile(
    r'''
    # Match SELECT statements and capture the content after FROM
    \bSELECT\b.*?\bFROM\s+([^\s;]+)                # Capture everything after FROM until whitespace or ;

    |                                              # OR

    # Match INSERT statements and capture the content after INTO
    \bINSERT\s+INTO\s+([^\s;]+)                    # Capture everything after INTO until whitespace or ;
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)