select_insert_pattern = re.compile(
    r'''
    (?<!\b[Ff]ailed\s+to\s)             # Negative lookbehind to ignore "Failed to " before the operation

    (
        # Capture SELECT statements
        \bSELECT\b.*?\bFROM\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)    # Capture table name after FROM
        (?=\s|WHERE|;|\)|,|\()                                    # Stop at space, WHERE, ), ;, ,, or (

        |                                                          # OR

        # Capture INSERT statements
        \bINSERT\s+INTO\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)         # Capture table name after INSERT INTO
        (?=\s|\(|;|,)                                              # Stop at space, (, ;, or ,
    )
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)