select_insert_pattern = re.compile(
    r'''
    # Match SELECT statements that start with SELECT keyword and contain FROM
    (?<![\w])\bSELECT\b\s+.*?\bFROM\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)    # Capture table name after FROM
    (?=\s|WHERE|;|\)|,|\()                                                # Stop at space, WHERE, ), ;, ,, or (

    |                                                                      # OR

    # Match INSERT statements that start with INSERT INTO
    (?<![\w])\bINSERT\s+INTO\s+([\w.\{\}\(\)\[\]\|\-\+\:\'\"]+)            # Capture table name after INSERT INTO
    (?=\s|\(|;|,)                                                          # Stop at space, (, ;, or ,
    ''', 
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)