import re

def clean_bteq_log(log_content):
    """
    Clean BTEQ log by removing:
    1. Comment blocks between /* */
    2. Lines starting with --
    3. Lines starting with ***
    4. Lines starting with .xyz (BTEQ commands)
    5. Lines starting with EXEC, COLLECT STATISTICS, or any number.
    """
    
    # Step 1: Remove comment blocks between /* */
    # This handles multi-line comments
    log_content = re.sub(r'/\*.*?\*/', '', log_content, flags=re.DOTALL)
    
    # Step 2: Split into lines for line-by-line processing
    lines = log_content.split('\n')
    in_sql_query = False
    sql_start_keywords = ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
                            'ALTER', 'WITH', 'REPLACE', 'COLLECT', 'CALL','SEL','DEL')
    cleaned_lines = []
    
    ignore_patterns = [
        r'^\+\-+',  # Separator lines
        r'^BTEQ\s+\d+',  # BTEQ version info
        r'^\s*\d+\s+rows?\s+(found|added|changed|removed)',  # Result messages
        r'^\s*Total elapsed time',  # Timing info
        r'^\s*Current TimeStamp',  # Timestamp results
        r'^\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',  # Date results
        r'^\s*Return Code',  # Return codes
        r'^\s*PID:',  # Process IDs
        r'^\s*\d{4}-\d{2}-\d{2}-\d{6}::', # Date-time prefixes
        r'^\s*grep:',  # Grep messages
        r'^\s*Executing',  # Execution messages
        r'^\s*\|\s*$',  # Empty table borders
        r'^\s*EXEC', # Lines starting with EXEC
        r'^\s*COLLECT STATISTICS', # Lines starting with COLLECT STATISTICS
        r'^\s*\d+', # Lines starting with any number
        r'^\s*BT', # Lines starting with EXEC
        r'^\s*ET', # Lines starting with COLLECT STATISTICS
    ]
    ignore_regex = re.compile('|'.join(ignore_patterns), re.IGNORECASE)

    for line in lines:
        stripped_line = line.strip()
        
        # Step 3: Remove lines starting with --
        if not stripped_line or stripped_line.startswith('--') or stripped_line.startswith('***') or stripped_line.startswith('.'):
            continue


        if in_sql_query:
            # --- We are INSIDE a query ---
            # Append the line as part of the ongoing statement.
            cleaned_lines.append(line)
            # Check if this line ends the statement.
            if stripped_line.endswith(';'):
                in_sql_query = False # Exit the query state.
                # Add a blank line for separation between queries
                cleaned_lines.append('\n') 
        else:
            # --- We are OUTSIDE a query ---
            # Ignore empty lines and known log noise.
            if not stripped_line or ignore_regex.match(stripped_line):
                continue

        if stripped_line.upper().startswith(sql_start_keywords) and not in_sql_query:
            in_sql_query = True # Enter the query state.
            cleaned_lines.append(line)
                # Handle single-line queries (e.g., "SELECT DATE;")
            if stripped_line.endswith(';'):
                in_sql_query = False
                cleaned_lines.append('\n')
    
    # Join back into single string
    return '\n'.join(cleaned_lines)

if __name__=="__main__":
    try:
        with open('test2.log', 'r', encoding='utf-8') as f:
            sample_sql = f.read()
        
        cleaned_sql = clean_bteq_log(sample_sql)
        with open('query2.txt', 'w', encoding='utf-8') as out_f:
            out_f.write(cleaned_sql)
        print("Cleaned SQL written to query.txt")
    except FileNotFoundError:
        print("Error: 'test2.log' not found. Please create this file with your BTEQ log content.")