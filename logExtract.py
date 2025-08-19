import re

def clean_bteq_log(log_content):
    """
    Clean BTEQ log by removing:
    1. Comment blocks between /* */
    2. Lines starting with --
    3. Lines starting with ***
    4. Lines starting with .xyz (BTEQ commands)
    """
    
    # Step 1: Remove comment blocks between /* */
    # This handles multi-line comments
    log_content = re.sub(r'/\*.*?\*/', '', log_content, flags=re.DOTALL)
    
    # Step 2: Split into lines for line-by-line processing
    lines = log_content.split('\n')
    
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
    ]
    ignore_regex = re.compile('|'.join(ignore_patterns), re.IGNORECASE)

    for line in lines:
        stripped_line = line.strip()
        
        # Skip empty lines
        if not stripped_line:
            continue
            
        # Step 3: Remove lines starting with --
        if stripped_line.startswith('--'):
            continue
            
        # Step 4: Remove lines starting with ***
        if stripped_line.startswith('***'):
            continue

        if ignore_regex.match(line.strip()):
            continue
            
        # Step 5: Remove lines starting with . (BTEQ commands)
        if stripped_line.startswith('.'):
            continue
        

        # Keep the line
        cleaned_lines.append(line)
    
    # Join back into single string
    return '\n'.join(cleaned_lines)

if __name__=="__main__":
    with open('test.log', 'r', encoding='utf-8') as f:
        sample_sql = f.read()
    
    cleaned_sql = clean_bteq_log(sample_sql)
    with open('query.txt', 'w', encoding='utf-8') as out_f:
        out_f.write(cleaned_sql)
    print("Cleaned SQL written to query.txt")