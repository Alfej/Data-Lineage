import logging
import sqlglot
from sqlglot import exp, parse_one
import pandas as pd
from sqllineage.runner import LineageRunner
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_queries_from_txt(file_path):
    """Read SQL queries from a text file."""
    with open(file_path, 'r') as file:
        return file.read()


def read_queries_from_excel(file_path):
    """Read SQL queries from an Excel file. 
    Assumes the Excel file has a column named 'query' or 'sql' containing SQL queries."""
    try:
        df = pd.read_excel(file_path)
        # Try common column names for SQL queries
        query_column = None
        for col in ['query', 'Query', 'QUERY', 'sql', 'SQL', 'Sql']:
            if col in df.columns:
                query_column = col
                break
        
        if query_column is None:
            # If no standard column found, use the first column
            query_column = df.columns[0]
            logger.warning(f"No 'query' or 'sql' column found in {file_path}. Using first column: {query_column}")
        
        # Combine all queries from the column
        queries = df[query_column].dropna().astype(str).tolist()
        return ';'.join(queries)
    except Exception as e:
        logger.error(f"Error reading Excel file {file_path}: {e}")
        return ""


def get_query_files_from_folder(folder_path):
    """Get all query files (txt and Excel) from a folder."""
    folder = Path(folder_path)
    query_files = []
    
    if not folder.exists() or not folder.is_dir():
        logger.error(f"Invalid folder path: {folder_path}")
        return query_files
    
    # Search for .txt and Excel files
    for file in folder.rglob('*'):
        if file.is_file():
            if file.suffix.lower() in ['.txt']:
                query_files.append(('txt', str(file)))
            elif file.suffix.lower() in ['.xlsx', '.xls', '.xlsm']:
                query_files.append(('excel', str(file)))
    
    return query_files


def read_queries_from_input(input_path):
    """Read queries from a file or folder.
    Supports: .txt files, Excel files (.xlsx, .xls), and folders containing these files."""
    path = Path(input_path)
    all_queries = []
    
    if not path.exists():
        logger.error(f"Path does not exist: {input_path}")
        return ""
    
    if path.is_file():
        # Single file processing
        if path.suffix.lower() in ['.txt']:
            logger.info(f"Reading queries from text file: {input_path}")
            return read_queries_from_txt(input_path)
        elif path.suffix.lower() in ['.xlsx', '.xls', '.xlsm']:
            logger.info(f"Reading queries from Excel file: {input_path}")
            return read_queries_from_excel(input_path)
        else:
            logger.warning(f"Unsupported file type: {path.suffix}")
            return ""
    
    elif path.is_dir():
        # Folder processing
        logger.info(f"Scanning folder for query files: {input_path}")
        query_files = get_query_files_from_folder(input_path)
        
        if not query_files:
            logger.warning(f"No query files found in folder: {input_path}")
            return ""
        
        for file_type, file_path in query_files:
            logger.info(f"Processing {file_type} file: {file_path}")
            if file_type == 'txt':
                all_queries.append(read_queries_from_txt(file_path))
            elif file_type == 'excel':
                all_queries.append(read_queries_from_excel(file_path))
        
        return ';'.join(all_queries)
    
    return ""


def process_queries(sql_content, custom_name):
    """Process SQL queries and generate lineage data."""
    dataframe_rows = []
    
    for one_sql in sql_content.split(';'):
        if len(one_sql.strip()) == 0:
            continue
        try:
            parsed_sql = parse_one(one_sql, dialect="teradata")
            sql_type = type(parsed_sql).__name__

            if sql_type == 'Delete':
                tables_involved = [table.name for table in parsed_sql.find_all(exp.Table)]
                if tables_involved:
                    child_table = tables_involved[0]
                    parent_tables = tables_involved[1:]
                    # Directly create a row for each parent-child relationship
                    for parent in parent_tables:
                        dataframe_rows.append({
                            "childTableName": child_table,
                            "relationship": sql_type,
                            "parentTableName": parent
                        })

            else:
                try:
                    # First, try to get lineage by transpiling to T-SQL.
                    # This might be desirable if the rest of your tools work better with T-SQL.
                    tsql = sqlglot.transpile(one_sql, read="teradata", write="tsql")[0]
                    print(tsql)
                    result = LineageRunner(tsql, dialect='tsql', verbose=False)
                    logger.info("Successfully transpiled to T-SQL for lineage analysis.")
                    target_tables = [str(table) for table in result.target_tables]
                    source_tables = [str(table) for table in result.source_tables]

                except Exception as e_transpile:
                    # If transpiling fails, log a warning and fall back to using the original Teradata dialect.
                    logger.warning(f"Could not transpile to T-SQL, falling back to Teradata dialect. Reason: {e_transpile}")
                    
                    # This is the code that you confirmed works
                    result = LineageRunner(one_sql, dialect='teradata', verbose=False)
                    target_tables = [str(table) for table in result.target_tables]
                    source_tables = [str(table) for table in result.source_tables]

                if sql_type == "Select":
                    # For SELECT, the custom name is the child and depends on the source tables
                    for parent in source_tables:
                        dataframe_rows.append({
                            "childTableName": custom_name,
                            "relationship": sql_type,
                            "parentTableName": parent
                        })
                else: # Handles Insert, Update, etc.
                    # Relationship 1: The target tables depend on the source tables
                    if source_tables:
                        for child in target_tables:
                            for parent in source_tables:
                                dataframe_rows.append({
                                    "childTableName": child,
                                    "relationship": sql_type,
                                    "parentTableName": parent
                                })
                    else: # Handle cases like INSERT INTO ... VALUES with no source tables
                        for child in target_tables:
                             dataframe_rows.append({
                                "childTableName": child,
                                "relationship": sql_type,
                                "parentTableName": None
                            })


                    # Relationship 2: The custom query name depends on the target tables
                    for parent in target_tables:
                        dataframe_rows.append({
                            "childTableName": custom_name,
                            "relationship": sql_type,
                            "parentTableName": parent # The target tables are the parents here
                        })

        except Exception as e:
            logger.error(f"Failed to process SQL statement: '{one_sql.strip()}'. Error: {e}")
    
    return dataframe_rows


# Main execution
custom_name = input("Enter Query name: ")
input_path = input("Enter path to query file or folder (press Enter for default 'query.txt'): ").strip()

# Use default query.txt if no path provided
if not input_path:
    input_path = 'query.txt'

# Read queries from the input (file or folder)
sql = read_queries_from_input(input_path)

if not sql:
    logger.error("No queries found to process. Exiting.")
    exit(1)

# Process the queries
dataframe_rows = process_queries(sql, custom_name)

# Create the final DataFrame from the list of rows in one go
df = pd.DataFrame(dataframe_rows)

# Print the resulting DataFrame
print("\n--- Generated DataFrame ---")
print(df)
csv_path = "test2.csv"
if not df.empty:
    df.to_csv(csv_path, index=False)
    logger.info(f"Lineage saved to {csv_path}")