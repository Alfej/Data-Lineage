import logging
import sqlglot
from sqlglot import exp, parse_one
import pandas as pd
from sqllineage.runner import LineageRunner
import os
from pathlib import Path
import time

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_queries_from_sql(file_path):
    """Read SQL queries from a sql file."""
    with open(file_path, 'r') as file:
        return (Path(file_path).stem, file.read())

def read_queries_from_excel(file_path):
    """Read SQL queries from an Excel file with support for multi-line queries."""
    try:
        df = pd.read_excel(file_path)
        
        # Find the query column
        query_column = None
        for col in ['query', 'Query', 'QUERY', 'sql', 'SQL', 'Sql', 'view_sql', 'VIEW_SQL', 'View_SQL']:
            if col in df.columns:
                query_column = col
                break
        
        if query_column is None:
            query_column = df.columns[-1]
            logger.warning(f"No query column found in {file_path}. Using last column: {query_column}")

        logger.info(f"Using column '{query_column}' for queries from {file_path}")
        
        # Check for LineNumber column
        line_number_column = None
        for col in ['LineNumber', 'linenumber', 'line_number', 'Line_Number', 'LINENUMBER']:
            if col in df.columns:
                line_number_column = col
                break
        if line_number_column is None:
            line_number_column = df.columns[-2]
            logger.info(f"No LineNumber column found in {file_path}. Queries will not be sorted.")
        
        # Check for QueryID or similar grouping column
        query_name_column = None
        for col in ['TableName', 'ViewName', 'SQLName', 'view_name', 'QueryNumber', 'query_number']:
            if col in df.columns:
                query_name_column = col
                break
        if query_name_column is None:
            query_name_column = df.columns[0]
            logger.info(f"No QueryID column found in {file_path}. All queries will be treated as one group.")
        
        # Group by QueryID if it exists, otherwise treat all rows as one query
        if query_name_column:
            df = df.sort_values(by=[query_name_column, line_number_column] if line_number_column else [query_name_column])
            grouped = df.groupby(query_name_column)[query_column].apply(lambda x: '\n'.join(x.astype(str))).tolist()
            queries = grouped
        else:
            queries = df[query_column].astype(str).tolist()
        
        return (Path(file_path).stem, ';'.join(queries))
        
    except Exception as e:
        logger.error(f"Error reading Excel file {file_path}: {e}")
        return (Path(file_path).stem, "")

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
            if file.suffix.lower() in ['.txt', '.sql']:
                query_files.append(('sql', str(file)))
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
        if path.suffix.lower() in ['.txt', '.sql']:
            logger.info(f"Reading queries from sql file: {input_path}")
            return read_queries_from_sql(input_path)
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
            if file_type == 'sql':
                all_queries.append(read_queries_from_sql(file_path))
            elif file_type == 'excel':
                all_queries.append(read_queries_from_excel(file_path))
        
        return all_queries
    
    return ""


def clean_table_name(table_name, file_nme):
    """Remove <default>. prefix from table names."""
    if table_name is None:
        return None
    table_str = str(table_name).strip()
    if table_str.startswith('<default>.'):
        return table_str.replace('<default>', file_nme, 1)
    return table_str