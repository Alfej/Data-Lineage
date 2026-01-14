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
        
        #return ';'.join(all_queries)
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


def process_queries(sql_content, custom_name):
    """Process SQL queries and generate lineage data."""
    dataframe_rows = []
    
    for one_sql in sql_content.split(';'):
        if len(one_sql.strip()) == 0:
            continue
        try:
            parsed_sql = parse_one(one_sql, dialect='teradata')
            sql_type = type(parsed_sql).__name__

            if sql_type == 'Delete':
                tables_involved = [table.name for table in parsed_sql.find_all(exp.Table)]
                if tables_involved:
                    child_table = tables_involved[0]
                    parent_tables = tables_involved[1:]
                    # Directly create a row for each parent-child relationship
                    for parent in parent_tables:
                        dataframe_rows.append({
                            "childTableName": clean_table_name(child_table),
                            "relationship": sql_type,
                            "parentTableName": clean_table_name(parent)
                        })

            else:
                try:
                    # First, try to get lineage by transpiling to T-SQL.
                    # This might be desirable if the rest of your tools work better with T-SQL.
                    tsql = sqlglot.transpile(one_sql, read="teradata", write="tsql")[0]
                    # print(tsql)  # Commented out for performance - printing slows down execution significantly
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
                            "parentTableName": clean_table_name(parent)
                        })
                else: # Handles Insert, Update, etc.
                    # Relationship 1: The target tables depend on the source tables
                    if source_tables:
                        for child in target_tables:
                            for parent in source_tables:
                                dataframe_rows.append({
                                    "childTableName": clean_table_name(child),
                                    "relationship": sql_type,
                                    "parentTableName": clean_table_name(parent)
                                })
                    else: # Handle cases like INSERT INTO ... VALUES with no source tables
                        for child in target_tables:
                             dataframe_rows.append({
                                "childTableName": clean_table_name(child),
                                "relationship": sql_type,
                                "parentTableName": None
                            })


                    # Relationship 2: The custom query name depends on the target tables
                    for parent in target_tables:
                        dataframe_rows.append({
                            "childTableName": custom_name,
                            "relationship": sql_type,
                            "parentTableName": clean_table_name(parent) # The target tables are the parents here
                        })

        except Exception as e:
            logger.error(f"Failed to process SQL statement: '{one_sql.strip()}'. Error: {e}")
    
    return dataframe_rows


# Main execution - only run if this file is executed directly
if __name__ == "__main__":
    print("\n" + "="*60)
    print("STANDARD SQL QUERY PARSER")
    print("="*60 + "\n")

    overall_start_time = time.time()

    custom_name = input("Enter Query name: ")
    input_path = input("Enter path to query file or folder (press Enter for default 'query.txt'): ").strip()

    # Use default query.txt if no path provided
    if not input_path:
        input_path = 'query.txt'

    # Read queries from the input (file or folder)
    logger.info("Reading queries from input...")
    read_start_time = time.time()
    sql = read_queries_from_input(input_path)
    read_time = time.time() - read_start_time
    logger.info(f"Queries read in {read_time:.2f} seconds")

    if not sql:
        logger.error("No queries found to process. Exiting.")
        exit(1)

    # Process the queries
    logger.info("Processing queries...")
    process_start_time = time.time()
    dataframe_rows = process_queries(sql, custom_name)
    process_time = time.time() - process_start_time
    logger.info(f"Queries processed in {process_time:.2f} seconds")

    # Create the final DataFrame from the list of rows in one go
    df = pd.DataFrame(dataframe_rows)

    # Add table types by looking up from the object types file
    if not df.empty:
        types_file_path = input("Enter path to object types CSV file (press Enter to skip): ").strip()
        types_start_time = time.time()
        if types_file_path and Path(types_file_path).exists():
            try:
                # Read the object types file
                types_df = pd.read_csv(types_file_path)
                logger.info(f"Loaded object types from {types_file_path}")
                
                # If input was an Excel file with view_name column, add those views to the types
                if input_path and Path(input_path).exists():
                    input_file = Path(input_path)
                    if input_file.suffix.lower() in ['.xlsx', '.xls', '.xlsm']:
                        try:
                            views_df = pd.read_excel(input_path)
                            if 'view_name' in views_df.columns:
                                logger.info(f"Found view_name column in {input_file.name}")
                                unique_views = views_df['view_name'].dropna().unique()
                                logger.info(f"Adding {len(unique_views)} unique views to ObjectTypes")
                                
                                # Create new entries for views not already in types_df
                                new_views = []
                                for view_name in unique_views:
                                    view_name_str = str(view_name).strip()
                                    if view_name_str.upper() not in [str(x).strip().upper() for x in types_df['obj_name']]:
                                        new_views.append({'obj_name': view_name_str, 'TableKind': 'V'})
                                
                                if new_views:
                                    new_views_df = pd.DataFrame(new_views)
                                    types_df = pd.concat([types_df, new_views_df], ignore_index=True)
                                    logger.info(f"Added {len(new_views)} new views to ObjectTypes")
                        except Exception as e:
                            logger.warning(f"Could not extract view names from Excel file: {e}")
                
                # Create a dictionary for fast lookup: obj_name -> TableKind
                # Handle case-insensitive matching and strip whitespace
                type_lookup = {}
                for _, row in types_df.iterrows():
                    obj_name = str(row['obj_name']).strip().upper()
                    table_kind = str(row['TableKind']).strip()
                    type_lookup[obj_name] = table_kind
                
                # Function to get table type - defaults to 'View' for unknown tables
                def get_table_type(table_name, is_query_name=False):
                    if pd.isna(table_name) or table_name is None:
                        return None
                    # If it's the query name, return 'Unknown'
                    if is_query_name:
                        return 'Unknown'
                    # Clean the table name and convert to uppercase for matching
                    clean_name = str(table_name).strip().upper()
                    # Get the type code (V or T)
                    type_code = type_lookup.get(clean_name, 'V')
                    # Convert to full names
                    if type_code == 'V':
                        return 'View'
                    elif type_code == 'T':
                        return 'Table'
                    else:
                        return 'View'
                
                # Add childTypes and parentTypes columns
                # Check if childTableName is the custom query name
                df['childTypes'] = df['childTableName'].apply(
                    lambda x: get_table_type(x, is_query_name=(x == custom_name))
                )
                df['parentTypes'] = df['parentTableName'].apply(get_table_type)
                logger.info("Added childTypes and parentTypes columns")
            except Exception as e:
                logger.error(f"Error processing object types file: {e}")
                logger.exception("Full traceback:")
                df['childTypes'] = df['childTableName'].apply(lambda x: 'Unknown' if x == custom_name else 'View')
                df['parentTypes'] = 'View'
        else:
            logger.info("Skipping table types lookup - defaulting all to View")
            df['childTypes'] = df['childTableName'].apply(lambda x: 'Unknown' if x == custom_name else 'View')
            df['parentTypes'] = 'View'
        
        if types_file_path:
            types_time = time.time() - types_start_time
            logger.info(f"Type matching completed in {types_time:.2f} seconds")
    else:
        logger.warning("DataFrame is empty, no types will be added")

    # Print the resulting DataFrame
    print("\n--- Generated DataFrame ---")
    print(df)
    csv_path = "test2.csv"
    if not df.empty:
        save_start_time = time.time()
        df.to_csv(csv_path, index=False)
        save_time = time.time() - save_start_time
        logger.info(f"CSV saved in {save_time:.2f} seconds")
        logger.info(f"Lineage saved to {csv_path}")
        
        # Print timing summary
        overall_time = time.time() - overall_start_time
        print("\n" + "="*60)
        print("PROCESSING COMPLETE - STANDARD APPROACH")
        print("="*60)
        print(f"Total rows generated: {len(df)}")
        print(f"Query reading time: {read_time:.2f} seconds")
        print(f"Query processing time: {process_time:.2f} seconds")
        if 'types_time' in locals():
            print(f"Type matching time: {types_time:.2f} seconds")
        print(f"CSV save time: {save_time:.2f} seconds")
        print(f"Total execution time: {overall_time:.2f} seconds")
        print(f"Output file: {csv_path}")
        print("="*60 + "\n")