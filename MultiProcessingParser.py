import logging
import pandas as pd
from pathlib import Path
import multiprocessing as mp
from functools import partial
import time
import sqlglot
from sqlglot import exp, parse_one
from sqllineage.runner import LineageRunner
from QueryParser import read_queries_from_input, clean_table_name

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_single_query(query_data):
    """Process a single SQL query and return lineage data.
    
    Args:
        query_data: Tuple of (query_string, custom_name, query_index)
    
    Returns:
        Tuple of (dataframe_rows, success_flag, error_info, query_text) 
    """
    one_sql, custom_name, query_idx = query_data
    dataframe_rows = []
    
    # Remove _x000d_ characters
    if '_x000d_' in one_sql:
        one_sql = one_sql.replace('_x000d_', ' ')
    
    # Remove LOCKING...ACCESS clause
    one_sql_lower = one_sql.lower()
    if 'locking' in one_sql_lower and 'access' in one_sql_lower:
        # Find position of 'locking' and 'access'
        locking_pos = one_sql_lower.find('locking')
        access_pos = one_sql_lower.find('access', locking_pos)
        
        if locking_pos != -1 and access_pos != -1 and access_pos > locking_pos:
            # Replace everything from 'locking' to end of 'access' with a space
            one_sql = one_sql[:locking_pos] + ' ' + one_sql[access_pos + 6:]
    
    if len(one_sql.strip()) == 0:
        return (dataframe_rows, False, "EmptyQuery", "Empty query string", one_sql)  # Empty query counts as failure
    
    try:
        parsed_sql = parse_one(one_sql, dialect="teradata")
        sql_type = type(parsed_sql).__name__

        if sql_type == 'Delete':
            tables_involved = [table.name for table in parsed_sql.find_all(exp.Table)]
            if tables_involved:
                child_table = tables_involved[0]
                parent_tables = tables_involved[1:]
                for parent in parent_tables:
                    dataframe_rows.append({
                        "childTableName": clean_table_name(child_table),
                        "relationship": sql_type,
                        "parentTableName": clean_table_name(parent)
                    })
        else:
            try:
                tsql = sqlglot.transpile(one_sql, read="teradata", write="tsql")[0]
                # Don't print in multiprocessing - it serializes execution
                result = LineageRunner(tsql, dialect='tsql', verbose=False)
                target_tables = [str(table) for table in result.target_tables]
                source_tables = [str(table) for table in result.source_tables]
            except Exception:
                # Silent fallback for parallel processing
                result = LineageRunner(one_sql, dialect='teradata', verbose=False)
                target_tables = [str(table) for table in result.target_tables]
                source_tables = [str(table) for table in result.source_tables]

            if sql_type == "Select":
                for parent in source_tables:
                    dataframe_rows.append({
                        "childTableName": custom_name,
                        "relationship": sql_type,
                        "parentTableName": clean_table_name(parent)
                    })
            else:
                if source_tables:
                    for child in target_tables:
                        for parent in source_tables:
                            dataframe_rows.append({
                                "childTableName": clean_table_name(child),
                                "relationship": sql_type,
                                "parentTableName": clean_table_name(parent)
                            })
                else:
                    for child in target_tables:
                        dataframe_rows.append({
                            "childTableName": clean_table_name(child),
                            "relationship": sql_type,
                            "parentTableName": None
                        })

                for parent in target_tables:
                    dataframe_rows.append({
                        "childTableName": custom_name,
                        "relationship": sql_type,
                        "parentTableName": clean_table_name(parent)
                    })
        
        # Successfully processed
        return (dataframe_rows, True, None, None, one_sql)

    except Exception as e:
        # Failed to process this query - capture error type and message
        error_type = type(e).__name__
        error_msg = str(e)  # Full error message
        return (dataframe_rows, False, error_type, error_msg, one_sql)


def process_with_multiprocessing(sql_content, custom_name, num_processes=None):
    """Process queries using multiprocessing pool - processes individual queries in parallel."""
    logger.info("Starting multiprocessing approach")
    start_time = time.time()
    
    # Determine number of processes
    if num_processes is None:
        num_processes = max(1, mp.cpu_count() - 1)  # Leave one core free
    
    logger.info(f"Using {num_processes} processes")
    
    # Split SQL content into individual queries
    queries = [q.strip() for q in sql_content.split(';') if q.strip()]
    total_queries = len(queries)
    
    if total_queries == 0:
        logger.error("No queries to process")
        return pd.DataFrame(), 0
    
    logger.info(f"Processing {total_queries} queries in parallel")
    
    # Prepare query data: (query_string, custom_name, query_index)
    query_data = [(query, custom_name, idx) for idx, query in enumerate(queries)]
    
    # Use chunksize to optimize process distribution
    chunksize = max(1, total_queries // (num_processes * 4))
    logger.info(f"Using chunksize of {chunksize} for process pool")
    
    # Create a pool and process queries in parallel
    print(f"Processing {total_queries} queries across {num_processes} CPU cores...")
    with mp.Pool(processes=num_processes) as pool:
        # Map individual queries to processes
        results = pool.map(process_single_query, query_data, chunksize=chunksize)
    print("Parallel processing completed!")
    
    # Combine all results and count successes/failures
    all_rows = []
    successful_queries = 0
    failed_queries = 0
    error_summary = {}
    error_details_list = []  # List to store detailed error information
    
    for idx, (query_result, success, error_type, error_info, query_text) in enumerate(results):
        all_rows.extend(query_result)
        if success:
            successful_queries += 1
        else:
            failed_queries += 1
            
            # Track error types for summary
            if error_type:
                error_summary[error_type] = error_summary.get(error_type, 0) + 1
                
                # Store detailed error info for CSV export
                error_details_list.append({
                    'query_index': idx,
                    'sql_query': query_text,
                    'error_type': error_type,
                    'error_info': error_info
                })
    
    # Create DataFrame for lineage results
    df = pd.DataFrame(all_rows)
    
    # Create DataFrame for error details and save to CSV
    if error_details_list:
        error_df = pd.DataFrame(error_details_list)
        error_csv_path = 'query_errors_detailed.csv'
        error_df.to_csv(error_csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {len(error_details_list)} error details to {error_csv_path}")
        print(f"\nDetailed error information saved to: {error_csv_path}")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Multiprocessing completed in {elapsed_time:.2f} seconds")
    logger.info(f"Processed {len(df)} lineage relationships from {total_queries} queries")
    
    # Print statistics
    print(f"\n{'='*60}")
    print(f"QUERY PROCESSING STATISTICS")
    print(f"{'='*60}")
    print(f"Total queries:           {total_queries}")
    print(f"Successfully converted:  {successful_queries} ({successful_queries/total_queries*100:.1f}%)")
    print(f"Failed to convert:       {failed_queries} ({failed_queries/total_queries*100:.1f}%)")
    print(f"Lineage relationships:   {len(df)}")
    print(f"{'='*60}")
    
    # Print error breakdown if there are failures
    if failed_queries > 0 and error_summary:
        print(f"\nFAILURE REASONS:")
        print(f"{'-'*60}")
        for error_type, count in sorted(error_summary.items(), key=lambda x: x[1], reverse=True):
            print(f"  {error_type:40s} {count:4d} ({count/failed_queries*100:.1f}%)")
        print(f"{'-'*60}")
        print(f"\nCommon reasons for failures:")
        print(f"  • ParseError: SQL syntax not supported by parser")
        print(f"  • Empty query: Query string is empty or whitespace only")
        print(f"  • UnsupportedError: SQL dialect features not recognized")
        print(f"  • AttributeError: Missing required SQL elements (tables, etc.)")
        print(f"  • TokenError: Invalid SQL tokens or characters")
    print()
    
    return df, elapsed_time, successful_queries, failed_queries


def add_table_types(df, types_file_path, input_path, custom_name):
    """Add childTypes and parentTypes columns to the DataFrame."""
    if df.empty:
        logger.warning("DataFrame is empty, no types will be added")
        return df
    
    if not types_file_path or not Path(types_file_path).exists():
        logger.info("Skipping table types lookup - defaulting all to View")
        df['childTypes'] = df['childTableName'].apply(lambda x: 'Unknown' if x == custom_name else 'View')
        df['parentTypes'] = 'View'
        return df
    
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
        type_lookup = {}
        for _, row in types_df.iterrows():
            obj_name = str(row['obj_name']).strip().upper()
            table_kind = str(row['TableKind']).strip()
            type_lookup[obj_name] = table_kind
        
        # Function to get table type
        def get_table_type(table_name, is_query_name=False):
            if pd.isna(table_name) or table_name is None:
                return None
            if is_query_name:
                return 'Unknown'
            clean_name = str(table_name).strip().upper()
            type_code = type_lookup.get(clean_name, 'V')
            if type_code == 'V':
                return 'View'
            elif type_code == 'T':
                return 'Table'
            else:
                return 'View'
        
        # Add childTypes and parentTypes columns
        df['childTypes'] = df['childTableName'].apply(
            lambda x: get_table_type(x, is_query_name=(x == custom_name))
        )
        df['parentTypes'] = df['parentTableName'].apply(get_table_type)
        logger.info("Added childTypes and parentTypes columns")
        
    except Exception as e:
        logger.error(f"Error processing object types file: {e}")
        df['childTypes'] = df['childTableName'].apply(lambda x: 'Unknown' if x == custom_name else 'View')
        df['parentTypes'] = 'View'
    
    return df


def main():
    """Main execution function for multiprocessing parser."""
    print("\n" + "="*60)
    print("MULTIPROCESSING SQL QUERY PARSER")
    print("="*60 + "\n")
    
    # Get user inputs
    custom_name = input("Enter Query name: ")
    input_path = input("Enter path to query file or folder (press Enter for default 'query.txt'): ").strip()
    
    # Use default query.txt if no path provided
    if not input_path:
        input_path = 'query.txt'
    
    # Read queries from the input
    logger.info("Reading queries from input...")
    read_start = time.time()
    sql = read_queries_from_input(input_path)
    read_time = time.time() - read_start
    logger.info(f"Queries read in {read_time:.2f} seconds")
    
    if not sql:
        logger.error("No queries found to process. Exiting.")
        return
    
    # Ask for number of processes
    num_processes_input = input(f"Enter number of processes to use (press Enter for default {max(1, mp.cpu_count() - 1)}): ").strip()
    num_processes = int(num_processes_input) if num_processes_input else None
    
    # Process the queries with multiprocessing
    df, processing_time, successful_queries, failed_queries = process_with_multiprocessing(sql, custom_name, num_processes)
    
    # Add table types
    types_file_path = input("Enter path to object types CSV file (press Enter to skip): ").strip()
    if types_file_path:
        types_start = time.time()
        df = add_table_types(df, types_file_path, input_path, custom_name)
        types_time = time.time() - types_start
        logger.info(f"Types added in {types_time:.2f} seconds")
    
    # Save to CSV
    csv_path = "test2_multiprocessing.csv"
    if not df.empty:
        save_start = time.time()
        df.to_csv(csv_path, index=False)
        save_time = time.time() - save_start
        logger.info(f"CSV saved in {save_time:.2f} seconds")
        logger.info(f"Lineage saved to {csv_path}")
        
        # Print summary
        print("\n" + "="*60)
        print("PROCESSING COMPLETE - MULTIPROCESSING APPROACH")
        print("="*60)
        print(f"Total rows generated: {len(df)}")
        print(f"Query reading time: {read_time:.2f} seconds")
        print(f"Query processing time: {processing_time:.2f} seconds")
        if types_file_path:
            print(f"Type matching time: {types_time:.2f} seconds")
            print(f"Total time: {read_time + processing_time + types_time + save_time:.2f} seconds")
        else:
            print(f"Total time: {read_time + processing_time + save_time:.2f} seconds")
        print(f"Output file: {csv_path}")
        print("="*60 + "\n")
        
        # Show sample of results
        print("\n--- Sample Results (first 10 rows) ---")
        print(df.head(10))
    else:
        logger.warning("No data to save")


if __name__ == "__main__":
    # query = "Replace VIEW SEM_PBDW_MAA.\"CUST_INQ_V\" AS SELECT * FROM PBDW_P_MAA. CUST_INQ_V"
    # df,_,_,_,_=process_single_query((query, "TestQuery", 0))
    # print(df)

    main()
