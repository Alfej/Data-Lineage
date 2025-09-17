import logging
import sqlglot
from sqlglot import exp, parse_one
import pandas as pd
from sqllineage.runner import LineageRunner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


custom_name = input("Enter Query name:")

# This list will directly store the dictionaries for each row of the final DataFrame.
dataframe_rows = []

with open('query.txt','r') as file:
    sql = file.read()
# sql = sample_sql

for one_sql in sql.split(';'):
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


# Create the final DataFrame from the list of rows in one go
df = pd.DataFrame(dataframe_rows)

# Print the resulting DataFrame
print("\n--- Generated DataFrame ---")
print(df)
csv_path = "test2.csv"
if not df.empty:
    df.to_csv(csv_path, index=False)
    logger.info(f"Lineage saved to {csv_path}")