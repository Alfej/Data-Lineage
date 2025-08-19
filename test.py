import logging 
from dataclasses import dataclass, field
from typing import List, Dict, Set, Any
import re
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TableInfo:
    name: str
    type: str  # 'Table', 'View', 'CTE'
    relationship: str  # 'Insert', 'Update', 'Select'
    tables_depends_on: List['TableInfo'] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'table_name': self.name,
            'type': self.type,
            'relationship': self.relationship,
            'tables_depends_on': [dep.to_dict() for dep in self.tables_depends_on]
        }

class SQLQueryParser:
    def __init__(self):
        self.cte_names: Set[str] = set()  # Store CTE names to avoid duplicates

    def parse_sql(self, sql: str, final_sql_name) -> List[TableInfo]:
        """Parse the SQL query and return a list of TableInfo objects."""
        logger.info(f"Parsing SQL query: {sql[:50]}...")

        result = sql.split(';')

        # Remove empty statements and strip whitespace
        statements = [stmt.strip() for stmt in result if stmt.strip()]

        results = []
        for statement in statements:
            results.extend(self.analyse_sql(statement))

        print(results)

        final_result = [TableInfo(name=final_sql_name,type="View",relationship="Insert",tables_depends_on=results)]
        return self.flatten_tableinfo_to_df(final_result)

    def get_subquery_end(self, statement, index_of_brac):
        stack = []
        for i in range(index_of_brac, len(statement)):
            if statement[i] == '(':
                stack.append('(')
            elif statement[i] == ')':
                if stack:
                    stack.pop()
                if not stack:
                    return i
        return -1

    def extract_all_table_names_and_aliases(self, text: str) -> List[str]:
        """Extract all table names from comma-separated text, handling aliases properly."""
        # Remove any leading/trailing whitespace
        text = text.strip()
        
        # Split by comma to handle comma-separated table names
        table_parts = [part.strip() for part in text.split(',')]
        
        table_names = []
        for part in table_parts:
            # Handle table aliases: "table_name alias" or "table_name AS alias"
            # We want to extract the actual table name, not the alias
            
            # First, extract the main identifier (table name)
            # This regex captures quoted or unquoted table names
            table_match = re.match(r'((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.#]+))', part.strip())
            if table_match:
                table_name = table_match.group(1).strip('[]"` ')
                
                # Only add if it's a valid table name (not a column or other SQL construct)
                if self.is_basic_table_name(table_name):
                    table_names.append(table_name)
        
        return table_names

    def is_basic_table_name(self, name: str) -> bool:
        """Basic validation for table names - less restrictive than is_valid_table_name."""
        # Remove quotes and brackets
        clean_name = name.strip('[]"` ')
        
        # Skip if it's empty or contains obvious non-table patterns
        if not clean_name or len(clean_name) < 1:
            return False
            
        # Must contain at least one letter (tables should have alphabetic characters)
        if not re.search(r'[a-zA-Z]', clean_name):
            return False
            
        # Skip obvious SQL keywords that aren't table names
        sql_keywords = ['SET', 'WHERE', 'AND', 'OR', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']
        if clean_name.upper() in sql_keywords:
            return False
            
        return True

    def analyse_sql(self, sql: str) -> List[TableInfo]:
        """Analyze the SQL query and return a TableInfo object for INSERT, UPDATE, DELETE, or MERGE."""
        logger.info(f"Analyzing SQL query: {sql[:50]}...")
        results = []
        masked_sql = sql

        subquery_pattern = re.compile(
            r'(\bAS\b|\bFROM\b|\bJOIN\b|\bWHERE\b|\bAND\b|=|>|<|!=|<>|,|\bIN\b)\s*\(',
            re.IGNORECASE
        )
        subquery_matches = list(subquery_pattern.finditer(masked_sql))

        for match in subquery_matches:
            keyword = match.group(1).upper()
            start = match.end() - 1
            end = self.get_subquery_end(masked_sql, start)
            if end != -1:
                subquery = masked_sql[start + 1:end]
                logger.info(f"Found subquery: {subquery}")

                sub_results = self.analyse_sql(subquery)

                # Mask the subquery in the parent SQL with <subquery> tag
                tag = '<subquery>'
                tagged = tag.ljust(end - start - 1, '<')
                masked_sql = masked_sql[:start+1] + tagged + masked_sql[end:]

                if keyword == "AS":
                    # Check if this is a CREATE TABLE AS statement
                    before_as = masked_sql[:match.start()]
                    
                    # Check for CREATE TABLE pattern
                    create_table_match = re.search(
                            r'CREATE\s+(?:VOLATILE\s+|GLOBAL\s+TEMPORARY\s+|LOCAL\s+TEMPORARY\s+|MULTISET\s+|SET\s+)*(?:TABLE|CT)\s+((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.#]+))\s*$',
                            before_as, re.IGNORECASE
                        )
                    
                    if create_table_match:
                        # This is CREATE TABLE AS - treat as Table, not CTE
                        table_name = create_table_match.group(1).strip('[]"` ')
                        logger.info(f"Found CREATE TABLE AS: {table_name}")
                        results.append(TableInfo(
                            name=table_name,
                            type='Table',
                            relationship='Created from',
                            tables_depends_on=sub_results
                        ))
                    else:
                        # This is a CTE - extract just the CTE name
                        cte_name_match = re.search(r'([a-zA-Z0-9_\[\]`". #]+)\s*$', before_as)
                        if cte_name_match:
                            cte_name = cte_name_match.group(1).strip('[]"` ')
                            # Remove "WITH" if it's part of the captured name
                            if cte_name.upper().startswith("WITH "):
                                cte_name = cte_name[5:].lstrip()
                            # Extract just the table name (last part after any keywords)
                            cte_parts = cte_name.split()
                            cte_name = cte_parts[-1] if cte_parts else cte_name
                            
                            logger.info(f"Found CTE: {cte_name}")
                            self.cte_names.add(cte_name)
                            results.append(TableInfo(
                                name=cte_name,
                                type='CTE',
                                relationship='Created from',
                                tables_depends_on=sub_results
                            ))
                else:
                    results.extend(sub_results)

        # Extract table names after FROM (including comma-separated tables)
        # Updated pattern to capture everything after FROM until hitting keywords
        from_pattern = re.compile(
            r'\bFROM\s+([^()]+?)(?=\s+(?:SET|WHERE|GROUP|HAVING|ORDER|LIMIT|UNION|INTERSECT|EXCEPT|;|$)|\s*(?:LEFT|RIGHT|INNER|FULL|CROSS)?\s*JOIN)',
            re.IGNORECASE | re.DOTALL
        )
        
        for match in from_pattern.finditer(masked_sql):
            table_text = match.group(1).strip()
            # Check if this contains masked subqueries or SET clause indicators
            if '<subquery>' not in table_text:
                table_names = self.extract_all_table_names_and_aliases(table_text)
                for table_name in table_names:
                    logger.info(f"Found table in FROM clause: {table_name}")
                    table_type = 'CTE' if table_name in self.cte_names else 'Table'
                    results.append(TableInfo(
                        name=table_name,
                        type=table_type,
                        relationship='Select',
                    ))

        # Extract table names after JOIN
        join_pattern = re.compile(
            r'(?:LEFT|RIGHT|INNER|FULL|CROSS)?\s*JOIN\s+((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.#]+))',
            re.IGNORECASE
        )
        for match in join_pattern.finditer(masked_sql):
            table_name = match.group(1).strip('[]"` ')
            after = sql[match.end():].lstrip()
            if not after.startswith('('):
                logger.info(f"Found table in JOIN: {table_name}")
                table_type = 'CTE' if table_name in self.cte_names else 'Table'
                results.append(TableInfo(
                    name=table_name,
                    type=table_type,
                    relationship='Select',
                ))

                # Extract table names after FROM or JOIN not followed by '(' 
        table_pattern = re.compile(
            r'(FROM|JOIN)\s+((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.#]+))',
            re.IGNORECASE
        )
        for match in table_pattern.finditer(masked_sql):
            table_name = match.group(2).strip('[]"` ')
            after = sql[match.end():].lstrip()
            if not after.startswith('('):
                logger.info(f"Found table: {table_name} after {match.group(1)}")
                table_type = 'CTE' if table_name in self.cte_names else 'Table'
                results.append(TableInfo(
                    name=table_name,
                    type=table_type,
                    relationship='Select',
                ))

        # Check for INSERT INTO, UPDATE, DELETE FROM, or MERGE INTO and build the main TableInfo
        insert_match = re.search(
            r'^\s*INSERT\s+INTO\s+((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.#]+))',
            sql, re.IGNORECASE | re.DOTALL | re.MULTILINE
        )
        update_match = re.search(
            r'^\s*UPDATE\s+((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.]+))',
            sql, re.IGNORECASE | re.DOTALL | re.MULTILINE
        )
        delete_match = re.search(
            r'^\s*DELETE\s+FROM\s+((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.]+))',
            sql, re.IGNORECASE | re.DOTALL | re.MULTILINE
        )
        merge_match = re.search(
            r'^\s*MERGE\s+INTO\s+((?:"[^"]+"|\[[^\]]+\]|`[^`]+`|[a-zA-Z0-9_.]+))',
            sql, re.IGNORECASE | re.DOTALL | re.MULTILINE
        )

        if insert_match:
            table_name = insert_match.group(1).strip('[]"` ')
            logger.info(f"Main table (INSERT): {table_name}")
            return [TableInfo(
                name=table_name,
                type='Table',
                relationship='Insert',
                tables_depends_on=results
            )]
        elif update_match:
            table_name = update_match.group(1).strip('[]"` ')
            logger.info(f"Main table (DELETE): {table_name}")
            return [TableInfo(
                name=table_name,
                type='Table',
                relationship='Update',
                tables_depends_on=results
            )]
        elif delete_match:
            table_name = delete_match.group(1).strip('[]"` ')
            logger.info(f"Main table (DELETE): {table_name}")
            return [TableInfo(
                name=table_name,
                type='Table',
                relationship='Delete',
                tables_depends_on=results
            )]
        elif merge_match:
            table_name = merge_match.group(1).strip('[]"` ')
            logger.info(f"Main table (MERGE): {table_name}")
            return [TableInfo(
                name=table_name,
                type='Table',
                relationship='Merge',
                tables_depends_on=results
            )]
        else:
            return results

    def flatten_tableinfo_to_df(self, tableinfos, csv_path='Frontend/parent_child_lineage.csv'):
        rows = []

        def recurse(child):
            for parent in child.tables_depends_on:
                rows.append({
                    'childTableName': child.name,
                    'childTableType': child.type,
                    'relationship': child.relationship,
                    'parentTableName': parent.name,
                    'parentTableType': parent.type
                })
                recurse(parent)

        for ti in tableinfos:
            recurse(ti)

        df = pd.DataFrame(rows).drop_duplicates()
        df.to_csv(csv_path, index=False)
        return df
        
if __name__ == "__main__":
    parser = SQLQueryParser()
    with open('query.txt', 'r', encoding='utf-8') as f:
        sample_sql = f.read()
    final_sql_name = input("Enter SQL Script name: ")

    parsed_queries = parser.parse_sql(sample_sql, final_sql_name)
    print(parsed_queries)
    print(f"Parsed {len(parsed_queries)} SQL statements.")