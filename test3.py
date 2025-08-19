import logging
import sqlglot
from sqlglot import expressions as exp
from dataclasses import dataclass, field
from typing import List, Dict, Set, Any, Optional
import pandas as pd
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TableInfo:
    name: str
    type: str  # 'Table', 'View', 'CTE'
    relationship: str  # 'Insert', 'Update', 'Delete', 'Create', 'Select'
    tables_depends_on: List['TableInfo'] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'table_name': self.name,
            'type': self.type,
            'relationship': self.relationship,
            'tables_depends_on': [dep.to_dict() for dep in self.tables_depends_on]
        }
    
    def __eq__(self, other):
        if not isinstance(other, TableInfo):
            return False
        return self.name == other.name and self.type == other.type
    
    def __hash__(self):
        return hash((self.name, self.type))

class SQLGlotParser:
    def __init__(self):
        self.cte_names: Set[str] = set()
        self.all_ctes: Dict[str, TableInfo] = {}
        
    def clean_table_name(self, name: str) -> str:
        """Clean table name by removing quotes and unnecessary characters."""
        if not name:
            return name
        # Handle schema.table format
        cleaned = name.strip('[]"` ')
        return cleaned
    
    def preprocess_sql(self, sql: str) -> str:
        """Preprocess SQL to handle Teradata-specific syntax."""
        # Remove Teradata-specific commands like 'ET;'
        sql = re.sub(r'\bET\s*;', '', sql, flags=re.IGNORECASE)
        
        # Handle multiple statements - split by semicolon but be careful with nested queries
        # For now, let's clean up extra whitespace and newlines
        sql = re.sub(r'\s+', ' ', sql)
        sql = sql.strip()
        
        return sql
    
    def split_statements(self, sql: str) -> List[str]:
        """Split SQL into individual statements, handling nested queries properly."""
        statements = []
        current_statement = ""
        paren_count = 0
        quote_char = None
        i = 0
        
        while i < len(sql):
            char = sql[i]
            
            # Handle quotes
            if char in ["'", '"'] and quote_char is None:
                quote_char = char
            elif char == quote_char:
                quote_char = None
            elif quote_char is not None:
                current_statement += char
                i += 1
                continue
            
            # Count parentheses when not in quotes
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ';' and paren_count == 0:
                # End of statement
                if current_statement.strip():
                    statements.append(current_statement.strip())
                current_statement = ""
                i += 1
                continue
            
            current_statement += char
            i += 1
        
        # Add the last statement if it exists
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return statements
    
    def parse_sql(self, sql: str, final_sql_name: str) -> pd.DataFrame:
        """Parse the SQL query and return a DataFrame with lineage information."""
        logger.info(f"Parsing SQL query with SQLGlot...")
        
        try:
            # Preprocess the SQL
            processed_sql = self.preprocess_sql(sql)
            
            # Split into individual statements
            statements = self.split_statements(processed_sql)
            logger.info(f"Found {len(statements)} statements to process")
            
            all_results = []
            
            for i, stmt in enumerate(statements):
                if not stmt.strip():
                    continue
                    
                logger.info(f"Processing statement {i+1}: {stmt[:100]}...")
                
                try:
                    # Parse each statement individually
                    parsed = sqlglot.parse(stmt, dialect="teradata")
                    
                    for statement in parsed:
                        if statement:
                            logger.info(f"Processing statement type: {type(statement).__name__}")
                            results = self.analyze_statement(statement)
                            all_results.extend(results)
                            
                except Exception as e:
                    logger.warning(f"Could not parse statement {i+1} with SQLGlot: {e}")
                    # Try fallback parsing for this statement
                    fallback_results = self.fallback_parse_statement(stmt)
                    all_results.extend(fallback_results)
            
            logger.info(f"Found {len(all_results)} main results")
            logger.info(f"Found {len(self.all_ctes)} CTEs: {list(self.all_ctes.keys())}")
            
            # Create final result with the given SQL script name
            if all_results:
                final_result = [TableInfo(
                    name=final_sql_name,
                    type="Script",
                    relationship="Contains",
                    tables_depends_on=all_results
                )]
            else:
                final_result = []
            
            return self.flatten_tableinfo_to_df(final_result)
            
        except Exception as e:
            logger.error(f"Error parsing SQL with SQLGlot: {e}")
            import traceback
            traceback.print_exc()
            # Fallback to original parsing method if SQLGlot fails
            return self.fallback_parse(sql, final_sql_name)
    
    def analyze_statement(self, statement) -> List[TableInfo]:
        """Analyze a single SQL statement and return TableInfo objects."""
        results = []
        
        # First, extract all CTEs from this statement recursively
        self.extract_all_ctes(statement)
        
        # Handle different types of statements
        if isinstance(statement, exp.Insert):
            main_table = self.handle_insert(statement)
            if main_table:
                results.append(main_table)
        elif isinstance(statement, exp.Update):
            main_table = self.handle_update(statement)
            if main_table:
                results.append(main_table)
        elif isinstance(statement, exp.Delete):
            main_table = self.handle_delete(statement)
            if main_table:
                results.append(main_table)
        elif isinstance(statement, exp.Merge):
            main_table = self.handle_merge(statement)
            if main_table:
                results.append(main_table)
        elif isinstance(statement, exp.Create):
            main_table = self.handle_create(statement)
            if main_table:
                results.append(main_table)
        elif isinstance(statement, exp.Select):
            # For standalone SELECT statements, we still want to capture CTEs
            select_results = self.handle_select(statement)
            results.extend(select_results)
        
        logger.info(f"Statement analysis complete. Found {len(results)} results")
        return [r for r in results if r is not None]
    
    def extract_all_ctes(self, node):
        """Recursively extract all CTEs from a node and its children."""
        if not node:
            return
            
        # Check if this node has CTEs
        if hasattr(node, 'ctes') and node.ctes:
            logger.info(f"Found {len(node.ctes)} CTEs in node")
            for cte in node.ctes:
                self.process_cte(cte)
        
        # Check for CTE nodes directly
        for cte_node in node.find_all(exp.CTE):
            self.process_cte(cte_node)
        
        # Recursively check child nodes
        if hasattr(node, 'args'):
            for arg_name, arg_value in node.args.items():
                if isinstance(arg_value, (list, tuple)):
                    for item in arg_value:
                        if hasattr(item, 'find_all'):
                            self.extract_all_ctes(item)
                elif hasattr(arg_value, 'find_all'):
                    self.extract_all_ctes(arg_value)
    
    def process_cte(self, cte):
        """Process a single CTE and add it to our tracking."""
        if not cte:
            return
        
        # Extract CTE name - it should be the alias
        cte_name = None
        if hasattr(cte, 'alias') and cte.alias:
            cte_name = str(cte.alias)
        elif hasattr(cte, 'args') and 'alias' in cte.args:
            cte_name = str(cte.args['alias'])
        
        # Extract CTE expression - it should be the 'this' part
        cte_expression = None
        if hasattr(cte, 'this') and cte.this:
            cte_expression = cte.this
        elif hasattr(cte, 'args') and 'this' in cte.args:
            cte_expression = cte.args['this']
        
        if not cte_name:
            logger.warning(f"Could not extract CTE name from: {cte}")
            return
        
        cte_name = self.clean_table_name(cte_name)
        
        # Only process if we haven't seen this CTE yet
        if cte_name in self.all_ctes:
            return
            
        self.cte_names.add(cte_name)
        
        # Extract dependencies from the CTE's query
        dependencies = []
        if cte_expression:
            logger.info(f"Processing CTE '{cte_name}' expression: {cte_expression}")
            dependencies = self.extract_table_references(cte_expression)
            logger.info(f"CTE '{cte_name}' found {len(dependencies)} dependencies: {[d.name for d in dependencies]}")
        else:
            logger.warning(f"CTE '{cte_name}' has no expression")
        
        cte_info = TableInfo(
            name=cte_name,
            type='CTE',
            relationship='Select',
            tables_depends_on=dependencies
        )
        
        self.all_ctes[cte_name] = cte_info
        logger.info(f"Processed CTE: {cte_name} with {len(dependencies)} dependencies")
    
    def extract_table_references(self, node) -> List[TableInfo]:
        """Extract all table references from a node."""
        references = []
        
        if not node:
            return references
        
        logger.info(f"Extracting table references from node type: {type(node).__name__}")
        
        # Find all table nodes
        for table_node in node.find_all(exp.Table):
            table_name = self.get_table_name(table_node)
            if table_name:
                table_name = self.clean_table_name(table_name)
                logger.info(f"Raw table found: {table_name}")
                
                # Determine if it's a CTE or regular table
                table_type = 'CTE' if table_name.split('.')[-1] in self.cte_names else 'Table'
                
                table_info = TableInfo(
                    name=table_name,
                    type=table_type,
                    relationship='Select'
                )
                
                # Avoid duplicates
                if not any(ref.name == table_name and ref.type == table_type for ref in references):
                    references.append(table_info)
                    logger.info(f"Added table reference: {table_name} ({table_type})")
        
        # Also look for aliased table references
        for from_node in node.find_all(exp.From):
            if hasattr(from_node, 'this') and from_node.this:
                table_name = self.get_table_name(from_node.this)
                if table_name:
                    table_name = self.clean_table_name(table_name)
                    table_type = 'CTE' if table_name.split('.')[-1] in self.cte_names else 'Table'
                    table_info = TableInfo(name=table_name, type=table_type, relationship='Select')
                    if not any(ref.name == table_name and ref.type == table_type for ref in references):
                        references.append(table_info)
                        logger.info(f"Added FROM table reference: {table_name} ({table_type})")
        
        # Look for JOIN references
        for join_node in node.find_all(exp.Join):
            if hasattr(join_node, 'this') and join_node.this:
                table_name = self.get_table_name(join_node.this)
                if table_name:
                    table_name = self.clean_table_name(table_name)
                    table_type = 'CTE' if table_name.split('.')[-1] in self.cte_names else 'Table'
                    table_info = TableInfo(name=table_name, type=table_type, relationship='Select')
                    if not any(ref.name == table_name and ref.type == table_type for ref in references):
                        references.append(table_info)
                        logger.info(f"Added JOIN table reference: {table_name} ({table_type})")
        
        logger.info(f"Total references found: {len(references)}")
        return references
    
    def handle_insert(self, insert_stmt) -> Optional[TableInfo]:
        """Handle INSERT statements."""
        table_name = self.get_table_name(insert_stmt.this)
        if not table_name:
            return None
        
        table_name = self.clean_table_name(table_name)
        dependencies = []
        
        # Get dependencies from the SELECT part
        if hasattr(insert_stmt, 'expression') and insert_stmt.expression:
            dependencies = self.extract_table_references(insert_stmt.expression)
            
            # Also check for CTEs in the INSERT expression
            self.extract_all_ctes(insert_stmt.expression)
        
        # Replace any CTE references with the actual CTE objects
        enhanced_dependencies = []
        for dep in dependencies:
            if dep.name.split('.')[-1] in self.all_ctes and dep.type == 'CTE':
                enhanced_dependencies.append(self.all_ctes[dep.name.split('.')[-1]])
            else:
                enhanced_dependencies.append(dep)
        
        logger.info(f"INSERT into table: {table_name} with {len(enhanced_dependencies)} dependencies")
        return TableInfo(
            name=table_name,
            type='Table',
            relationship='Insert',
            tables_depends_on=enhanced_dependencies
        )
    
    def handle_update(self, update_stmt) -> Optional[TableInfo]:
        """Handle UPDATE statements."""
        # For Teradata UPDATE syntax, the table might be in different places
        target_table = None
        
        # Try different ways to get the target table
        if hasattr(update_stmt, 'this') and update_stmt.this:
            target_table = self.get_table_name(update_stmt.this)
        
        # If not found, look for the table in expressions
        if not target_table:
            # Look for table references in the entire statement
            all_tables = self.extract_table_references(update_stmt)
            if all_tables:
                # Usually the first table is the target in UPDATE
                target_table = all_tables[0].name
        
        if not target_table:
            logger.warning("Could not find target table in UPDATE statement")
            return None
        
        target_table = self.clean_table_name(target_table)
        
        # Get all table references from the UPDATE statement
        all_dependencies = self.extract_table_references(update_stmt)
        
        # Remove the target table from dependencies (it's not a dependency, it's the target)
        dependencies = [dep for dep in all_dependencies if dep.name != target_table]
        
        logger.info(f"UPDATE table: {target_table} with {len(dependencies)} dependencies")
        return TableInfo(
            name=target_table,
            type='Table',
            relationship='Update',
            tables_depends_on=dependencies
        )
    
    def handle_delete(self, delete_stmt) -> Optional[TableInfo]:
        """Handle DELETE statements."""
        table_name = self.get_table_name(delete_stmt.this)
        if not table_name:
            return None
        
        table_name = self.clean_table_name(table_name)    
        dependencies = self.extract_table_references(delete_stmt)
        
        # Remove the target table from dependencies
        dependencies = [dep for dep in dependencies if dep.name != table_name]
        
        logger.info(f"DELETE from table: {table_name}")
        return TableInfo(
            name=table_name,
            type='Table',
            relationship='Delete',
            tables_depends_on=dependencies
        )
    
    def handle_merge(self, merge_stmt) -> Optional[TableInfo]:
        """Handle MERGE statements."""
        table_name = self.get_table_name(merge_stmt.this)
        if not table_name:
            return None
        
        table_name = self.clean_table_name(table_name)
        dependencies = self.extract_table_references(merge_stmt)
        
        # Remove the target table from dependencies
        dependencies = [dep for dep in dependencies if dep.name != table_name]
        
        logger.info(f"MERGE into table: {table_name}")
        return TableInfo(
            name=table_name,
            type='Table',
            relationship='Insert',  # Treating MERGE as Insert type as requested
            tables_depends_on=dependencies
        )
    
    def handle_create(self, create_stmt) -> Optional[TableInfo]:
        """Handle CREATE statements."""
        if hasattr(create_stmt, 'this') and create_stmt.this:
            table_name = self.get_table_name(create_stmt.this)
            if not table_name:
                return None
            
            table_name = self.clean_table_name(table_name)
            dependencies = []
            
            # For CREATE TABLE AS SELECT, extract dependencies
            if hasattr(create_stmt, 'expression') and create_stmt.expression:
                dependencies = self.extract_table_references(create_stmt.expression)
            
            # Determine if it's a view or table
            table_type = 'View' if isinstance(create_stmt, exp.Create) and create_stmt.kind == "VIEW" else 'Table'
            
            logger.info(f"CREATE {table_type}: {table_name}")
            return TableInfo(
                name=table_name,
                type=table_type,
                relationship='Insert',  # Treating CREATE as INSERT type as requested
                tables_depends_on=dependencies
            )
        
        return None
    
    def handle_select(self, select_stmt) -> List[TableInfo]:
        """Handle SELECT statements and CTEs."""
        results = []
        
        # The CTEs are already extracted in analyze_statement
        # For standalone SELECT, we don't create a main table entry
        # but we still track the CTEs
        return results
    
    def get_table_name(self, table_expr) -> Optional[str]:
        """Extract table name from table expression."""
        if not table_expr:
            return None
        
        # Handle different types of table expressions
        if isinstance(table_expr, str):
            return table_expr
        elif hasattr(table_expr, 'name'):
            return str(table_expr.name)
        elif hasattr(table_expr, 'this'):
            if isinstance(table_expr.this, str):
                return table_expr.this
            elif hasattr(table_expr.this, 'name'):
                return str(table_expr.this.name)
            elif hasattr(table_expr.this, 'this'):
                return str(table_expr.this.this)
            else:
                return str(table_expr.this)
        else:
            # Try to convert to string and extract meaningful part
            table_str = str(table_expr)
            # Remove common prefixes/suffixes
            if ' AS ' in table_str.upper():
                table_str = table_str.split(' AS ')[0].strip()
            return table_str
    
    def fallback_parse_statement(self, stmt: str) -> List[TableInfo]:
        """Fallback parsing for individual statements that SQLGlot can't handle."""
        results = []
        stmt_upper = stmt.upper().strip()
        
        # Simple regex-based parsing for common patterns
        if stmt_upper.startswith('INSERT'):
            # Extract INSERT target and source tables
            insert_match = re.search(r'INSERT\s+INTO\s+([^\s(]+)', stmt, re.IGNORECASE)
            if insert_match:
                target_table = self.clean_table_name(insert_match.group(1))
                
                # Find FROM tables
                from_tables = self.extract_tables_regex(stmt, ['FROM', 'JOIN'])
                
                dependencies = [TableInfo(name=table, type='Table', relationship='Select') 
                               for table in from_tables]
                
                results.append(TableInfo(
                    name=target_table,
                    type='Table',
                    relationship='Insert',
                    tables_depends_on=dependencies
                ))
        
        elif stmt_upper.startswith('UPDATE'):
            # Extract UPDATE target
            # Handle both "UPDATE table" and "UPDATE target FROM table" patterns
            update_match = re.search(r'UPDATE\s+(?:(\w+)\s+FROM\s+)?([^\s,]+)', stmt, re.IGNORECASE)
            if update_match:
                alias = update_match.group(1)
                target_table = update_match.group(2)
                
                if alias and not target_table.replace('.', '').replace('_', '').isalnum():
                    # If target_table looks like it might be the actual table, use it
                    # Otherwise, look for the actual table name
                    target_table = self.clean_table_name(target_table)
                else:
                    target_table = self.clean_table_name(target_table)
                
                # Find source tables (excluding the target)
                from_tables = self.extract_tables_regex(stmt, ['FROM', 'JOIN'])
                from_tables = [t for t in from_tables if t != target_table]
                
                dependencies = [TableInfo(name=table, type='Table', relationship='Select') 
                               for table in from_tables]
                
                results.append(TableInfo(
                    name=target_table,
                    type='Table',
                    relationship='Update',
                    tables_depends_on=dependencies
                ))
        
        return results
    
    def extract_tables_regex(self, sql: str, keywords: List[str]) -> List[str]:
        """Extract table names using regex patterns."""
        tables = set()
        
        for keyword in keywords:
            # Pattern to match table names after keywords
            pattern = rf'{keyword}\s+([^\s,()]+(?:\.[^\s,()]+)?)'
            matches = re.findall(pattern, sql, re.IGNORECASE)
            
            for match in matches:
                # Clean up the match
                table = match.strip()
                # Remove alias if present
                if ' ' in table:
                    table = table.split()[0]
                table = self.clean_table_name(table)
                if table and not table.upper() in ['SELECT', 'WHERE', 'ORDER', 'GROUP', 'HAVING']:
                    tables.add(table)
        
        return list(tables)
    
    def fallback_parse(self, sql: str, final_sql_name: str) -> pd.DataFrame:
        """Fallback to regex-based parsing method if SQLGlot fails completely."""
        logger.warning("Using complete fallback parsing method...")
        
        # Split statements and parse each one
        statements = self.split_statements(sql)
        all_results = []
        
        for stmt in statements:
            if stmt.strip():
                results = self.fallback_parse_statement(stmt)
                all_results.extend(results)
        
        if all_results:
            final_result = [TableInfo(
                name=final_sql_name,
                type="Script",
                relationship="Contains",
                tables_depends_on=all_results
            )]
        else:
            final_result = []
        
        return self.flatten_tableinfo_to_df(final_result)
    
    def flatten_tableinfo_to_df(self, tableinfos: List[TableInfo], csv_path: str = 'parent_child_lineage.csv') -> pd.DataFrame:
        """Convert TableInfo objects to a flat DataFrame structure."""
        rows = []
        processed_pairs = set()  # To avoid duplicate rows
        
        def recurse(child: TableInfo):
            for parent in child.tables_depends_on:
                # Create a unique key to avoid duplicates
                pair_key = (child.name, child.type, parent.name, parent.type)
                if pair_key not in processed_pairs:
                    rows.append({
                        'childTableName': child.name,
                        'childTableType': child.type,
                        'relationship': child.relationship,
                        'parentTableName': parent.name,
                        'parentTableType': parent.type
                    })
                    processed_pairs.add(pair_key)
                
                recurse(parent)
        
        for ti in tableinfos:
            recurse(ti)
        
        df = pd.DataFrame(rows)
        if not df.empty:
            df.to_csv(csv_path, index=False)
            logger.info(f"Lineage saved to {csv_path}")
        
        return df

# Example usage and testing
if __name__ == "__main__":
    parser = SQLGlotParser()
    
    try:
        final_sql_name = input("Enter SQL Script name (or press Enter for 'test_script'): ").strip()
        if not final_sql_name:
            final_sql_name = "test_script"
        
        # You can read from file instead
        with open('query.txt', 'r', encoding='utf-8') as f:
            sample_sql = f.read()
        
        parsed_df = parser.parse_sql(sample_sql, final_sql_name)
        print("\nParsed Lineage:")
        print(parsed_df)
        print(f"\nParsed {len(parsed_df)} lineage relationships.")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")