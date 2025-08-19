import logging
import sqlglot
from sqlglot import expressions as exp
from dataclasses import dataclass, field
from typing import List, Dict, Set, Any, Optional
import pandas as pd

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
        return name.strip('[]"` ')
    
    def parse_sql(self, sql: str, final_sql_name: str) -> pd.DataFrame:
        """Parse the SQL query and return a DataFrame with lineage information."""
        logger.info(f"Parsing SQL query with SQLGlot...")
        
        try:
            # Parse the SQL using SQLGlot with Teradata dialect
            parsed = sqlglot.parse(sql, dialect="teradata")
            
            all_results = []
            
            for statement in parsed:
                if statement:
                    logger.info(f"Processing statement type: {type(statement).__name__}")
                    # Debug: print the AST structure
                    print(f"Statement AST: {parsed}")
                    
                    # Debug CTE structure if present
                    if hasattr(statement, 'ctes') and statement.ctes:
                        for i, cte in enumerate(statement.ctes):
                            print(f"CTE {i}: {cte}")
                            print(f"CTE type: {type(cte)}")
                            if hasattr(cte, 'alias'):
                                print(f"CTE alias: {cte.alias}")
                            if hasattr(cte, 'this'):
                                print(f"CTE this: {cte.this}")
                                print(f"CTE this type: {type(cte.this)}")
                            if hasattr(cte, 'args'):
                                print(f"CTE args: {cte.args}")
                    
                    results = self.analyze_statement(statement)
                    all_results.extend(results)
            
            logger.info(f"Found {len(all_results)} main results")
            logger.info(f"Found {len(self.all_ctes)} CTEs: {list(self.all_ctes.keys())}")
            
            # Create final result with the given SQL script name
            if all_results:
                final_result = [TableInfo(
                    name=final_sql_name,
                    type="View",
                    relationship="Insert",
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
        
        # Debug: print CTE structure
        print(f"Processing CTE structure: {cte}")
        print(f"CTE args: {cte.args if hasattr(cte, 'args') else 'No args'}")
        
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
            relationship='Created from',
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
                table_type = 'CTE' if table_name in self.cte_names else 'Table'
                
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
                    table_type = 'CTE' if table_name in self.cte_names else 'Table'
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
                    table_type = 'CTE' if table_name in self.cte_names else 'Table'
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
            if dep.name in self.all_ctes and dep.type == 'CTE':
                enhanced_dependencies.append(self.all_ctes[dep.name])
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
        table_name = self.get_table_name(update_stmt.this)
        if not table_name:
            return None
        
        table_name = self.clean_table_name(table_name)
        dependencies = self.extract_table_references(update_stmt)
        
        # Remove the target table from dependencies
        dependencies = [dep for dep in dependencies if dep.name != table_name]
        
        logger.info(f"UPDATE table: {table_name}")
        return TableInfo(
            name=table_name,
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
            relationship='Merge',
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
                relationship='Insert',  # Treating CREATE as INSERT type
                tables_depends_on=dependencies
            )
        
        return None
    
    def handle_select(self, select_stmt) -> List[TableInfo]:
        """Handle SELECT statements and CTEs."""
        results = []
        
        # The CTEs are already extracted in analyze_statement
        # Just return the table references for standalone SELECT
        dependencies = self.extract_table_references(select_stmt)
        results.extend(dependencies)
        
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
    
    def fallback_parse(self, sql: str, final_sql_name: str) -> pd.DataFrame:
        """Fallback to the original parsing method if SQLGlot fails."""
        logger.warning("Using fallback parsing method...")
        # You can implement your original parsing logic here as a backup
        return pd.DataFrame(columns=['childTableName', 'childTableType', 'relationship', 'parentTableName', 'parentTableType'])
    
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
    
    # Test with sample SQL
    # sample_sql = """
    # WITH sales_summary AS (
    #     SELECT 
    #         customer_id,
    #         SUM(amount) as total_amount
    #     FROM orders o
    #     JOIN order_items oi ON o.order_id = oi.order_id
    #     WHERE o.order_date >= '2023-01-01'
    #     GROUP BY customer_id
    # ),
    # customer_metrics AS (
    #     SELECT 
    #         ss.customer_id,
    #         ss.total_amount,
    #         c.customer_name
    #     FROM sales_summary ss
    #     JOIN customers c ON ss.customer_id = c.customer_id
    # )
    # INSERT INTO customer_analytics
    # SELECT 
    #     cm.customer_id,
    #     cm.customer_name,
    #     cm.total_amount,
    #     CURRENT_DATE as analysis_date
    # FROM customer_metrics cm
    # WHERE cm.total_amount > 1000;
    # """
    
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