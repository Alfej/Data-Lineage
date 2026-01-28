
"""
Pandas-based VOLTABLE Resolver - Optimized solution using DataFrame operations.
"""

import pandas as pd
from typing import Set, Tuple, Dict
import sys


class PandasVOLTABLEResolver:
    """Efficient VOLTABLE resolver using Pandas DataFrame operations."""
    
    def __init__(self, input_file: str, output_file: str, verbose: bool = False):
        self.input_file = input_file
        self.output_file = output_file
        self.verbose = verbose
        self.df: pd.DataFrame = None
        self.voltable_dependencies: Dict[str, Set[Tuple[str, str]]] = {}
        
    def log(self, message: str) -> None:
        """Print log messages if verbose mode is enabled."""
        if self.verbose:
            print(message)

    
    
    def load_data(self) -> bool:
        """Load CSV data into DataFrame."""
        try:
            self.df = pd.read_csv(self.input_file).drop_duplicates()
            self.df = self.df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

            self.df.loc[self.df['RELATIONSHIP']!='Delete','RELATIONSHIP'] = 'Loaded From'
            
            # Validate required columns
            required_cols = {'CHILDTABLENAME', 'CHILDTABLETYPE', 'RELATIONSHIP', 
                           'PARENTTABLENAME', 'PARENTTABLETYPE'}
            if not required_cols.issubset(self.df.columns):
                missing = required_cols - set(self.df.columns)
                print(f"Error: Missing required columns: {missing}")
                return False
            
            self.log(f"✓ Loaded {len(self.df)} relationships")
            return True
            
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found!")
            return False
        except Exception as e:
            print(f"Error loading file: {e}")
            return False
    
    def build_voltable_map(self) -> None:
        """Build a mapping of VOLTABLEs to their parent tables using Pandas."""
        # Filter rows where child is VOLTABLE
        voltable_rows = self.df[self.df['CHILDTABLETYPE'] == 'VOLTABLE']
        
        # Group by VOLTABLE name and collect parent tables
        for voltable_name, group in voltable_rows.groupby('CHILDTABLENAME'):
            parents = set(zip(group['PARENTTABLENAME'], group['PARENTTABLETYPE']))
            self.voltable_dependencies[voltable_name] = parents
        
        self.log(f"✓ Found {len(self.voltable_dependencies)} VOLTABLE tables")
        
        if self.verbose:
            for voltable, parents in self.voltable_dependencies.items():
                self.log(f"  {voltable} → {parents}")
    
    def resolve_parent_chain(self, table_name: str, table_type: str, 
                            visited: Set[str] = None) -> Set[Tuple[str, str]]:
        """
        Recursively resolve VOLTABLE dependencies to get final parent tables.
        
        Args:
            table_name: Name of the table to resolve
            table_type: Type of the table
            visited: Set to track visited tables (cycle detection)
            
        Returns:
            Set of (parent_name, parent_type) tuples that are not VOLTABLEs
        """
        if visited is None:
            visited = set()
        
        # Cycle detection
        if table_name in visited:
            return set()
        visited.add(table_name)
        
        # Base case: if not a VOLTABLE, return itself
        if table_type != 'VOLTABLE':
            return {(table_name, table_type)}
        
        # Recursive case: resolve all parents of this VOLTABLE
        resolved_parents = set()
        for parent_name, parent_type in self.voltable_dependencies.get(table_name, set()):
            resolved_parents.update(
                self.resolve_parent_chain(parent_name, parent_type, visited.copy())
            )
        
        return resolved_parents
    
    def resolve_relationships(self) -> pd.DataFrame:
        """
        Resolve all VOLTABLE relationships and return a new DataFrame.
        
        Returns:
            DataFrame with resolved relationships (no VOLTABLEs)
        """
        # Filter out rows where child is VOLTABLE (these are intermediate definitions)
        non_voltable_children = self.df[self.df['CHILDTABLETYPE'] != 'VOLTABLE'].copy()
        
        self.log(f"✓ Processing {len(non_voltable_children)} non-VOLTABLE relationships")
        
        # Separate direct relationships (parent is not VOLTABLE) 
        # from those that need resolution (parent is VOLTABLE)
        direct_relationships = non_voltable_children[
            non_voltable_children['PARENTTABLETYPE'] != 'VOLTABLE'
        ]
        
        voltable_relationships = non_voltable_children[
            non_voltable_children['PARENTTABLETYPE'] == 'VOLTABLE'
        ]
        
        self.log(f"  - {len(direct_relationships)} direct relationships (no resolution needed)")
        self.log(f"  - {len(voltable_relationships)} relationships to resolve")
        
        # Resolve VOLTABLE relationships
        resolved_rows = []
        
        for _, row in voltable_relationships.iterrows():
            child_name = row['CHILDTABLENAME']
            child_type = row['CHILDTABLETYPE']
            relationship = row['RELATIONSHIP']
            parent_name = row['PARENTTABLENAME']
            parent_type = row['PARENTTABLETYPE']
            
            # Resolve the VOLTABLE parent to its actual parent tables
            resolved_parents = self.resolve_parent_chain(parent_name, parent_type)
            
            # Create a new row for each resolved parent
            for resolved_parent_name, resolved_parent_type in resolved_parents:
                resolved_rows.append({
                    'CHILDTABLENAME': child_name,
                    'CHILDTABLETYPE': child_type,
                    'RELATIONSHIP': relationship,
                    'PARENTTABLENAME': resolved_parent_name,
                    'PARENTTABLETYPE': resolved_parent_type
                })
        
        # Combine direct and resolved relationships
        if resolved_rows:
            resolved_df = pd.DataFrame(resolved_rows)
            result = pd.concat([direct_relationships, resolved_df], ignore_index=True)
        else:
            result = direct_relationships
        
        # Remove duplicates
        result = result.drop_duplicates()
        
        # Sort for consistent output
        result = result.sort_values(
            by=['CHILDTABLENAME', 'PARENTTABLENAME']
        ).reset_index(drop=True)
        
        self.log(f"✓ Resolved to {len(result)} relationships")
        
        return result
    
    def save_data(self, df: pd.DataFrame) -> bool:
        """Save resolved DataFrame to CSV."""
        try:
            df = df.drop_duplicates()
            df.to_csv(self.output_file, index=False)
            self.log(f"✓ Saved to '{self.output_file}'")
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False
    
    def print_statistics(self, input_df: pd.DataFrame, output_df: pd.DataFrame) -> None:
        """Print detailed statistics about the transformation."""
        print("\n" + "="*70)
        print("VOLTABLE RESOLUTION STATISTICS")
        print("="*70)
        
        # Input statistics
        print(f"Input relationships:          {len(input_df)}")
        voltables_in_input = input_df[input_df['CHILDTABLETYPE'] == 'VOLTABLE']['CHILDTABLENAME'].nunique()
        print(f"VOLTABLEs found:              {voltables_in_input}")
        
        # Output statistics
        print(f"Output relationships:         {len(output_df)}")
        
        # Changes
        removed = len(input_df[input_df['CHILDTABLETYPE'] == 'VOLTABLE'])
        print(f"Relationships removed:        {removed}")
        
        # Table type distribution
        print(f"\nInput table type distribution:")
        for type_name, count in input_df['CHILDTABLETYPE'].value_counts().items():
            print(f"  {type_name:20} {count}")
        
        print(f"\nOutput table type distribution:")
        for type_name, count in output_df['CHILDTABLETYPE'].value_counts().items():
            print(f"  {type_name:20} {count}")
        
        # Net change
        net_change = len(output_df) - len(input_df)
        print(f"\nNet change:                   {net_change:+d}")
        print("="*70 + "\n")
    
    def process(self) -> bool:
        """Main processing pipeline."""
        print("Starting VOLTABLE resolution...")
        
        # Load data
        if not self.load_data():
            return False
        
        input_df = self.df.copy()
        
        # Build VOLTABLE dependency map
        self.build_voltable_map()
        
        # Resolve relationships
        resolved_df = self.resolve_relationships()
        
        # Save results
        if not self.save_data(resolved_df):
            return False
        
        # Print statistics
        self.print_statistics(input_df, resolved_df)
        
        print("✓ Processing complete!")
        return True


def main():
    """Main entry point."""
    input_path = input("Enter input CSV file path: ").strip()
    output_path = input_path.replace('.csv', '_simplified.csv')
    verbose_input = input("Enable verbose logging? (y/n): ").strip().lower()
    
    # Create resolver and process
    resolver = PandasVOLTABLEResolver(
        input_file=input_path,
        output_file=output_path,
        verbose=(verbose_input == 'y')
    )
    
    success = resolver.process()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()