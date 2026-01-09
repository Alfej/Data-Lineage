import pandas as pd

# Read the CSV file
df = pd.read_csv('test.csv')

print("Original Data Shape:", df.shape)
print("\n" + "="*80 + "\n")

# Step 1: Filter tables by parentTableType
print("Step 1: Filtering tables by parent type\n")

sector_table = df[df['parentTableType'] == 'Sector'][['childTableName', 'parentTableName']].copy()
sector_table.columns = ['Application', 'Sector']
print(f"Sector table: {len(sector_table)} records")

application_table = df[df['parentTableType'] == 'Application'][['childTableName', 'parentTableName']].copy()
application_table.columns = ['Purpose', 'Application']
print(f"Application table: {len(application_table)} records")

purpose_table = df[df['parentTableType'] == 'Purpose'][['childTableName', 'parentTableName']].copy()
purpose_table.columns = ['Client', 'Purpose']
print(f"Purpose table: {len(purpose_table)} records")

client_table = df[df['parentTableType'] == 'Client'][['childTableName', 'parentTableName']].copy()
client_table.columns = ['Tool', 'Client']
print(f"Client table: {len(client_table)} records")

tool_table = df[df['parentTableType'] == 'Tool'][['childTableName', 'parentTableName']].copy()
tool_table.columns = ['System', 'Tool']
print(f"Tool table: {len(tool_table)} records")

system_table = df[df['parentTableType'] == 'System'][['childTableName', 'parentTableName']].copy()
system_table.columns = ['SystemID', 'System']
print(f"System table: {len(system_table)} records")

systemid_table = df[df['parentTableType'] == 'SystemID'][['childTableName', 'parentTableName']].copy()
systemid_table.columns = ['Schema', 'SystemID']
print(f"SystemID table: {len(systemid_table)} records")

schema_table = df[df['parentTableType'] == 'Schema'][['childTableName', 'parentTableName']].copy()
schema_table.columns = ['ObjectName', 'Schema']
print(f"Schema table: {len(schema_table)} records")

print("\n" + "="*80 + "\n")

# Step 2: Full outer join all tables
print("Step 2: Performing full outer joins\n")

# Start with sector and join progressively
hierarchy = sector_table.merge(application_table, on='Application', how='outer')
print(f"After Application join: {len(hierarchy)} records")

hierarchy = hierarchy.merge(purpose_table, on='Purpose', how='outer')
print(f"After Purpose join: {len(hierarchy)} records")

hierarchy = hierarchy.merge(client_table, on='Client', how='outer')
print(f"After Client join: {len(hierarchy)} records")

hierarchy = hierarchy.merge(tool_table, on='Tool', how='outer')
print(f"After Tool join: {len(hierarchy)} records")

hierarchy = hierarchy.merge(system_table, on='System', how='outer')
print(f"After System join: {len(hierarchy)} records")

hierarchy = hierarchy.merge(systemid_table, on='SystemID', how='outer')
print(f"After SystemID join: {len(hierarchy)} records")

hierarchy = hierarchy.merge(schema_table, on='Schema', how='outer')
print(f"After Schema join: {len(hierarchy)} records")

# Reorder columns
hierarchy = hierarchy[['Sector', 'Application', 'Purpose', 'Client', 'Tool', 'System', 'SystemID', 'Schema', 'ObjectName']]

print("\nHierarchy Table Sample:")
print(hierarchy.head(10))
print("\n" + "="*80 + "\n")

# Step 3: Filter relationship table (parent type NOT in hierarchy levels)
print("Step 3: Filtering relationship data\n")

hierarchy_types = ['Sector', 'Application', 'Purpose', 'Client', 'Tool', 'System', 'SystemID', 'Schema']
relationship_table = df[~df['parentTableType'].isin(hierarchy_types)][['parentTableName', 'relationship', 'childTableName']].copy()
relationship_table.columns = ['ParentObjectName', 'InternalRelationship', 'ChildObjectName']

print(f"Relationship table: {len(relationship_table)} records")
print("\nRelationship Table Sample:")
print(relationship_table.head(10))
print("\n" + "="*80 + "\n")

# Step 4: Join hierarchy with relationship data
print("Step 4: Creating final output\n")

# Rename hierarchy columns for parent side
parent_hierarchy = hierarchy.copy()
parent_hierarchy.columns = ['ParentSector', 'ParentApplication', 'ParentPurpose', 'ParentClient', 
                            'ParentTool', 'ParentSystem', 'ParentSystemID', 'ParentSchema', 'ParentObjectName']

# Rename hierarchy columns for child side
child_hierarchy = hierarchy.copy()
child_hierarchy.columns = ['ChildSector', 'ChildApplication', 'ChildPurpose', 'ChildClient', 
                           'ChildTool', 'ChildSystem', 'ChildSystemID', 'ChildSchema', 'ChildObjectName']

# Join: Parent hierarchy + Relationship + Child hierarchy
final_output = relationship_table.merge(parent_hierarchy, on='ParentObjectName', how='left')
final_output = final_output.merge(child_hierarchy, on='ChildObjectName', how='left')

# Reorder columns
final_output = final_output[['ParentSector', 'ParentApplication', 'ParentPurpose', 'ParentClient', 
                             'ParentTool', 'ParentSystem', 'ParentSystemID', 'ParentSchema', 'ParentObjectName',
                             'InternalRelationship',
                             'ChildSector', 'ChildApplication', 'ChildPurpose', 'ChildClient', 
                             'ChildTool', 'ChildSystem', 'ChildSystemID', 'ChildSchema', 'ChildObjectName']]

print(f"Final output: {len(final_output)} records")
print("\nFinal Output Sample:")
print(final_output.head(10))
print("\n" + "="*80 + "\n")

# Step 5: Save outputs
print("Step 5: Saving files\n")

hierarchy.to_csv('hierarchy_table.csv', index=False)
print("Saved: hierarchy_table.csv")

relationship_table.to_csv('relationship_table.csv', index=False)
print("Saved: relationship_table.csv")

final_output.to_csv('final_output.csv', index=False)
print("Saved: final_output.csv")

print("\n" + "="*80)
print("DONE!")
print("="*80)