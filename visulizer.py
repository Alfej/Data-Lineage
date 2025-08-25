import pandas as pd
import networkx as nx
import json
import webbrowser
from pathlib import Path

# --- NEW IMPORTS ---
import http.server
import socketserver
import threading
import time

# Sample data
# Ensure the CSV is in the same directory as this script, or provide the full path.
data = pd.read_csv("C:\\DataLineage\\parent_child_lineage.csv")
df = pd.DataFrame(data)

def process_data():
    """Process CSV data and prepare for visualization"""
    
    # Create NetworkX graph
    G = nx.DiGraph()
    all_nodes = set(df['childTableName'].tolist() + df['parentTableName'].tolist())
    
    # Add nodes with types
    for node in all_nodes:
        if node in df['childTableName'].values:
            node_type = df[df['childTableName'] == node]['childTableType'].iloc[0]
        else:
            node_type = df[df['parentTableName'] == node]['parentTableType'].iloc[0]
        
        G.add_node(node, node_type=node_type)
    
    # Add edges with relationship information from CSV
    for _, row in df.iterrows():
        relationship = row.get('relationship', 'unknown')  # Use actual relationship column
        G.add_edge(
            row['childTableName'], 
            row['parentTableName'], 
            relationship=relationship
        )
    
    # Convert to data for D3.js
    nodes_data = []
    for node in G.nodes(data=True):
        display_name = node[0]
        
        # Get relationships for this node
        incoming_edges = []
        outgoing_edges = []
        
        for _, row in df.iterrows():
            if row['parentTableName'] == node[0]:  # This node is a parent
                relationship = row.get('relationship', 'depends on')
                child_name = row['childTableName'].split('.')[-1] if '.' in row['childTableName'] else row['childTableName']
                child_type = row['childTableType']
                incoming_edges.append(f"← {child_type}: {child_name} ({relationship})")
            
            if row['childTableName'] == node[0]:  # This node is a child
                relationship = row.get('relationship', 'depends on')
                parent_name = row['parentTableName'].split('.')[-1] if '.' in row['parentTableName'] else row['parentTableName']
                parent_type = row['parentTableType']
                outgoing_edges.append(f"→ {parent_type}: {parent_name} ({relationship})")
        
        nodes_data.append({
            'id': node[0],
            'name': display_name,
            'fullName': node[0],
            'type': node[1].get('node_type', 'Unknown'),
            'incoming': incoming_edges,
            'outgoing': outgoing_edges,
            'hidden': False
        })
    
    edges_data = []
    for _, row in df.iterrows():
        relationship = row.get('relationship', 'unknown')
        edges_data.append({
            'source': row['childTableName'],
            'target': row['parentTableName'],
            'relationship': relationship,
            'type': 'direct'
        })
    
    return nodes_data, edges_data

def inject_data_into_html(nodes_data, edges_data):
    """Read static HTML file and inject data"""
    
    frontend_dir = Path('Frontend_Updated')
    html_file = frontend_dir / 'index.html'
    output_file = frontend_dir / 'graph_visualization.html'
    
    # Read the static HTML file
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Create data injection script
    data_script = f"""    <script id="graph-data">
        // Data injected by Python script
        const csvData = {json.dumps(df.to_csv(index=False))};
    </script>"""
    
    # Replace the placeholder data script
    html_content = html_content.replace(
        '''    <script id="graph-data">
        // Data will be injected here by Python script
        const csvData = '';
        const originalNodes = [];
        const originalLinks = [];
    </script>''',
        data_script
    )
    
    # Save the new HTML file with data
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return output_file

# --- MODIFIED FUNCTION ---
def create_interactive_html_graph():
    """Main function to create interactive graph and serve it locally"""
    
    print("🚀 Starting graph visualization creation...")
    print("✅ Frontend files found!")
    
    # Process CSV data
    print("📊 Processing CSV data...")
    nodes_data, edges_data = process_data()
    print(f"📈 Processed {len(nodes_data)} nodes and {len(edges_data)} edges")
    
    unique_relationships = set(edge['relationship'] for edge in edges_data)
    print(f"🔗 Found {len(unique_relationships)} unique relationship types:")
    for i, rel in enumerate(sorted(unique_relationships), 1):
        print(f"   {i}. {rel}")
    
    # Inject data into HTML
    print("💉 Injecting data into HTML...")
    output_file = inject_data_into_html(nodes_data, edges_data)
    
    # --- SERVER AND BROWSER LOGIC ---
    PORT = 8000
    DIRECTORY_TO_SERVE = "Frontend_Updated"
    FILE_TO_OPEN = output_file.name

    # Create a custom handler to serve files from the 'Frontend' directory
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DIRECTORY_TO_SERVE, **kwargs)

    httpd = socketserver.TCPServer(("", PORT), Handler)
    
    # Start the server in a background thread
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    # Give the server a moment to start up
    time.sleep(1)

    url = f"http://localhost:{PORT}/{FILE_TO_OPEN}"
    webbrowser.open(url)

    print(f"\n✅ Enhanced interactive graph created!")
    print("📁 File structure:")
    print("   Frontend/")
    print("   ├── index.html              (Static template)")
    print("   ├── graph_visualization.html (Generated with data)")
    print("   ├── css/")
    print("   │   └── styles.css          (Static styles)")
    print("   └── js/")
    print("       └── graph.js            (Static logic)")

    print(f"\n🌐 Server running at: {url}")
    print("   The script has started a local web server.")
    
    try:
        # Keep the main script alive to allow the server thread to run
        input("   Press Enter or close this terminal to stop the server...\n")
    except KeyboardInterrupt:
        print("\nCaught keyboard interrupt, shutting down.")
    finally:
        # Cleanly shut down the server
        print("🛑 Stopping server...")
        httpd.shutdown()
        httpd.server_close()
        
    return str(output_file)

def check_csv_structure():
    """Check if CSV has required columns"""
    
    required_columns = ['childTableName', 'parentTableName', 'childTableType', 'parentTableType']
    
    print("🔍 Checking CSV structure...")
    print(f"📋 Available columns: {list(df.columns)}")
    
    missing_required = [col for col in required_columns if col not in df.columns]
    if missing_required:
        print(f"❌ Missing required columns: {missing_required}")
        return False
    
    if 'relationship' in df.columns:
        print("✅ Relationship column found!")
        print(f"🔗 Unique relationships: {list(df['relationship'].unique())}")
    else:
        print("⚠️  No 'relationship' column found. Will use 'unknown' as default.")
    
    print(f"📊 Data shape: {df.shape}")
    return True

if __name__ == "__main__":
    if check_csv_structure():
        create_interactive_html_graph()
    else:
        print("❌ Please check your CSV file structure and try again.")