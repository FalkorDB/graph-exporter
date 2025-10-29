import argparse
from collections import defaultdict

from falkordb import FalkorDB
import pandas as pd

def export_graph(graph_name, host, port, username=None, password=None, split_by_type=True):
    # Connect to FalkorDB using URL
    # Build connection URL: redis://[username:password@]host:port
    if username and password:
        url = f"redis://{username}:{password}@{host}:{port}"
    elif username:
        url = f"redis://{username}@{host}:{port}"
    else:
        url = f"redis://{host}:{port}"
    
    db = FalkorDB(url=url)
    g = db.select_graph(graph_name)

    # Export Nodes by Label
    print("🔄 Fetching nodes...")
    last_id = -1
    batch_size = 10000
    nodes_by_label = defaultdict(list)
    total_nodes = 0

    while True:
        # Use ID-based pagination for better performance
        nodes_result = g.ro_query(
            f"MATCH (n) WHERE ID(n) > {last_id} RETURN ID(n), labels(n), properties(n) ORDER BY ID(n) LIMIT {batch_size}"
        )
        
        if not nodes_result.result_set:
            break
        
        for record in nodes_result.result_set:
            node_id = record[0]
            labels = record[1]
            props = record[2] or {}

            # Handle nodes with multiple labels or no labels
            if labels:
                for label in labels:
                    node = {"id": node_id}
                    node.update(props)
                    nodes_by_label[label].append(node)
            else:
                # Handle nodes without labels
                node = {"id": node_id}
                node.update(props)
                nodes_by_label["unlabeled"].append(node)
            
            # Track last ID for next iteration
            last_id = node_id
        
        total_nodes += len(nodes_result.result_set)
        print(f"  Fetched {total_nodes} nodes...")
        
        if len(nodes_result.result_set) < batch_size:
            break

    # Export nodes
    if split_by_type:
        # Export each node label to its own CSV file
        for label, nodes in nodes_by_label.items():
            filename = f"nodes_{label}.csv"
            pd.DataFrame(nodes).to_csv(filename, index=False)
            print(f"✅ Exported {len(nodes)} nodes with label '{label}' to {filename}")
    else:
        # Export all nodes to a single CSV file with a label column
        all_nodes = []
        for label, nodes in nodes_by_label.items():
            for node in nodes:
                node["label"] = label
                all_nodes.append(node)
        if all_nodes:
            pd.DataFrame(all_nodes).to_csv("nodes.csv", index=False)
            print(f"✅ Exported {len(all_nodes)} nodes to nodes.csv")

    # Export Edges by Type
    print("\n🔄 Fetching edges...")
    last_id = -1
    batch_size = 10000
    edges_by_type = defaultdict(list)
    total_edges = 0

    while True:
        # Use ID-based pagination for better performance
        edges_result = g.ro_query(
            f"MATCH (a)-[e]->(b) WHERE ID(e) > {last_id} RETURN ID(e), TYPE(e), ID(a), ID(b), properties(e) ORDER BY ID(e) LIMIT {batch_size}"
        )
        
        if not edges_result.result_set:
            break
        
        for record in edges_result.result_set:
            edge_id = record[0]
            edge_type = record[1]
            from_id = record[2]
            to_id = record[3]
            props = record[4] or {}

            edge = {
                "id": edge_id,
                "from_id": from_id,
                "to_id": to_id
            }
            edge.update(props)
            edges_by_type[edge_type].append(edge)
            
            # Track last ID for next iteration
            last_id = edge_id
        
        total_edges += len(edges_result.result_set)
        print(f"  Fetched {total_edges} edges...")
        
        if len(edges_result.result_set) < batch_size:
            break

    # Export edges
    if split_by_type:
        # Export each edge type to its own CSV file
        for edge_type, edges in edges_by_type.items():
            filename = f"edges_{edge_type}.csv"
            pd.DataFrame(edges).to_csv(filename, index=False)
            print(f"✅ Exported {len(edges)} edges of type '{edge_type}' to {filename}")
    else:
        # Export all edges to a single CSV file with a type column
        all_edges = []
        for edge_type, edges in edges_by_type.items():
            for edge in edges:
                edge["type"] = edge_type
                all_edges.append(edge)
        if all_edges:
            pd.DataFrame(all_edges).to_csv("edges.csv", index=False)
            print(f"✅ Exported {len(all_edges)} edges to edges.csv")

    # Print summary
    print("\n📊 Summary:")
    print(f"   Node labels exported: {len(nodes_by_label)}")
    print(f"   Edge types exported: {len(edges_by_type)}")

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Export FalkorDB graph nodes and edges to CSV files by label/type."
    )
    parser.add_argument("graph_name", help="Name of the graph to export")
    parser.add_argument("--host", default="localhost", help="FalkorDB host (default: localhost)")
    parser.add_argument("--port", type=int, default=6379, help="FalkorDB port (default: 6379)")
    parser.add_argument("--username", help="FalkorDB username (optional)")
    parser.add_argument("--password", help="FalkorDB password (optional)")
    parser.add_argument(
        "--split-by-type",
        action="store_true",
        default=True,
        help="Create separate CSV files per node label and edge type (default: True)"
    )
    parser.add_argument(
        "--no-split-by-type",
        dest="split_by_type",
        action="store_false",
        help="Create single CSV files for all nodes and edges"
    )

    args = parser.parse_args()

    export_graph(args.graph_name, args.host, args.port, args.username, args.password, args.split_by_type)


if __name__ == "__main__":
    main()
