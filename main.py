import argparse
from falkordb import FalkorDB
import pandas as pd

def export_graph(graph_name, host, port):
    # Connect to FalkorDB
    db = FalkorDB(host=host, port=port)
    g = db.select_graph(graph_name)

    # Export Nodes
    nodes_result = g.ro_query("MATCH (n) RETURN ID(n), labels(n), properties(n)")
    nodes = []
    for record in nodes_result.result_set:
        node_id = record[0]
        labels = record[1]
        props = record[2] or {}
        node = {"id": node_id, "label": labels[0] if labels else ""}
        node.update(props)
        nodes.append(node)

    pd.DataFrame(nodes).to_csv("nodes.csv", index=False)
    print("✅ Exported nodes to nodes.csv")

    # Export Edges
    edges_result = g.ro_query("MATCH (a)-[e]->(b) RETURN ID(e), TYPE(e), ID(a), ID(b), properties(e)")
    edges = []
    for record in edges_result.result_set:
        edge_id = record[0]
        edge_type = record[1]
        from_id = record[2]
        to_id = record[3]
        props = record[4] or {}
        edge = {
            "id": edge_id,
            "type": edge_type,
            "from_id": from_id,
            "to_id": to_id
        }
        edge.update(props)
        edges.append(edge)

    pd.DataFrame(edges).to_csv("edges.csv", index=False)
    print("✅ Exported edges to edges.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export FalkorDB graph nodes and edges to CSV.")
    parser.add_argument("graph_name", help="Name of the graph to export")
    parser.add_argument("--host", default="localhost", help="FalkorDB host (default: localhost)")
    parser.add_argument("--port", type=int, default=6379, help="FalkorDB port (default: 6379)")

    args = parser.parse_args()

    export_graph(args.graph_name, args.host, args.port)

