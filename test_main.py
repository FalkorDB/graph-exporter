import pytest
import os
import pandas as pd
from falkordb import FalkorDB
from main import export_graph
import tempfile
import shutil


class TestGraphExporter:
    """Test suite for the graph exporter functionality."""
    
    @pytest.fixture(scope="class")
    def falkordb_connection(self):
        """Set up FalkorDB connection for testing."""
        # Connect to FalkorDB (assumes it's running on localhost:6379)
        db = FalkorDB(host="localhost", port=6379)
        return db
    
    @pytest.fixture(scope="class")
    def test_graph(self, falkordb_connection):
        """Create a test graph with sample data."""
        graph_name = "test_graph"
        g = falkordb_connection.select_graph(graph_name)
        
        # Clear any existing data
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass  # Graph might not exist yet
        
        # Create test nodes
        g.query("CREATE (:Person {name: 'Alice', age: 30})")
        g.query("CREATE (:Person {name: 'Bob', age: 25})")
        g.query("CREATE (:Company {name: 'TechCorp', founded: 2010})")
        
        # Create test relationships
        g.query("MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) CREATE (a)-[:WORKS_FOR {since: 2020}]->(c)")
        g.query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2015}]->(b)")
        
        yield graph_name
        
        # Cleanup: delete the test graph
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        temp_dir = tempfile.mkdtemp()
        original_dir = os.getcwd()
        os.chdir(temp_dir)
        
        yield temp_dir
        
        # Cleanup
        os.chdir(original_dir)
        shutil.rmtree(temp_dir)
    
    def test_export_graph_creates_csv_files(self, test_graph, temp_dir):
        """Test that export_graph creates nodes.csv and edges.csv files."""
        export_graph(test_graph, "localhost", 6379)
        
        # Check that CSV files were created
        assert os.path.exists("nodes.csv"), "nodes.csv file should be created"
        assert os.path.exists("edges.csv"), "edges.csv file should be created"
    
    def test_export_nodes_content(self, test_graph, temp_dir):
        """Test that exported nodes contain expected data."""
        export_graph(test_graph, "localhost", 6379)
        
        # Read the nodes CSV
        nodes_df = pd.read_csv("nodes.csv")
        
        # Check that we have the expected number of nodes
        assert len(nodes_df) == 3, f"Expected 3 nodes, got {len(nodes_df)}"
        
        # Check that nodes have required columns
        required_columns = ["id", "label"]
        for col in required_columns:
            assert col in nodes_df.columns, f"Column '{col}' should be in nodes.csv"
        
        # Check that we have both Person and Company labels
        labels = set(nodes_df["label"].tolist())
        assert "Person" in labels, "Should have Person nodes"
        assert "Company" in labels, "Should have Company nodes"
        
        # Check that Person nodes have name and age properties
        person_nodes = nodes_df[nodes_df["label"] == "Person"]
        assert len(person_nodes) == 2, "Should have 2 Person nodes"
        assert "name" in nodes_df.columns, "Should have name property for Person nodes"
        assert "age" in nodes_df.columns, "Should have age property for Person nodes"
        
        # Check specific values
        names = set(person_nodes["name"].tolist())
        assert "Alice" in names, "Should have Alice as a person"
        assert "Bob" in names, "Should have Bob as a person"
    
    def test_export_edges_content(self, test_graph, temp_dir):
        """Test that exported edges contain expected data."""
        export_graph(test_graph, "localhost", 6379)
        
        # Read the edges CSV
        edges_df = pd.read_csv("edges.csv")
        
        # Check that we have the expected number of edges
        assert len(edges_df) == 2, f"Expected 2 edges, got {len(edges_df)}"
        
        # Check that edges have required columns
        required_columns = ["id", "type", "from_id", "to_id"]
        for col in required_columns:
            assert col in edges_df.columns, f"Column '{col}' should be in edges.csv"
        
        # Check that we have the expected relationship types
        types = set(edges_df["type"].tolist())
        assert "WORKS_FOR" in types, "Should have WORKS_FOR relationship"
        assert "KNOWS" in types, "Should have KNOWS relationship"
        
        # Check that edge properties are included
        works_for_edges = edges_df[edges_df["type"] == "WORKS_FOR"]
        assert len(works_for_edges) == 1, "Should have 1 WORKS_FOR edge"
        assert "since" in edges_df.columns, "Should have since property for edges"
    
    def test_export_with_empty_graph(self, falkordb_connection, temp_dir):
        """Test export behavior with an empty graph."""
        # Create an empty test graph
        empty_graph_name = "empty_test_graph"
        g = falkordb_connection.select_graph(empty_graph_name)
        
        # Clear any existing data
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
        
        export_graph(empty_graph_name, "localhost", 6379)
        
        # Check that CSV files are created even for empty graph
        assert os.path.exists("nodes.csv"), "nodes.csv should be created even for empty graph"
        assert os.path.exists("edges.csv"), "edges.csv should be created even for empty graph"
        
        # Read and verify empty CSVs
        # For empty files, pandas may create files with no columns, so we handle that case
        try:
            nodes_df = pd.read_csv("nodes.csv")
            assert len(nodes_df) == 0, "Empty graph should produce empty nodes.csv"
        except pd.errors.EmptyDataError:
            # This is expected for completely empty CSV files
            pass
            
        try:
            edges_df = pd.read_csv("edges.csv")
            assert len(edges_df) == 0, "Empty graph should produce empty edges.csv"
        except pd.errors.EmptyDataError:
            # This is expected for completely empty CSV files
            pass
    
    def test_export_connection_error(self):
        """Test behavior when FalkorDB connection fails."""
        with pytest.raises(Exception):
            # Try to connect to non-existent server
            export_graph("test_graph", "nonexistent_host", 9999)