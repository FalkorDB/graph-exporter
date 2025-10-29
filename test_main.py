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
        """Test that export_graph creates separate CSV files for each label and edge type."""
        export_graph(test_graph, "localhost", 6379)
        
        # Check that label-specific CSV files were created
        assert os.path.exists("nodes_Person.csv"), "nodes_Person.csv file should be created"
        assert os.path.exists("nodes_Company.csv"), "nodes_Company.csv file should be created"
        
        # Check that edge-type-specific CSV files were created
        assert os.path.exists("edges_WORKS_FOR.csv"), "edges_WORKS_FOR.csv file should be created"
        assert os.path.exists("edges_KNOWS.csv"), "edges_KNOWS.csv file should be created"
    
    def test_export_nodes_content(self, test_graph, temp_dir):
        """Test that exported nodes contain expected data in separate files by label."""
        export_graph(test_graph, "localhost", 6379)
        
        # Read the Person nodes CSV
        person_nodes_df = pd.read_csv("nodes_Person.csv")
        
        # Check that we have the expected number of Person nodes
        assert len(person_nodes_df) == 2, f"Expected 2 Person nodes, got {len(person_nodes_df)}"
        
        # Check that Person nodes have required columns
        required_columns = ["id"]
        for col in required_columns:
            assert col in person_nodes_df.columns, f"Column '{col}' should be in nodes_Person.csv"
        
        # Check that Person nodes have name and age properties
        assert "name" in person_nodes_df.columns, "Should have name property for Person nodes"
        assert "age" in person_nodes_df.columns, "Should have age property for Person nodes"
        
        # Check specific values
        names = set(person_nodes_df["name"].tolist())
        assert "Alice" in names, "Should have Alice as a person"
        assert "Bob" in names, "Should have Bob as a person"
        
        # Read the Company nodes CSV
        company_nodes_df = pd.read_csv("nodes_Company.csv")
        
        # Check that we have the expected number of Company nodes
        assert len(company_nodes_df) == 1, f"Expected 1 Company node, got {len(company_nodes_df)}"
        
        # Check that Company node has name and founded properties
        assert "name" in company_nodes_df.columns, "Should have name property for Company nodes"
        assert "founded" in company_nodes_df.columns, "Should have founded property for Company nodes"
        
        # Check specific values
        company_names = set(company_nodes_df["name"].tolist())
        assert "TechCorp" in company_names, "Should have TechCorp as a company"
    
    def test_export_edges_content(self, test_graph, temp_dir):
        """Test that exported edges contain expected data in separate files by type."""
        export_graph(test_graph, "localhost", 6379)
        
        # Read the WORKS_FOR edges CSV
        works_for_df = pd.read_csv("edges_WORKS_FOR.csv")
        
        # Check that we have the expected number of WORKS_FOR edges
        assert len(works_for_df) == 1, f"Expected 1 WORKS_FOR edge, got {len(works_for_df)}"
        
        # Check that WORKS_FOR edges have required columns
        required_columns = ["id", "from_id", "to_id"]
        for col in required_columns:
            assert col in works_for_df.columns, f"Column '{col}' should be in edges_WORKS_FOR.csv"
        
        # Check that edge properties are included
        assert "since" in works_for_df.columns, "Should have since property for WORKS_FOR edges"
        
        # Read the KNOWS edges CSV
        knows_df = pd.read_csv("edges_KNOWS.csv")
        
        # Check that we have the expected number of KNOWS edges
        assert len(knows_df) == 1, f"Expected 1 KNOWS edge, got {len(knows_df)}"
        
        # Check that KNOWS edges have required columns
        for col in required_columns:
            assert col in knows_df.columns, f"Column '{col}' should be in edges_KNOWS.csv"
        
        # Check that edge properties are included
        assert "since" in knows_df.columns, "Should have since property for KNOWS edges"
    
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
        
        # For an empty graph, no CSV files should be created since there are no nodes or edges
        # The new implementation only creates files when there's data to export
        
        # Get list of CSV files in the current directory
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        
        # For empty graph, no CSV files should be created
        assert len(csv_files) == 0, f"Empty graph should not create any CSV files, but found: {csv_files}"
    
    def test_export_nodes_with_multiple_labels(self, falkordb_connection, temp_dir):
        """Test export behavior when nodes have multiple labels."""
        # Create a test graph with multi-labeled nodes
        multi_label_graph_name = "multi_label_test_graph"
        g = falkordb_connection.select_graph(multi_label_graph_name)
        
        # Clear any existing data
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
        
        # Create a node with multiple labels
        g.query("CREATE (:Person:Employee {name: 'Charlie', age: 35, department: 'Engineering'})")
        
        export_graph(multi_label_graph_name, "localhost", 6379)
        
        # The node should appear in both Person and Employee CSV files
        assert os.path.exists("nodes_Person.csv"), "nodes_Person.csv should be created"
        assert os.path.exists("nodes_Employee.csv"), "nodes_Employee.csv should be created"
        
        # Read both CSV files
        person_df = pd.read_csv("nodes_Person.csv")
        employee_df = pd.read_csv("nodes_Employee.csv")
        
        # Both should have the same node
        assert len(person_df) == 1, "Should have 1 Person node"
        assert len(employee_df) == 1, "Should have 1 Employee node"
        
        # Both should have the same data
        assert person_df.at[0, "name"] == "Charlie"
        assert employee_df.at[0, "name"] == "Charlie"
        assert person_df.at[0, "age"] == 35
        assert employee_df.at[0, "age"] == 35
        
        # Cleanup
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
    
    def test_export_unlabeled_nodes(self, falkordb_connection, temp_dir):
        """Test export behavior with unlabeled nodes."""
        # Create a test graph with unlabeled nodes
        unlabeled_graph_name = "unlabeled_test_graph"
        g = falkordb_connection.select_graph(unlabeled_graph_name)
        
        # Clear any existing data
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
        
        # Create an unlabeled node
        g.query("CREATE ({name: 'Unlabeled Node', value: 42})")
        
        export_graph(unlabeled_graph_name, "localhost", 6379)
        
        # Should create a file for unlabeled nodes
        assert os.path.exists("nodes_unlabeled.csv"), "nodes_unlabeled.csv should be created"
        
        # Read the CSV file
        unlabeled_df = pd.read_csv("nodes_unlabeled.csv")
        
        # Should have one unlabeled node
        assert len(unlabeled_df) == 1, "Should have 1 unlabeled node"
        assert unlabeled_df.at[0, "name"] == "Unlabeled Node"
        assert unlabeled_df.at[0, "value"] == 42
        
        # Cleanup
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
    
    def test_export_connection_error(self, temp_dir):
        """Test behavior when FalkorDB connection fails."""
        # Test with localhost on a port that's definitely not listening
        # This should cause a connection refused error
        with pytest.raises(Exception):
            export_graph("test_graph", "localhost", 1)
    
    def test_export_with_auth_params(self, test_graph, temp_dir):
        """Test that export_graph accepts username and password parameters."""
        # Test that function accepts auth parameters (even if server doesn't require them)
        export_graph(test_graph, "localhost", 6379, username=None, password=None)
        
        # Should create the expected files
        assert os.path.exists("nodes_Person.csv"), "nodes_Person.csv should be created"
        assert os.path.exists("edges_WORKS_FOR.csv"), "edges_WORKS_FOR.csv should be created"
    
    def test_export_combined_mode(self, test_graph, temp_dir):
        """Test export with split_by_type=False creates combined files."""
        export_graph(test_graph, "localhost", 6379, split_by_type=False)
        
        # Check that combined CSV files were created
        assert os.path.exists("nodes.csv"), "nodes.csv file should be created in combined mode"
        assert os.path.exists("edges.csv"), "edges.csv file should be created in combined mode"
        
        # Verify nodes.csv has label column
        nodes_df = pd.read_csv("nodes.csv")
        assert "label" in nodes_df.columns, "nodes.csv should have label column"
        assert len(nodes_df) >= 3, "Should have at least 3 nodes (2 Person + 1 Company)"
        
        # Verify edges.csv has type column
        edges_df = pd.read_csv("edges.csv")
        assert "type" in edges_df.columns, "edges.csv should have type column"
        assert len(edges_df) >= 2, "Should have at least 2 edges (WORKS_FOR + KNOWS)"
    
    def test_export_split_mode_default(self, test_graph, temp_dir):
        """Test that split_by_type=True is the default behavior."""
        # Call without split_by_type parameter (should default to True)
        export_graph(test_graph, "localhost", 6379)
        
        # Should create separate files per label/type (default behavior)
        assert os.path.exists("nodes_Person.csv"), "Should create nodes_Person.csv by default"
        assert os.path.exists("nodes_Company.csv"), "Should create nodes_Company.csv by default"
        assert os.path.exists("edges_WORKS_FOR.csv"), "Should create edges_WORKS_FOR.csv by default"
        
        # Should NOT create combined files
        assert not os.path.exists("nodes.csv"), "Should not create nodes.csv in split mode"
        assert not os.path.exists("edges.csv"), "Should not create edges.csv in split mode"
    
    def test_export_pagination_large_graph(self, falkordb_connection, temp_dir):
        """Test that pagination works with larger datasets."""
        # Create a graph with more nodes than would fit in a single batch
        large_graph_name = "large_test_graph"
        g = falkordb_connection.select_graph(large_graph_name)
        
        # Clear any existing data
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
        
        # Create 100 nodes (simulating pagination, though batch size is 10000)
        g.query("UNWIND range(1, 100) AS i CREATE (:TestNode {id: i, name: 'Node' + toString(i)})")
        
        # Create some edges
        g.query("MATCH (a:TestNode), (b:TestNode) WHERE a.id < b.id AND b.id - a.id = 1 CREATE (a)-[:NEXT]->(b)")
        
        export_graph(large_graph_name, "localhost", 6379)
        
        # Verify all nodes were exported
        assert os.path.exists("nodes_TestNode.csv"), "nodes_TestNode.csv should be created"
        nodes_df = pd.read_csv("nodes_TestNode.csv")
        assert len(nodes_df) == 100, f"Should have 100 nodes, got {len(nodes_df)}"
        
        # Verify edges were exported
        assert os.path.exists("edges_NEXT.csv"), "edges_NEXT.csv should be created"
        edges_df = pd.read_csv("edges_NEXT.csv")
        assert len(edges_df) == 99, f"Should have 99 edges, got {len(edges_df)}"
        
        # Cleanup
        try:
            g.query("MATCH (n) DETACH DELETE n")
        except:
            pass
