[![license](https://img.shields.io/github/license/falkordb/graph-exporter.svg)](https://github.com/falkordb/graph-exporter)
[![Release](https://img.shields.io/github/release/falkordb/graph-exporter.svg)](https://github.com/falkordb/graph-exporter/releases/latest)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/FalkorDB/graph-exporter/workflows/CI/badge.svg)](https://github.com/FalkorDB/graph-exporter/actions)
[![codecov](https://codecov.io/gh/FalkorDB/graph-exporter/branch/main/graph/badge.svg)](https://codecov.io/gh/FalkorDB/graph-exporter)
[![Forum](https://img.shields.io/badge/Forum-falkordb-blue)](https://github.com/orgs/FalkorDB/discussions)
[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.gg/6M4QwDXn2w)

# FalkorDB Graph Exporter

[![Try Free](https://img.shields.io/badge/Try%20Free-FalkorDB%20Cloud-FF8101?labelColor=FDE900&style=for-the-badge&link=https://app.falkordb.cloud)](https://app.falkordb.cloud)

A command-line tool to export FalkorDB graph data to CSV files. This tool connects to a FalkorDB instance and exports both nodes and edges to separate CSV files for analysis, backup, or data migration purposes.

## Features

- Export graph nodes to CSV with properties and labels
- Export graph edges/relationships to CSV with properties and types
- Connect to local or remote FalkorDB instances
- **Authentication support** with username and password
- **Pagination** for large graphs (handles unlimited nodes/edges)
- **Flexible output**: separate files per label/type or combined files
- Simple command-line interface
- Handles graph properties and metadata

## Installation

### Prerequisites

- Python 3.8+ 
- pipenv (recommended) or pip

### Method 1: Using pipenv (Recommended)

If you don't have pipenv installed:

```bash
pip install pipenv
```

Install and run:

```bash
# Clone the repository
git clone https://github.com/FalkorDB/graph-exporter.git
cd graph-exporter

# Install dependencies using pipenv
pipenv install

# Activate the virtual environment
pipenv shell
```

## Usage

### Basic Usage

Export a graph from a local FalkorDB instance:

```bash
python main.py <graph_name>
```

### Advanced Usage

Export from a remote FalkorDB instance:

```bash
python main.py <graph_name> --host <hostname> --port <port>
```

### Command Line Options

```
usage: main.py [-h] [--host HOST] [--port PORT] [--username USERNAME] 
               [--password PASSWORD] [--split-by-type] [--no-split-by-type] 
               graph_name

Export FalkorDB graph nodes and edges to CSV files by label/type.

positional arguments:
  graph_name            Name of the graph to export

options:
  -h, --help            show this help message and exit
  --host HOST           FalkorDB host (default: localhost)
  --port PORT           FalkorDB port (default: 6379)
  --username USERNAME   FalkorDB username (optional)
  --password PASSWORD   FalkorDB password (optional)
  --split-by-type       Create separate CSV files per node label and edge type (default)
  --no-split-by-type    Create single CSV files for all nodes and edges
```

### Examples

**Basic export** (separate files per label/type):
```bash
python main.py social
```

**Export from remote instance:**
```bash
python main.py social --host redis.example.com --port 6379
```

**Export with authentication:**
```bash
python main.py social --username myuser --password mypass
```

**Export to combined files** (single nodes.csv and edges.csv):
```bash
python main.py social --no-split-by-type
```

**Full example with all options:**
```bash
python main.py social --host db.example.com --port 6379 \
  --username admin --password secret --split-by-type
```

## Output

The tool can generate CSV files in two modes:

### Default Mode (--split-by-type)

Creates **separate files for each node label and edge type**:

#### Node files: `nodes_<label>.csv`
Each node label gets its own file (e.g., `nodes_Person.csv`, `nodes_Company.csv`):
- `id`: Node ID
- Additional columns for each node property

#### Edge files: `edges_<type>.csv`
Each edge type gets its own file (e.g., `edges_WORKS_AT.csv`, `edges_KNOWS.csv`):
- `id`: Edge ID
- `from_id`: Source node ID
- `to_id`: Target node ID
- Additional columns for each edge property

### Combined Mode (--no-split-by-type)

Creates **two combined files**:

#### nodes.csv
All nodes in a single file:
- `id`: Node ID
- `label`: Node label/type
- Additional columns for each node property

#### edges.csv  
All edges in a single file:
- `id`: Edge ID
- `type`: Relationship type
- `from_id`: Source node ID
- `to_id`: Target node ID
- Additional columns for each edge property

### Large Graph Support

The exporter uses **pagination** to handle graphs of any size. It fetches data in batches of 10,000 records and displays progress during export.

## Running FalkorDB

### Docker
```bash
docker run --rm -p 6379:6379 falkordb/falkordb
```

### FalkorDB Cloud
Use [FalkorDB Cloud](https://app.falkordb.cloud) for a managed FalkorDB instance.

## Example Workflow

1. Start FalkorDB:
   ```bash
   docker run --rm -p 6379:6379 falkordb/falkordb
   ```

2. Create some graph data using FalkorDB client or [FalkorDB Browser](https://browser.falkordb.com)

3. Export the graph:
   ```bash
   # Using pipenv
   pipenv shell
   python main.py my_graph
   ```

4. Find your exported data in `nodes.csv` and `edges.csv`

## Development

### Using pipenv for Development

```bash
# Install development dependencies (if any)
pipenv install --dev

# Add new dependencies
pipenv install <package_name>
```

### Testing

The project includes comprehensive tests that verify the export functionality with real FalkorDB instances.

#### Running Tests Locally

1. Start a FalkorDB instance:
   ```bash
   docker run --rm -p 6379:6379 falkordb/falkordb:latest
   ```

2. Install test dependencies:
   ```bash
   pipenv install --dev
   ```

3. Run the tests:
   ```bash
   pytest test_main.py -v
   ```

4. Run tests with coverage:
   ```bash
   pytest --cov=main --cov-report=term-missing test_main.py
   ```

#### CI/CD

The project uses GitHub Actions for continuous integration:
- Tests are run against multiple Python versions (3.8-3.12)
- FalkorDB is started as a service container
- Coverage reports are uploaded to Codecov
- All tests must pass before merging PRs

### Dependencies

- **falkordb**: Python client for FalkorDB
- **pandas**: Data manipulation and CSV export

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://docs.falkordb.com)
- 💬 [Community Forum](https://github.com/orgs/FalkorDB/discussions) 
- 🐛 [Report Issues](https://github.com/FalkorDB/graph-exporter/issues)
- 💬 [Discord Community](https://discord.gg/6M4QwDXn2w)
