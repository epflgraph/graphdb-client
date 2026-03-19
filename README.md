<img src="assets/icon.png" alt="Project logo" height="64">

[![License](https://img.shields.io/github/license/epflgraph/graphdb-client)](https://github.com/epflgraph/graphdb-client/blob/master/LICENSE)
[![Latest Release on Github](https://img.shields.io/github/v/release/epflgraph/graphdb-client?sort=semver)](https://github.com/epflgraph/graphdb-client/releases/latest)
[![GitHub Stars](https://img.shields.io/github/stars/epflgraph/graphdb-client?style=social)](https://github.com/epflgraph/graphdb-client/stargazers)
[![Contributors](https://img.shields.io/github/contributors/epflgraph/graphdb-client)](https://github.com/epflgraph/graphdb-client/graphs/contributors)
[![Last Commit](https://img.shields.io/github/last-commit/epflgraph/graphdb-client)](https://github.com/epflgraph/graphdb-client/commits/master)
[![Open Issues](https://img.shields.io/github/issues/epflgraph/graphdb-client)](https://github.com/epflgraph/graphdb-client/issues)
[![Open PRs](https://img.shields.io/github/issues-pr/epflgraph/graphdb-client)](https://github.com/epflgraph/graphdb-client/pulls)

Why Graph?
==========
The *Graph Data Platform* - developed by the AI engineering team at the [EPFL Center for Digital Education](https://www.epfl.ch/education/educational-initiatives/cede/) - is an open-source alternative to proprietary research information systems like Elsevier Pure. It federates educational and institutional data into a semantically interconnected knowledge graph of people, publications, labs, startups, courses, video lectures, and other educational resources. The [GraphSearch](https://graphsearch.epfl.ch/en) application provides lightning-fast search and discovery of the knowledge graph, as well as LLM-powered [chatbot](https://graphsearch.epfl.ch/en/chatbot) interaction with the indexed resources.

**List of Graph services:**<br/>
 [Registry](https://github.com/epflgraph/graphregistry/)  |
       [AI](https://github.com/epflgraph/graphai/)        |
 [Ontology](https://github.com/epflgraph/graphontology/)  |
   [Search](https://github.com/epflgraph/graphsearch_ui/) |
     [Chat](https://github.com/epflgraph/graphchatbot/)   |
     [Dash](https://github.com/epflgraph/graphdashboard/) |
 DB client                                                |
[ES client](https://github.com/epflgraph/graphes-client/)

Graph DB Client
===============
*Graph DB Client* is a Python-based command-line interface (CLI) tool designed to facilitate the management and interaction with the Graph Data Platform's underlying MySQL database. It provides a unified interface for performing various database operations, including configuration management, data import/export, and server administration tasks. The CLI is built using Python's `argparse` library, allowing users to execute commands in a structured and intuitive manner.

Configuration
=============
The CLI expects a `config.yaml` file (repository root format) describing database environments and MySQL binaries. Use the provided `config.example.yaml` as a template to create your own configuration.

Installation
============

### 🐳 Deploy with Docker
The Graph DB Client is available as a Docker image, which provides a convenient way to run the CLI without needing to set up a local Python environment. The image includes all necessary dependencies and can be easily updated by pulling the latest version from Docker Hub.

Steps to deploy with Docker:

1. Pull the image:
    ```bash
    docker pull epflgraph/graphdb-client:latest
    ```

2. Run the CLI help:
    ```bash
    docker run --rm epflgraph/graphdb-client:latest -h
    ```

3. Run with your local configuration mounted (recommended):
    ```bash
    docker run --rm \
    -v "$(pwd)/config.yaml:/app/config.yaml:ro" \
    epflgraph/graphdb-client:latest test --env <env_name>
    ```

To run commands as `graphdb [cmd]`, add this to your `~/.zshrc` file:
```
graphdb() {
  docker run --rm \
    -v "$PWD/config.yaml:/app/config.yaml:ro" \
    epflgraph/graphdb-client:latest "$@"
}
```
Then reload your shell:
```bash
source ~/.zshrc
```
Test with:
```bash
graphdb test --env <env_name>
```

### 👨🏻‍💻 Local installation
For users who prefer to run the CLI directly on their local machine, follow these steps to set up a Python virtual environment and install the package:

1. Clone the repository:
   ```bash
   git clone https://github.com/epflgraph/graphdb-client.git
   cd graphdb-client
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv.graphdb
   source .venv.graphdb/bin/activate
   ```

3. Install the package:
   ```bash
   pip install .
   ```

4. Verify installation:
   ```bash
   graphdb -h
   ```

5. To test the connection to a database environment:
    ```bash
    graphdb test --env <env_name>
    ```
