# neo4j-sync

This is a simple tool to sync data from one neo4j database to another. It only
uses plain Cypher queries to do so, so it is useful for syncing data from source
databases that are not directly accessible through the command line and don't
have the apoc plugin installed.


## Usage

```bash
pip install -r requirements.txt
python neo4j-sync --source-uri bolt://localhost:7687 --source-user neo4j --source-password neo4j --target-uri bolt://localhost:7687 --target-user neo4j --target-password neo4j
```

Alternatively, you can use docker to run the tool:

```bash
docker run -it --rm -e SOURCE_URI=bolt://localhost:7687 -e SOURCE_USER=neo4j -e SOURCE_PASSWORD=neo4j -e TARGET_URI=bolt://localhost:7687 -e TARGET_USER=neo4j -e TARGET_PASSWORD=neo4j neo4j-sync
```
