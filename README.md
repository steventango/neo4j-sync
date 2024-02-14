# neo4j-sync

This is a simple tool to sync data from one neo4j database to another. It only
uses plain Cypher queries to do so, so it is useful for syncing data from source
databases that are not directly accessible through the command line and don't
have the apoc plugin installed.


## Usage

```bash
pip install -r requirements.txt
python neo4j-sync --source-uri bolt://localhost:7687 --source-user neo4j --source-password neo4j --target-uri bolt://localhost:7687 --target-user neo4j --target-password neo4j
python neo4j-sync --from-uri neo4j+ssc://disease.ncats.io:7687 --target-uri neo4j://neo4j:7687 --from-user "" --from-password "" --target-user neo4j --target-password "12345678"
```

Alternatively, you can use docker to run the tool:

```bash
docker build -t neo4j-sync .
docker run -it --rm neo4j-sync python --source-uri bolt://localhost:7687 --source-user neo4j --source-password neo4j --target-uri bolt://localhost:7687 --target-user neo4j --target-password neo4j
```
