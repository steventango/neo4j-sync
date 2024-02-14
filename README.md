# neo4j-sync

This is a simple tool to sync data from one neo4j database to another. It only
uses plain Cypher queries to do so, so it is useful for syncing data from source
databases that are not directly accessible through the command line and don't
have the apoc plugin installed.


## Usage

```bash
pip install -r requirements.txt
python neo4j-sync.py --from-uri neo4j+ssc://disease.ncats.io:7687 --to-uri neo4j://localhost:7687 --to-user neo4j --to-password neo4j
```

Alternatively, you can use docker to run the tool:

```bash
docker build -t neo4j-sync .
docker run -it --rm neo4j-sync python neo4j-sync.py --from-uri neo4j+ssc://disease.ncats.io:7687 --to-uri neo4j://localhost:7687 --to-user neo4j --to-password neo4j
```
