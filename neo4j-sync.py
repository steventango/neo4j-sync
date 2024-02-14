import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

from neo4j import GraphDatabase, RoutingControl

LOG_PATH = Path(f"logs/{Path(__file__).stem}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")


logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler(LOG_PATH),
    ],
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()


def write_node(driver, node):
    n = node["n"]
    labels = ":".join(f"`{label}`" for label in n.labels)
    properties = "{" + ", ".join(f"`{key}`: {json.dumps(value)}" for key, value in n.items()) + "}"
    query = f"MERGE (n:{labels} {properties})"
    logger.debug(query)
    driver.execute_query(
        query,
        database_="neo4j",
        routing_=RoutingControl.WRITE,
    )


def write_nodes(driver, nodes):
    for node in nodes:
        write_node(driver, node)


def write_relationship(driver, relationship):
    r = relationship["r"]
    properties = "{" + ", ".join(f"`{key}`: {json.dumps(value)}" for key, value in r.items()) + "}"
    query = f"""
        MATCH (n) WHERE ID(n) = {r.start_node.element_id}
        MATCH (m) WHERE ID(m) = {r.end_node.element_id}
        MERGE (n)-[r:{r.type} {properties}]->(m)
    """
    logger.debug(query)
    driver.execute_query(
        query,
        database_="neo4j",
        routing_=RoutingControl.WRITE,
    )


def write_relationships(driver, relationships):
    for relationship in relationships:
        write_relationship(driver, relationship)


def read_nodes(driver, limit: int = -1):
    query = """
        MATCH (n)
        RETURN n
    """
    if limit > 0:
        query += f"""
        LIMIT {limit}
        """
    logger.debug(query)
    records, _, _ = driver.execute_query(
        query,
        database_="neo4j",
        routing_=RoutingControl.READ,
    )
    return records


def read_relationships(driver, limit: int = -1):
    # TODO DUMB, should split into batches
    # OOM
    query = """
        MATCH (n)-[r]->(m)
        RETURN r
    """
    if limit > 0:
        query += f"""
        LIMIT {limit}
        """
    logger.debug(query)
    records, _, _ = driver.execute_query(
        query,
        database_="neo4j",
        routing_=RoutingControl.READ,
    )
    return records


def main():
    parser = argparse.ArgumentParser(description="Sync Neo4j databases")
    parser.add_argument(
        "--from-uri",
        type=str,
        required=True,
        help="URI of the source Neo4j database",
    )
    parser.add_argument(
        "--to-uri",
        type=str,
        required=True,
        help="URI of the destination Neo4j database",
    )
    parser.add_argument(
        "--from-user",
        type=str,
        default="",
        help="Username of the source Neo4j database",
    )
    parser.add_argument(
        "--from-password",
        type=str,
        default="",
        help="Password of the source Neo4j database",
    )
    parser.add_argument(
        "--to-user",
        type=str,
        default="",
        help="Username of the destination Neo4j database",
    )
    parser.add_argument(
        "--to-password",
        type=str,
        default="",
        help="Password of the destination Neo4j database",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Limit the number of nodes and relationships to sync",
    )
    args = parser.parse_args()

    with GraphDatabase.driver(args.from_uri, auth=(args.from_user, args.from_password)) as from_driver, GraphDatabase.driver(args.to_uri, auth=(args.to_user, args.to_password)) as to_driver:
        logging.info("Reading nodes from disease.ncats.io")
        start = time.time()
        nodes = read_nodes(from_driver)
        end = time.time()
        logging.info(f"Time to read nodes from disease.ncats.io: {timedelta(seconds=end - start)}")
        logging.info("Writing nodes to local")
        start = time.time()
        write_nodes(to_driver, nodes)
        end = time.time()
        logging.info(f"Time to write nodes to local: {timedelta(seconds=end - start)}")
        logging.info("Reading relationships from disease.ncats.io")
        start = time.time()
        relationships = read_relationships(from_driver)
        end = time.time()
        logging.info(f"Time to read relationships from disease.ncats.io: {timedelta(seconds=end - start)}")
        logging.info("Writing relationships to local")
        start = time.time()
        write_relationships(to_driver, relationships)
        end = time.time()
        logging.info(f"Time to write relationships to local: {timedelta(seconds=end - start)}")


if __name__ == "__main__":
    main()
