import argparse
import asyncio
import json
import logging
import os
import time
from asyncio import Semaphore
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from neo4j import AsyncDriver, AsyncGraphDatabase, RoutingControl

LOG_PATH = Path(f"logs/{Path(__file__).stem}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_PATH),
    ],
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()


async def sync_nodes(
    from_driver: AsyncDriver,
    from_database: str,
    to_driver: AsyncDriver,
    to_database: str,
    limit: int = -1,
    batch_size: int = 1000,
    start: int = 0,
):
    if limit == -1:
        from_query = "MATCH (n) RETURN count(n) as count"
        logger.info(from_query)
        from_results, _, _ = await from_driver.execute_query(
            from_query, database=from_database, routing=RoutingControl.READ
        )
        limit = from_results[0]["count"]
    logger.info(f"Number of nodes to sync: {limit}")

    from_queries = [
        dedent(
            f"""
                MATCH (n)
                RETURN n
                SKIP {i}
                LIMIT {min(batch_size, limit - i)}
            """
        )
        for i in range(start, limit, batch_size)
    ]
    semaphore = Semaphore(os.cpu_count())
    for from_query in from_queries:
        logger.info(from_query)
        from_results, _, _ = await from_driver.execute_query(
            from_query, database=from_database, routing=RoutingControl.READ
        )
        coroutines = [create_node(to_driver, to_database, from_result, semaphore) for from_result in from_results]
        await asyncio.gather(*coroutines)


async def create_node(to_driver: AsyncDriver, to_database: str, from_result: dict, semaphore: Semaphore):
    async with semaphore:
        n = from_result["n"]
        labels = n.labels + ["_neo4j_sync_node"]
        labels = ":".join(f"`{label}`" for label in labels)
        properties = dict(n.items())
        properties["_neo4j_sync_from_id"] = n.element_id
        properties = "{" + ", ".join(f"`{key}`: {json.dumps(value)}" for key, value in properties.items()) + "}"
        to_query = f"CREATE (n:{labels} {properties})"
        logger.info(to_query)
        await to_driver.execute_query(to_query, database=to_database, routing=RoutingControl.WRITE)


async def sync_relationships(
    from_driver: AsyncDriver,
    from_database: str,
    to_driver: AsyncDriver,
    to_database: str,
    limit: int = -1,
    batch_size: int = 1000,
    start: int = 0,
):
    if limit == -1:
        from_query = "MATCH (n)-[r]->(m) RETURN count(r) as count"
        logger.info(from_query)
        from_results, _, _ = await from_driver.execute_query(
            from_query, database=from_database, routing=RoutingControl.READ
        )
        limit = from_results[0]["count"]
    logger.info(f"Number of relationships to sync: {limit}")

    create_index_query = dedent(
        """
            CREATE RANGE INDEX node_range_index__neo4j_sync_node__neo4j_sync_from_id IF NOT EXISTS
            FOR (n:_neo4j_sync_node)
            ON (n._neo4j_sync_from_id)
        """
    )
    logger.info(create_index_query)
    await to_driver.execute_query(create_index_query, database=to_database, routing=RoutingControl.WRITE)
    from_queries = [
        dedent(
            f"""
                MATCH (n)-[r]->(m)
                RETURN r
                SKIP {i}
                LIMIT {min(batch_size, limit - i)}
            """
        )
        for i in range(start, limit, batch_size)
    ]
    semaphore = Semaphore(os.cpu_count())
    for from_query in from_queries:
        logger.info(from_query)
        from_results, _, _ = await from_driver.execute_query(
            from_query, database=from_database, routing=RoutingControl.READ
        )
        coroutines = [
            create_relationship(to_driver, to_database, from_result, semaphore) for from_result in from_results
        ]
        await asyncio.gather(*coroutines)


async def create_relationship(to_driver: AsyncDriver, to_database: str, from_result: dict, semaphore: Semaphore):
    async with semaphore:
        r = from_result["r"]
        properties = dict(r.items())
        properties["_neo4j_sync_from_id"] = r.element_id
        properties = "{" + ", ".join(f"`{key}`: {json.dumps(value)}" for key, value in properties.items()) + "}"
        to_query = dedent(
            f"""
                MATCH (n:_neo4j_sync_node {{`_neo4j_sync_from_id`: "{r.start_node.element_id}"}})
                MATCH (m:_neo4j_sync_node {{`_neo4j_sync_from_id`: "{r.end_node.element_id}"}})
                CREATE (n)-[r:{r.type} {properties}]->(m)
            """
        )
        logger.info(to_query)
        await to_driver.execute_query(to_query, database=to_database, routing=RoutingControl.WRITE)


async def main():
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
        "--from-database",
        type=str,
        default="neo4j",
        help="Name of the source Neo4j database",
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
        "--to-database",
        type=str,
        default="neo4j",
        help="Name of the destination Neo4j database",
    )
    parser.add_argument(
        "--nodes-limit",
        type=int,
        default=-1,
        help="Limit the number of nodes to sync",
    )
    parser.add_argument(
        "--relationships-limit",
        type=int,
        default=-1,
        help="Limit the number of relationships to sync",
    )
    parser.add_argument(
        "--nodes-batch-size",
        type=int,
        default=1000,
        help="Batch size for syncing nodes",
    )
    parser.add_argument(
        "--relationships-batch-size",
        type=int,
        default=1000,
        help="Batch size for syncing relationships",
    )
    parser.add_argument(
        "--nodes-start",
        type=int,
        default=0,
        help="Start index for syncing nodes",
    )
    parser.add_argument(
        "--relationships-start",
        type=int,
        default=0,
        help="Start index for syncing relationships",
    )
    parser.add_argument(
        "--skip-nodes",
        action="store_true",
        help="Skip syncing nodes",
    )
    parser.add_argument(
        "--skip-relationships",
        action="store_true",
        help="Skip syncing relationships",
    )
    args = parser.parse_args()
    logging.info(args)

    async with AsyncGraphDatabase.driver(
        args.from_uri,
        auth=(args.from_user, args.from_password),
    ) as from_driver, AsyncGraphDatabase.driver(
        args.to_uri,
        auth=(args.to_user, args.to_password),
    ) as to_driver:
        if not args.skip_nodes:
            logging.info(f"Syncing nodes from {args.from_uri} to {args.to_uri}")
            start = time.time()
            await sync_nodes(
                from_driver,
                args.from_database,
                to_driver,
                args.to_database,
                args.nodes_limit,
                args.nodes_batch_size,
                args.nodes_start,
            )
            end = time.time()
            logging.info(f"Time to sync nodes from {args.from_uri} to {args.to_uri}: {timedelta(seconds=end - start)}")
        if not args.skip_relationships:
            logging.info(f"Syncing relationships from {args.from_uri} to {args.to_uri}")
            start = time.time()
            await sync_relationships(
                from_driver,
                args.from_database,
                to_driver,
                args.to_database,
                args.relationships_limit,
                args.relationships_batch_size,
                args.relationships_start,
            )
            end = time.time()
            logging.info(
                f"Time to sync relationships from {args.from_uri} to {args.to_uri}: {timedelta(seconds=end - start)}"
            )


if __name__ == "__main__":
    asyncio.run(main())
