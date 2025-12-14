import logging
import os
import uuid

import databuilder.publisher.neo4j_csv_publisher as neo4j_csv_publisher
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import (
    FSElasticsearchJSONLoader,
)
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from dagster import job, op

logging.basicConfig(level=logging.INFO)

POSTGRES_CONN_STRING = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}"
    f"/{os.environ['POSTGRES_DB']}"
)

NEO4J_ENDPOINT = "bolt://minerva-amundsen-neo4j:7687"
NEO4J_USER_VAL = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD_VAL = os.getenv("NEO4J_PASSWORD", "test")

NODE_DIR = "/tmp/amundsen/node"
RELATION_DIR = "/tmp/amundsen/relationship"

SUPPORTED_SCHEMAS = [
    "core",
    "metadata",
    "mart_match_performance",
    "mart_player_performance",
    "mart_team_standings",
]
SUPPORTED_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(
    schemas="', '".join(SUPPORTED_SCHEMAS)
)


def extract():
    where_clause_suffix = f"st.schemaname in {SUPPORTED_SCHEMA_SQL_IN_CLAUSE}"

    job_config = ConfigFactory.from_dict(
        {
            f"extractor.postgres_metadata.{PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": where_clause_suffix,
            f"extractor.postgres_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}": POSTGRES_CONN_STRING,
            f"loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}": NODE_DIR,
            f"loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}": RELATION_DIR,
            f"publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}": NODE_DIR,
            f"publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}": RELATION_DIR,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}": NEO4J_ENDPOINT,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}": NEO4J_USER_VAL,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}": NEO4J_PASSWORD_VAL,
            f"publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}": str(uuid.uuid4()),
        }
    )

    extractor = PostgresMetadataExtractor()
    loader = FsNeo4jCSVLoader()
    transformer = NoopTransformer()

    task = DefaultTask(extractor=extractor, loader=loader, transformer=transformer)

    publisher = Neo4jCsvPublisher()

    job = DefaultJob(conf=job_config, task=task, publisher=publisher)

    try:
        job.launch()
        logging.info("Metadata extraction and publishing completed successfully!")
    except Exception as e:
        logging.error(f"Job failed with error: {str(e)}")
        raise


def elasticsearch_publish():
    extracted_search_data_path = "/var/tmp/amundsen/search_data.json"

    task = DefaultTask(
        loader=FSElasticsearchJSONLoader(),
        extractor=Neo4jSearchDataExtractor(),
        transformer=NoopTransformer(),
    )

    elasticsearch_client = Elasticsearch(
        [{"host": "minerva-elasticsearch", "port": 9200}],
        timeout=60,
        max_retries=10,
        retry_on_timeout=True,
    )
    elasticsearch_client.indices.put_settings(
        index="_all", body={"index.blocks.read_only_allow_delete": None}
    )

    elasticsearch_new_index_key = f"tables{uuid.uuid4()}"
    elasticsearch_new_index_key_type = "table"
    elasticsearch_index_alias = "table_search_index"

    job_config = ConfigFactory.from_dict(
        {
            f"extractor.search_data.extractor.neo4j.{Neo4jExtractor.GRAPH_URL_CONFIG_KEY}": NEO4J_ENDPOINT,
            f"extractor.search_data.extractor.neo4j.{Neo4jExtractor.MODEL_CLASS_CONFIG_KEY}": "databuilder.models.table_elasticsearch_document.TableESDocument",
            f"extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_USER}": NEO4J_USER_VAL,
            f"extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_PW}": NEO4J_PASSWORD_VAL,
            f"loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY}": extracted_search_data_path,
            f"loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY}": "w",
            f"publisher.elasticsearch.{ElasticsearchPublisher.FILE_PATH_CONFIG_KEY}": extracted_search_data_path,
            f"publisher.elasticsearch.{ElasticsearchPublisher.FILE_MODE_CONFIG_KEY}": "r",
            f"publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY}": elasticsearch_client,
            f"publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY}": elasticsearch_new_index_key,
            f"publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY}": elasticsearch_new_index_key_type,
            f"publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY}": elasticsearch_index_alias,
        }
    )

    job = DefaultJob(conf=job_config, task=task, publisher=ElasticsearchPublisher())

    try:
        job.launch()
        logging.info("Metadata extraction and publishing completed successfully!")
    except Exception as e:
        logging.error(f"Job failed with error: {str(e)}")
        raise


@op
def extract_metadata_op():
    extract()


@op
def publish_search_op():
    elasticsearch_publish()


@job
def amundsen_postgres_ingest_job():
    extract_metadata_op()
    publish_search_op()
