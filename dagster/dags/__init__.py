from dagster import Definitions

from .associations_etl import associations_etl

defs = Definitions(
    jobs=[associations_etl],
)
