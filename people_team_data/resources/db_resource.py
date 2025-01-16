from dagster import EnvVar, InitResourceContext, resource
from dagster_duckdb import DuckDBResource

from ..utils.constants import DUCKDB_LOCATION


@resource
def db_resource(context: InitResourceContext):
    """A local instance of duckdb.db"""
    return DuckDBResource(
        database=DUCKDB_LOCATION
    )