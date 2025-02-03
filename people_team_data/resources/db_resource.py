import duckdb_engine  # Ensure the duckdb-engine package is imported
from dagster import InitResourceContext, resource
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..utils.constants import DUCKDB_LOCATION


@resource
def db_resource(context: InitResourceContext):
    """A local instance of duckdb.db with connection pooling"""
    engine = create_engine(
        f"duckdb:///{DUCKDB_LOCATION}",
        pool_size=5,  # Adjust the pool size as needed
        max_overflow=10,  # Adjust the overflow size as needed
    )
    Session = sessionmaker(bind=engine)
    return Session