from sqlalchemy import create_engine
from dagster import resource, InitResourceContext

@resource
def postgres_db(context: InitResourceContext):
    """A resource that provides a PostgreSQL database connection using SQLAlchemy."""
    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@localhost:5432/calibrate"
    )
    return engine