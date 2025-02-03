import psycopg2
from dagster import InitResourceContext, resource

class PostgresDBResource:
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

@resource(
    config_schema={
        "db_name": str,
        "user": str,
        "password": str,
        "host": str,
        "port": int,
    }
)
def postgres_db(context: InitResourceContext):
    """A resource that provides a connection to a PostgreSQL database."""
    return PostgresDBResource(
        db_name=context.resource_config["db_name"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        host=context.resource_config["host"],
        port=context.resource_config["port"]
    )