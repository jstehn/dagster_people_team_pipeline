import json
import os
from datetime import datetime
import psycopg2.extras
import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetKey,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    Output,
    asset,
)
from ..resources import PostgresDBResource
from ..utils import currency_to_decimal, is_camel_case, to_camel_case
from ..utils.constants import BAMBOOHR_REPORT_FILE_TEMPLATE
from people_team_data.resources.postgres_db_resource import postgres_db



@asset
def bamboohr_report_file(context: AssetExecutionContext) -> Output:
    """A point-in-time report of all employees in BambooHR."""
    api_key = EnvVar("BAMBOOHR_API_KEY").get_value()
    subdomain = EnvVar("BAMBOOHR_SUBDOMAIN").get_value()
    report_id = EnvVar("BAMBOOHR_REPORT_ID").get_value()
    request_url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1/reports/{report_id}?format=json&onlyCurrent=true"
    context.log.info(f"Retrieving BambooHR report: {request_url}")
    response = requests.get(request_url, auth=(api_key, ""))

    if response.status_code != 200:
        context.log.error(
            f"Failed to retrieve BambooHR report: {response.status_code} {response.text}"
        )
        response.raise_for_status()

    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d")
    file_path = BAMBOOHR_REPORT_FILE_TEMPLATE.format(date=formatted_date)
    context.log.info("Saving BambooHR report: " + file_path)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write(response.text)

    metadata = {
        "request_url": MetadataValue.url(request_url),
        "response_status_code": MetadataValue.int(response.status_code),
        "file_path": MetadataValue.path(file_path),
        "num_employees": MetadataValue.int(len(json.loads(response.text)["employees"])),
        "num_fields": MetadataValue.int(len(json.loads(response.text)["fields"])),
    }
    return Output(value=file_path, metadata=metadata)


@asset(
    required_resource_keys={"postgres_db"},
    resource_defs={
        "postgres_db": postgres_db.configured({
            "dbname": "calibrate",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 5432,
        })
    },
    deps=["bamboohr_report_file"],
)
def bamboohr_report(context: AssetExecutionContext, bamboohr_report_file: Output) -> Output[pd.DataFrame]:
    """Processes BambooHR report data and uploads it to the PostgreSQL database."""
    """Database table of all active employees in BambooHR."""
    with open(bamboohr_report_file, "r") as f:
        report_data = json.load(f)
        context.log.debug(f"Loaded BambooHR report data: {bamboohr_report_file}")
    df = pd.json_normalize(report_data["employees"])

    # Change column names to camelCase for consistency
    column_mapping = {
        str(field["id"]): str(to_camel_case(str(field["name"])))
        for field in report_data["fields"]
        if not is_camel_case(str(field["id"]))
    }
    df.rename(columns=column_mapping, inplace=True)
    context.log.debug(f"Renamed columns:\n{column_mapping}")

    # Filter out rows with missing Employee # and Status as 'Inactive'
    initial_row_count = df.shape[0]
    df = df[~(df["employeeNumber"].isna() & (df["status"] != "Active"))]
    filtered_row_count = df.shape[0]
    context.log.info(
        f"Filtered out {initial_row_count - filtered_row_count} "
        f"rows with missing employeeNumber and are not active."
    )

    # Set Employee # as index
    df.drop(columns=["id"], inplace=True)

    # Check for duplicate Employee # entries
    duplicates_mask = df.index.duplicated(keep=False)
    if duplicates_mask.any():
        duplicate_entries = df[duplicates_mask]
        context.log.error(f"Duplicate Employee # entries found:\n{duplicate_entries}")
        raise ValueError(
            "Duplicate Employee # entries detected. Ensure all Employee # values are unique."
        )

    context.log.debug(f"Set up DataFrame for upload:\n{df.describe()}")

    # Upload DataFrame to PostgreSQL
    with context.resources.postgres_db as conn:
        cursor = conn.cursor()
        table_name = "bamboohr_report"
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_table_query = f"""
        CREATE TABLE {table_name} (
            {', '.join([f'{col} TEXT' for col in df.columns])}
        )
        """
        cursor.execute(create_table_query)
        insert_query = f"INSERT INTO {table_name} VALUES %s"
        psycopg2.extras.execute_values(cursor, insert_query, df.values)
        conn.commit()

    context.log.info("BambooHR report data loaded into the database")

    df_preview = df.head().to_markdown()
    metadata = {
        "num_rows": MetadataValue.int(df.shape[0]),
        "num_columns": MetadataValue.int(df.shape[1]),
        "columns": MetadataValue.json(list(df.columns)),
        "preview": MetadataValue.md(df_preview),
    }

    return Output(value=df, metadata=metadata)
