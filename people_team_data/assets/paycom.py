"""Assets related to importing data from Paycom"""
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    Output,
    asset,
)
import psycopg2.extras
import json

from ..resources import postgres_db

from ..config.asset_configs import paycom_config
from ..utils import apply_config_to_dataframe, is_camel_case, to_camel_case, currency_to_decimal
from ..utils.constants import PAYCOM_REPORT_FILE_TEMPLATE


@asset
def paycom_report_file(context: AssetExecutionContext) -> Output:
    """A point-in-time report of paycom data on employees."""
    context.log.warning("TODO: Implement this")
    return Output("people_team_data/data/raw/paycom/paycom.csv")

@asset(required_resource_keys={"postgres_db"},
       resource_defs={
            "postgres_db": postgres_db.configured({
                "dbname": "calibrate",
                "user": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": 5432,
            })
        },
       deps=[AssetKey("bamboohr_report_file")])
def bamboohr_report(
    context: AssetExecutionContext, bamboohr_report_file: str
) -> None:
    """Database table of all active employees in BambooHR."""
    with open(bamboohr_report_file, "r") as f:
        report_data = json.load(f)
        context.log.debug(f"Loaded BambooHR report data: {bamboohr_report_file}")
    df = pd.json_normalize(report_data["employees"])
    # Apply typing to DataFrame columns
    column_types = {
        str(field["id"]): str(field["type"]) for field in report_data["fields"]
    }
    context.log.debug(f"Dataframe contains the following columns: {df.columns}")
    if df.columns.duplicated().any():
        dup_cols = df.columns[df.columns.duplicated()]
        context.log.warning(
            f"Found duplicate column names in the DataFrame: {dup_cols}"
        )
    for column, col_type in column_types.items():
        if column in df.columns:
            if col_type == "text":
                df[column] = df[column].astype(str)
            elif col_type == "employee_number":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
            elif col_type == "date":
                df[column] = pd.to_datetime(df[column], errors="coerce")
            elif col_type == "list" or col_type == "multilist":
                df[column] = df[column].astype(str)
            elif col_type == "decimal":
                df[column] = pd.to_numeric(df[column].fillna(-1), errors="coerce")
            elif col_type == "currency":
                df[column] = pd.to_numeric(df[column].apply(currency_to_decimal))
            else:
                context.log.debug(
                    f"No predefined conversion found. "
                    f"Converting column '{column}' to type 'str'"
                )
                df[column] = df[column].astype(str)
        else:
            context.log.warning(f"Column '{column}' not found in DataFrame")
    # Change column names to camelCase for consistency.
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
        table_name = "paycom_report"
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

    context.log.info("Paycom report data loaded into the database")

    df_preview = df.head().to_markdown()
    metadata = {
        "num_rows": MetadataValue.int(df.shape[0]),
        "num_columns": MetadataValue.int(df.shape[1]),
        "columns": MetadataValue.json(list(df.columns)),
        "preview": MetadataValue.md(df_preview),
    }

    return Output(value=df, metadata=metadata)
