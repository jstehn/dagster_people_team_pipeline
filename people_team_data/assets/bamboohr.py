import json
import os
from datetime import datetime

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
from dagster_duckdb import DuckDBResource

from ..utils import currency_to_decimal, is_camel_case, to_camel_case
from ..utils.constants import BAMBOOHR_REPORT_FILE_TEMPLATE


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


@asset(deps=[AssetKey("bamboohr_report_file")])
def bamboohr_report(
    context: AssetExecutionContext, duckdb:DuckDBResource, bamboohr_report_file: str
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
    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE bamboohr_report AS SELECT * FROM df")
    context.log.info("BambooHR report data loaded into the database")
    df_preview = df.head().to_markdown()
    metadata = {
        "num_rows": MetadataValue.int(df.shape[0]),
        "num_columns": MetadataValue.int(df.shape[1]),
        "columns": MetadataValue.json(
            {col: str(dtype) for col, dtype in df.dtypes.items()}
        ),
        "preview": MetadataValue.md(df_preview),
    }

    return MaterializeResult(metadata=metadata)
