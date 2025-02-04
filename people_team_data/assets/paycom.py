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


PAYCOM_DTYPES = {
    "Hire_Date": "datetime64[ns]",
    "Rehire_Date": "datetime64[ns]",
    "Termination_Date": "datetime64[ns]",
    "Salary": "currency",
    "Rate_1": "currency",
    "Annual_Salary": "currency",
}


@asset
def paycom_report_file(context: AssetExecutionContext) -> Output:
    """A point-in-time report of paycom data on employees."""
    context.log.warning("TODO: Implement this")
    return Output("people_team_data/data/raw/paycom/paycom.csv")

@asset(required_resource_keys={"postgres_db"},
       deps=[AssetKey("paycom_report_file")])
def paycom_report(
    context: AssetExecutionContext, paycom_report_file: str
) -> Output:
    """Database table of all active employees in BambooHR."""
    paycom_data = pd.read_csv(paycom_report_file, dtype=str)
    context.log.debug(f"Loaded Paycom file. Current columns\n{paycom_data.dtypes}")
    for column, dtype in PAYCOM_DTYPES.items():
        if dtype == "currency":
            paycom_data[column] = pd.to_numeric(paycom_data[column].apply(currency_to_decimal), errors='coerce')
        elif dtype == "datetime64[ns]":
            paycom_data[column] = pd.to_datetime(paycom_data[column], errors="coerce")
        else:
            paycom_data[column] = paycom_data[column].where(
                paycom_data[column].isna(),
                paycom_data[column].astype(dtype)
            )

    # Ensure Employee_Code is a string type
    if 'Employee_Code' in paycom_data.columns:
        paycom_data['Employee_Code'] = paycom_data['Employee_Code'].astype(str)
 
    # Upload DataFrame to PostgreSQL
    engine = context.resources.postgres_db
    df = paycom_data
    paycom_data.to_sql("paycom_report", engine, if_exists="replace", index=False)

    context.log.info("Paycom report data loaded into the database")

    df_preview = paycom_data.head().to_markdown()
    metadata = {
        "num_rows": MetadataValue.int(df.shape[0]),
        "num_columns": MetadataValue.int(df.shape[1]),
        "columns": MetadataValue.json(list(df.columns)),
        "preview": MetadataValue.md(df_preview),
    }

    return Output(value=df, metadata=metadata)
